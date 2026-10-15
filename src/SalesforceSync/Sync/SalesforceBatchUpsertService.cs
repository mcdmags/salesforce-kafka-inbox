using System.Text.Json;
using Microsoft.Extensions.Options;
using SalesforceSync.Data;
using SalesforceSync.Inbox;
using SalesforceSync.RateLimiting;
using SalesforceSync.Salesforce;
using SalesforceSync.Sync.Upsert;

namespace SalesforceSync.Sync;

/// <summary>
/// The one sync engine for every Salesforce object. <see cref="UpsertBatchAsync{TEvent, TRow}"/>
/// runs one batch end to end; the inbox repository (its row type is the type's inbox table) holds the events, and the
/// strategy says how to read one, how it maps to Salesforce records and what to read from Salesforce.
/// <list type="number">
/// <item>Takes a global concurrency slot and respects a shared pause (rate limits, all pods).</item>
/// <item>Claims up to <see cref="SyncOptions.BatchSize"/> due entities: each one's sf_sync_state row is locked with
/// FOR UPDATE SKIP LOCKED and leased, so no two workers in any pod ever process the same entity at once, and its
/// oldest pending events are read from the inbox table.</item>
/// <item>Per entity, reads the claimed events and keeps the one with the newest event time (events carry the full
/// record); the others are superseded, and so is the newest if it is not newer than what was already synced. Arrival
/// order plays no part, so events may come out of order, over any partitions. An event that can't be read or
/// mapped is dead-lettered on its own.</item>
/// <item>Writes the units to Salesforce (<see cref="WriteToSalesforceAsync"/>), giving up before the lease runs out:
/// one Composite request reads the current records (more only when very long ids need more than 5 queries), then
/// what differs is written with sObject Collections upserts, one per level and object: one upsert call when the
/// writes are of one object, else one Composite request of them, parents first (Accounts, then their Profiles: 1
/// call), whatever the objects.</item>
/// <item>Records each entity's outcome in one transaction: done (and the watermark advanced), released for the
/// next claim, retried with backoff, or dead-lettered. A Salesforce call that fails as a whole fails the whole batch:
/// every claimed event is held (the write's outcome is unknown), deferred (rate limited) or retried.</item>
/// </list>
/// This class is the flow. The rules it applies are small classes without I/O (<see cref="PreparedBatch"/> and the
/// ones in <c>Sync/Upsert</c>); Salesforce is reached only through <see cref="SalesforceRestClient"/> and the inbox only
/// through <see cref="IRepository{TEntity}"/>, whose every unit of work runs through <see cref="PostgresRetryStrategy"/>.
/// </summary>
public sealed class SalesforceBatchUpsertService(
    ISalesforceBatchGate gate,
    SalesforceRestClient salesforce,
    SalesforceSchemaCache schemaCache,
    SalesforceErrors errors,
    IOptions<SalesforceOptions> salesforceOptions,
    IOptions<SyncOptions> options,
    ILogger<SalesforceBatchUpsertService> log)
{
    /// <summary>
    /// How inbox payloads (whole event envelopes) are deserialized into a strategy's event type: camelCase JSON,
    /// matched to the PascalCase CLR properties without regard to case.
    /// </summary>
    public static readonly JsonSerializerOptions EventJson = new(JsonSerializerDefaults.Web);

    /// <summary>
    /// Share of the lease a batch may spend before its Salesforce call is cancelled; the rest is margin for recording
    /// the outcome. A write cancelled this way has an unknown outcome and puts its entities on hold.
    /// </summary>
    private const double LeaseUsableFraction = 0.8;

    /// <summary>Recording a hold must not be cut short by the cancellation that caused it (shutdown), but may not hang it either.</summary>
    private static readonly TimeSpan HoldRecordingTimeout = TimeSpan.FromSeconds(10);

    private readonly SyncOptions _opt = options.Value;
    private readonly SalesforceOptions _salesforceOpt = salesforceOptions.Value;
    private readonly WriteResultInterpreter _interpreter = new(errors);
    private readonly SalesforceErrors _errors = errors;

    /// <summary>Claims, upserts and records one batch of the strategy's entity type.</summary>
    /// <returns>Inbox rows processed (0 = nothing to do, or no capacity right now).</returns>
    public async Task<int> UpsertBatchAsync<TEvent, TRow>(UpsertStrategy<TEvent> strategy, IRepository<TRow> inbox, CancellationToken ct)
        where TRow : InboxRow, new()
    {
        await using var admission = await gate.TryEnterAsync(ct);
        if (admission is null)
        {
            return 0;
        }

        // Give up on Salesforce before the lease runs out, so another worker never claims an entity while this one
        // can still write to it. Started before the claim, so the margin is measured from when the lease began.
        using var leaseCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        leaseCts.CancelAfter(_opt.Lease * LeaseUsableFraction);

        // 1. Claim events from the inbox table (short transaction; the lease, not a transaction, protects the rows
        // until step 4)
        var claim = await inbox.ClaimAsync(ct);
        if (claim.Events.Count == 0)
        {
            return 0;
        }

        // 2. Per entity: keep the newest event, drop it if it isn't newer than what is synced, map it to records
        var batch = PreparedBatch.Prepare(strategy, claim.Events, claim.SyncedTimes);

        // 3. Salesforce (outside any transaction): read, decide, send, interpret. Not inside one on purpose: Salesforce
        // can't take part in a Postgres transaction (a rollback can't undo a saved write), the execution strategy
        // re-runs its whole delegate on a transient Postgres error (the HTTP calls would be sent again), and a
        // transaction held for up to the request timeout keeps locks, a pooled connection per worker and autovacuum's
        // horizon on a high-churn table. A crash between here and step 4 leaves the rows pending under an expiring
        // lease; the retry reads Salesforce and writes only what still differs.
        Dictionary<string, UpsertResult> results;
        try
        {
            results = batch.Work.Count == 0
                ? []
                : (await WriteToSalesforceAsync(batch.Units, [.. strategy.Queries([.. batch.Work.Select(w => w.Unit)])],
                    leaseCts.Token)).ToDictionary(r => r.Key);
        }
        catch (UnknownOutcomeException ex)
        {
            // Salesforce may still apply the write. Caught on shutdown too: an entity released now could be written
            // by another worker before the lost write lands. If the hold can't be recorded (or the process is killed
            // first), the lease is what still protects the entities.
            log.LogWarning(ex, "{Type} batch: holding {Count} events for {Hold}", strategy.EntityType, claim.Events.Count, _opt.UnknownOutcomeHold);
            using var recording = new CancellationTokenSource(HoldRecordingTimeout);
            await inbox.HoldAsync(claim, ex.Message, recording.Token);
            ct.ThrowIfCancellationRequested();
            return claim.Events.Count;
        }
        catch (RateLimitedException ex)
        {
            log.LogInformation("Rate limited; deferring {Count} {Type} events for {Delay}", claim.Events.Count, strategy.EntityType, ex.RetryAfter);
            await inbox.DeferAsync(claim, ex.RetryAfter, ct);
            return 0;
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            // A request failed that Salesforce provably did not apply (describe, the read or a write): every entity is
            // retried. Records that earlier write requests of the batch saved are safe: the retry reads them and writes only what still differs. Shutdown (ct) is not caught:
            // the failed request wrote nothing, and the lease simply expires.
            log.LogError(ex, "{Type} batch failed", strategy.EntityType);
            await inbox.FailAllAsync(claim, ex.Message, ex is SalesforceQueryException q ? q.Errors.FirstOrDefault()?.Code : null, ct);
            return claim.Events.Count;
        }

        // 4. Record every entity's outcome in the inbox
        LogFailures(strategy.EntityType, batch, results.Values);
        await inbox.RecordAsync(claim, batch.Outcomes(results), ct);
        return claim.Events.Count;
    }

    /// <summary>
    /// The Salesforce half of a batch, for any sObjects: one Composite request reads the current records (one per 5
    /// queries; only very long ids make more than 5, as each query's URL must fit a call), then what differs is
    /// written with sObject Collections upserts by external id, one per level and object (and 200 records).
    /// Each write request (more than 5 upserts are split over further requests) is one call: its upsert on its own when
    /// it has one (a single object), else one Composite request of them, parents first. Returns one outcome per unit.
    /// Nothing here touches the inbox.
    /// </summary>
    /// <remarks>
    /// A write call that fails as a whole (the call, not records or subrequests in it) throws, and the caller fails,
    /// holds or defers the whole claim, even when an earlier request of the batch was saved: the outcome of the failed
    /// call decides, so an unknown one holds every claimed entity. Nothing earlier calls learned is recorded, and
    /// nothing needs to be: every write is matched by external id, so the retry reads Salesforce, finds what they saved
    /// and writes only what still differs. A Composite subrequest that fails as a whole fails only its records (retried).
    /// </remarks>
    /// <param name="units">At most <see cref="ReadQueryBuilder.MaxEntitiesPerBatch"/>, keyed by entity key.</param>
    /// <param name="queries">The strategy's reads: the units' records and the records their lookups and foreign keys point at.</param>
    /// <exception cref="UnknownOutcomeException">The write call failed without proving that nothing was applied.</exception>
    /// <param name="ct">Cancels the calls; shutdown, not a failure.</param>
    internal async Task<List<UpsertResult>> WriteToSalesforceAsync(IReadOnlyList<(string Key, UpsertUnit Unit)> units,
        IReadOnlyList<SoqlQuery> queries, CancellationToken ct)
    {
        if (units.Count > ReadQueryBuilder.MaxEntitiesPerBatch)
        {
            throw new ArgumentOutOfRangeException(nameof(units), $"At most {ReadQueryBuilder.MaxEntitiesPerBatch} units per call");
        }

        var results = new List<UpsertResult>(units.Count);
        if (units.Count == 0)
        {
            return results;
        }

        // 1. Field metadata of every object involved (cached per process, revalidated with If-Modified-Since)
        var schemas = await DescribeAsync(units, queries, ct);

        // 2. Normalize (and set each record's RecordTypeId); a unit Salesforce would reject fails on its own without
        // reaching the query
        var (normalized, rejected) = RecordNormalizer.Normalize(units, schemas, _errors);
        results.AddRange(rejected);
        if (normalized.Count == 0)
        {
            await InvalidateRecordTypesAsync(units, results, ct);
            return results;
        }

        // 3. Read: one Composite call with the strategy's queries (more only if long ids split them into more than 5)
        var current = await ReadCurrentAsync(ReadQueryBuilder.Build(normalized, queries, schemas), schemas.Keys, ct);

        // 4. Decide per record: unchanged, create or update (by what the read found), and how each lookup is linked;
        // the writes are grouped into collections and Composite requests
        var plan = WritePlanner.Plan(normalized, current);
        results.AddRange(plan.NotSent);

        // 5. Send. A request of one object is its Collections upsert; a request of several objects (Accounts and their
        // Profiles, or a new child linked to its new parent by a lookup's @{reference}) is one Composite request of
        // their upserts, parents first. Either way one call per request.
        // 6. Per-record results -> one outcome per unit, the same way for both; a unit with writes in several upserts
        // gets one outcome from all of them. Inside a Composite request a child is sent even if its new parent failed
        // in an earlier subrequest: the org decides, and the parent's result decides the unit.
        foreach (var request in plan.Requests)
        {
            var collections = request.BuildRequests();
            results.AddRange(_interpreter.Interpret(request, await WriteAsync(() => salesforce.UpsertAsync(collections, ct))));
        }

        await InvalidateRecordTypesAsync(units, results, ct);
        return results;
    }

    /// <summary>
    /// A record type the cached describe doesn't have (or not as active and available), or whose Id Salesforce
    /// rejected, may just have been created, activated or assigned: its object is described again next time.
    /// </summary>
    private async Task InvalidateRecordTypesAsync(IReadOnlyList<(string Key, UpsertUnit Unit)> units, IEnumerable<UpsertResult> results,
        CancellationToken ct)
    {
        var failed = results
            .SelectMany(r => r.Errors.Where(e => e.Code == SalesforceErrors.InvalidRecordType).Select(e => (r.Key, e.ReferenceId)))
            .ToHashSet();
        if (failed.Count == 0)
        {
            return;
        }

        foreach (var (key, unit) in units)
        {
            foreach (var record in unit.Records.Where(r => failed.Contains((key, r.ReferenceId))))
            {
                await schemaCache.InvalidateAsync(record.SObject, ct);
            }
        }
    }

    private async Task<Dictionary<string, SObjectSchema>> DescribeAsync(IReadOnlyList<(string Key, UpsertUnit Unit)> units,
        IReadOnlyList<SoqlQuery> queries, CancellationToken ct)
    {
        var sobjects = units.SelectMany(u => u.Unit.Records)
            .SelectMany(r => r.Parents.Select(p => p.SObject).Append(r.SObject))
            .Concat(queries.Select(q => q.SObject))
            .Distinct(StringComparer.OrdinalIgnoreCase);

        var schemas = new Dictionary<string, SObjectSchema>(StringComparer.OrdinalIgnoreCase);
        foreach (var sobject in sobjects)
        {
            schemas[sobject] = await schemaCache.GetAsync(sobject, _salesforceOpt.SchemaRefreshInterval,
                (since, token) => salesforce.DescribeAsync(sobject, since, token), ct);
        }

        return schemas;
    }

    private async Task<CurrentRecords> ReadCurrentAsync(IReadOnlyList<ReadQuery> queries, IEnumerable<string> describedSObjects,
        CancellationToken ct)
    {
        if (queries.Count == 0)
        {
            return CurrentRecords.None;
        }

        try
        {
            // One Composite request (call) per 5 queries: normally one
            var rows = new Dictionary<string, List<JsonElement>>(StringComparer.Ordinal);
            foreach (var request in queries.Chunk(SalesforceRestClient.MaxCompositeQueries))
            {
                foreach (var (referenceId, found) in await salesforce.QueryAsync([.. request.Select(q => q.Query)], ct))
                {
                    rows[referenceId] = found;
                }
            }

            return CurrentRecords.From(rows, queries);
        }
        catch (SalesforceQueryException ex) when (ex.Errors.Any(e => e.Code == "INVALID_FIELD"))
        {
            // The cached describe is out of date (a field was removed or renamed): describe again next time
            foreach (var sobject in describedSObjects)
            {
                await schemaCache.InvalidateAsync(sobject, ct);
            }

            throw;
        }
    }

    private static async Task<T> WriteAsync<T>(Func<Task<T>> write)
    {
        try
        {
            return await write();
        }
        catch (Exception ex) when (WriteOutcome.IsUnknown(ex))
        {
            // Everything before this point only read from Salesforce, so its failures are safe to retry. From here
            // on Salesforce may have the write, and may still apply it after we stop waiting.
            throw new UnknownOutcomeException(ex);
        }
    }

    private void LogFailures(string entityType, PreparedBatch batch, IEnumerable<UpsertResult> results)
    {
        foreach (var (id, error) in batch.Invalid)
        {
            log.LogWarning("Dead-lettered event {Id} ({Code}): {Error}", id, SalesforceErrors.InvalidEvent, error);
        }

        foreach (var r in results.Where(r => r.Outcome is SyncOutcome.PermanentFailure or SyncOutcome.TransientFailure))
        {
            log.LogWarning("{Type} {Key}: {Outcome} ({Code}) {Errors}", entityType, r.Key, r.Outcome,
                r.PrimaryError?.Code, string.Join("; ", r.Errors));
        }
    }
}