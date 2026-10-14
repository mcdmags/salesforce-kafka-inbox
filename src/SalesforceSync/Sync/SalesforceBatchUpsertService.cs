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
/// <item>Respects the rate limiter's pause.</item>
/// <item>Reads the next batch of up to <see cref="SyncOptions.BatchSize"/> entities from the inbox table, in arrival
/// order, with each entity's oldest pending rows. One sequential worker does this: nothing is locked or leased, so two
/// workers on one inbox would process the same entities at the same time.</item>
/// <item>Per entity, reads the batch's events and keeps the one with the newest event time (events carry the full
/// record); the others are superseded, and so is the newest if it is not newer than what was already synced. Arrival
/// order plays no part, so events may come out of order, over any partitions. An event that can't be read or
/// mapped is dead-lettered on its own.</item>
/// <item>Writes the units to Salesforce in 2 API calls (<see cref="WriteToSalesforceAsync"/>): one Composite request
/// reads the current records, one writes what differs with sObject Collections.</item>
/// <item>Records each entity's outcome in one transaction: done (and the watermark advanced), left pending for the
/// next batch, retried with backoff, or dead-lettered. A Salesforce call that fails as a whole fails the whole batch:
/// every event of the batch is held (the write's outcome is unknown), deferred (rate limited) or retried.</item>
/// </list>
/// This class is the flow. The rules it applies are small classes without I/O (<see cref="BatchPreparer"/> and the
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

    /// <summary>Recording a hold must not be cut short by the cancellation that caused it (shutdown), but may not hang it either.</summary>
    private static readonly TimeSpan HoldRecordingTimeout = TimeSpan.FromSeconds(10);

    private readonly SyncOptions _opt = options.Value;
    private readonly SalesforceOptions _salesforceOpt = salesforceOptions.Value;
    private readonly WriteResultInterpreter _interpreter = new(errors);

    /// <summary>Reads, upserts and records one batch of the strategy's entity type.</summary>
    /// <returns>Inbox rows processed (0 = nothing to do, or paused right now).</returns>
    public async Task<int> UpsertBatchAsync<TEvent, TRow>(UpsertStrategy<TEvent> strategy, IRepository<TRow> inbox, CancellationToken ct)
        where TRow : InboxRow, new()
    {
        if (!await gate.IsOpenAsync(ct))
        {
            return 0;
        }

        // 1. Read the next batch from the inbox table (short transaction)
        var rows = await inbox.ReadBatchAsync(strategy.EntityType, ct);
        if (rows.Events.Count == 0)
        {
            return 0;
        }

        // 2. Per entity: keep the newest event, drop it if it isn't newer than what is synced, map it to records
        var batch = BatchPreparer.Prepare(strategy, rows.Events, rows.SyncedTimes);

        // 3. Salesforce (outside any transaction): read, decide, send, interpret
        Dictionary<string, UpsertResult> results;
        try
        {
            results = batch.Work.Count == 0
                ? []
                : (await WriteToSalesforceAsync(batch.Units, [.. strategy.Queries([.. batch.Work.Select(w => w.Unit)])],
                    ct)).ToDictionary(r => r.Key);
        }
        catch (UnknownOutcomeException ex)
        {
            // Salesforce may still apply the write. Caught on shutdown too: an entity synced again before the lost
            // write lands could be overwritten by it.
            log.LogWarning(ex, "{Type} batch: holding {Count} events for {Hold}", strategy.EntityType, rows.Events.Count, _opt.UnknownOutcomeHold);
            using var recording = new CancellationTokenSource(HoldRecordingTimeout);
            await inbox.HoldAsync(rows, ex.Message, recording.Token);
            ct.ThrowIfCancellationRequested();
            return rows.Events.Count;
        }
        catch (RateLimitedException ex)
        {
            log.LogInformation("Rate limited; deferring {Count} {Type} events for {Delay}", rows.Events.Count, strategy.EntityType, ex.RetryAfter);
            await inbox.DeferAsync(rows, ex.RetryAfter, ct);
            return 0;
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            // A request failed that Salesforce provably did not apply (describe, the read or a write): every entity is
            // retried. Records that earlier write requests of the batch saved are safe: the retry reads them and
            // writes only what still differs. Shutdown (ct) is not caught: the failed request wrote nothing, and the
            // rows are still pending.
            log.LogError(ex, "{Type} batch failed", strategy.EntityType);
            await inbox.FailAllAsync(rows, ex.Message, ex is SalesforceQueryException q ? q.Errors.FirstOrDefault()?.Code : null, ct);
            return rows.Events.Count;
        }

        // 4. Record every entity's outcome in the inbox
        LogFailures(strategy.EntityType, batch, results.Values);
        await inbox.RecordAsync(rows, batch.Outcomes(results), ct);
        return rows.Events.Count;
    }

    /// <summary>
    /// The Salesforce half of a batch, for any sObjects, in two API calls: one Composite request reads the current
    /// records, one writes what differs with sObject Collections (more than 5 collections are split over further
    /// requests). Returns one outcome per unit. Nothing here touches the inbox.
    /// </summary>
    /// <remarks>
    /// A write request that fails as a whole (the call, not records in it) throws, and the caller fails, holds or
    /// defers the whole batch, even when earlier requests of the batch were saved: the retry diffs against Salesforce
    /// again, so what they saved is not written twice.
    /// </remarks>
    /// <param name="units">At most <see cref="ReadQueryBuilder.MaxEntitiesPerBatch"/>, keyed by entity key.</param>
    /// <param name="queries">The strategy's reads: the units' records and the records their lookups point at.</param>
    /// <exception cref="UnknownOutcomeException">The write call failed without proving that nothing was applied.</exception>
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

        // 2. Normalize; a unit Salesforce would reject fails on its own without reaching the query
        var (normalized, rejected) = RecordNormalizer.Normalize(units, schemas);
        results.AddRange(rejected);
        if (normalized.Count == 0)
        {
            return results;
        }

        // 3. Read: one Composite call with the strategy's queries
        var current = await ReadCurrentAsync(ReadQueryBuilder.Build(normalized, queries, schemas), schemas.Keys, ct);

        // 4. Decide per record: unchanged, create or update (by what the read found), and how each lookup is linked;
        // the writes are grouped into collections and Composite requests
        var plan = WritePlanner.Plan(normalized, current);
        results.AddRange(plan.NotSent);

        // 5. Send: normally one Composite call. 6. Per-record results -> one outcome per unit.
        foreach (var request in plan.Requests)
        {
            var answers = await SendAsync(request, ct);
            results.AddRange(_interpreter.Interpret(request, answers));
        }

        return results;
    }

    private async Task<Dictionary<string, SObjectSchema>> DescribeAsync(IReadOnlyList<(string Key, UpsertUnit Unit)> units,
        IReadOnlyList<SoqlQuery> queries, CancellationToken ct)
    {
        var sobjects = units.SelectMany(u => u.Unit.Records)
            .SelectMany(r => (r.Lookups ?? []).Select(l => l.SObject).Append(r.SObject))
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
            var rows = await salesforce.QueryAsync([.. queries.Select(q => q.Query)], ct);
            return CurrentRecords.From(rows, queries);
        }
        catch (SalesforceQueryException ex) when (ex.Errors.Any(e => e.Code == "INVALID_FIELD"))
        {
            // The cached describe is out of date (a field was removed or renamed): describe again next time
            foreach (var sobject in describedSObjects)
            {
                schemaCache.Invalidate(sobject);
            }

            throw;
        }
    }

    private async Task<IReadOnlyList<CollectionResult>> SendAsync(CompositeWrite request, CancellationToken ct)
    {
        try
        {
            return await salesforce.SendCollectionsAsync(request.BuildRequests(), ct);
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