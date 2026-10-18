using System.Text.Json;
using Microsoft.Extensions.Options;
using SalesforceSync.Data;
using SalesforceSync.Data.Inbox;
using SalesforceSync.Infrastructure.RateLimiting;
using SalesforceSync.Infrastructure.Salesforce;

namespace SalesforceSync.Sync;

/// <summary>
/// The one sync engine for every Salesforce object. <see cref="SyncBatchAsync{TEvent, TRow}"/>
/// runs one batch end to end; the inbox repository (its row type is the type's inbox table) holds each entity's newest
/// event, and the strategy says how to read one, how it maps to Salesforce records and what to read from Salesforce.
/// <list type="number">
/// <item>Takes a global concurrency slot and respects a shared pause (rate limits, all pods).</item>
/// <item>Claims up to <see cref="SyncOptions.BatchSize"/> due entities: each one's inbox row is locked with FOR UPDATE
/// SKIP LOCKED, leased and read, so no two workers in any pod ever process the same entity at once.</item>
/// <item>Per entity, reads the claimed event: the newest one stored (events carry the full record, and a newer event
/// overwrites the entity's row; one not newer than a synced event is never stored). Arrival order plays no
/// part, so events may come out of order, over any partitions. An event that can't be read or mapped is
/// dead-lettered on its own.</item>
/// <item>Writes the units to Salesforce (<see cref="WriteToSalesforceAsync"/>), giving up before the lease runs out:
/// reads the current records (one query per object), then sends what differs, one upsert per object with parents first,
/// and last the records to delete that Salesforce has, in one delete.</item>
/// <item>Records each entity's outcome in one transaction: done, released for the
/// next claim, retried with backoff, or dead-lettered. A Salesforce call that fails as a whole fails the whole batch:
/// every claimed entity is held (the write's outcome is unknown), deferred (rate limited) or retried. An entity whose
/// row a newer event overwrote meanwhile gets no outcome: its row stays pending and due.</item>
/// </list>
/// <para>
/// From an event to Salesforce, step by step (event objects and SObjects are two different classes):
/// </para>
/// <list type="number">
/// <item><b>Event object</b> (<c>AccountEvent</c>, <c>ContactEvent</c>, ...): the inbox payload as the source sent it
/// (camelCase names, its own shape). Only the strategy's <c>Map</c> reads it.</item>
/// <item><b>SObject</b> (<c>AccountSObject</c>, <c>ContactSObject</c>, ...): the same data as Salesforce names it (API
/// field names, <c>[SObject]</c> attributes). <c>Map</c> builds one per Salesforce record and
/// <see cref="SObjectRecord.From{TSObject}"/> turns it into a plain <see cref="SObjectRecord"/>: that is where the event stops
/// and Salesforce's side begins, and the engine never sees an event object again.</item>
/// <item>The records of one entity are its <see cref="SyncUnit"/>.</item>
/// <item><b>Diff</b> (<see cref="Differ.Plan"/>): each record is compared with what the read found in Salesforce, and
/// becomes a create (all its fields), an update (only the fields that differ), a delete or nothing.</item>
/// <item><b>Write</b> (<see cref="Sender.WriteAsync"/>): one upsert per object, parents first, then the deletes.</item>
/// </list>
/// This class is the flow (<see cref="SyncBatchAsync{TEvent, TRow}"/>) and, below it, the steps of
/// <see cref="WriteToSalesforceAsync"/> in the order they run. The pure ones (normalize, build the read, diff) are static
/// and do no I/O; Salesforce is reached only through <see cref="ISalesforceRestClient"/> and the inbox only through
/// <see cref="IRepository{TEntity}"/>, whose every unit of work runs through <see cref="PostgresRetryStrategy"/>.
/// </summary>
public sealed class SalesforceBatchSyncService(
    ISalesforceBatchGate gate,
    ISalesforceRestClient salesforce,
    SalesforceSchemaCache schemaCache,
    SalesforceErrors errors,
    IOptions<SalesforceOptions> salesforceOptions,
    IOptions<SyncOptions> options,
    ILogger<SalesforceBatchSyncService> log)
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

    // The steps of the Salesforce half of a batch, each a class below
    private readonly Describer _describer = new(salesforce, schemaCache, salesforceOptions.Value.SchemaRefreshInterval);
    private readonly Normalizer _normalizer = new(errors);
    private readonly Reader _reader = new(salesforce, schemaCache);
    private readonly Sender _sender = new(salesforce, new Interpreter(errors));

    /// <summary>Claims, writes (upserts and deletes) and records one batch of the strategy's entity type.</summary>
    /// <returns>Inbox rows processed (0 = nothing to do, or no capacity right now).</returns>
    public async Task<int> SyncBatchAsync<TEvent, TRow>(SyncStrategy<TEvent> strategy, IRepository<TRow> inbox, CancellationToken ct)
        where TRow : InboxRow, IInboxRow, new()
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

        // 1. Claim entities and their rows from the inbox table (short transaction; the lease, not a transaction,
        // protects the entities until step 4)
        var claim = await inbox.ClaimAsync(ct);
        if (claim.Events.Count == 0)
        {
            return 0;
        }

        // 2. Per entity: map its event to records
        var batch = PreparedBatch.Prepare(strategy, claim.Events);

        // 3. Salesforce (outside any transaction): read, decide, send, interpret. Not inside one on purpose: Salesforce
        // can't take part in a Postgres transaction (a rollback can't undo a saved write), the execution strategy
        // re-runs its whole delegate on a transient Postgres error (the HTTP calls would be sent again), and a
        // transaction held for up to the request timeout keeps locks, a pooled connection per worker and autovacuum's
        // horizon on a high-churn table. A crash between here and step 4 leaves the rows pending under an expiring
        // lease; the retry reads Salesforce and writes only what still differs.
        Dictionary<string, SyncResult> results;
        try
        {
            results = (await WriteToSalesforceAsync(batch.Units, [.. strategy.Queries([.. batch.Work.Select(w => w.Unit)])],
                TRow.MaxValuesPerQuery, TRow.MaxRecordsPerWrite, leaseCts.Token)).ToDictionary(r => r.Key);
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
        catch (MappingErrorException ex)
        {
            // Nothing was written. The inbox stops (every due row waits) with the error on the rows to fix, so nothing is
            // dead-lettered and the rows go on after the deploy with no replay.
            log.LogError(ex, "{Type} inbox stopped for {Hold} by {Count} mapping error(s), fix the mapping or add the field: {Errors}",
                strategy.EntityType, _opt.MappingErrorHold, ex.Errors.Count,
                string.Join("; ", ex.Errors.Select(e => $"{e.Key}: {e.Error.Code} {e.Error.Message}")));
            await inbox.HaltAsync(claim, [.. ex.Errors.GroupBy(e => e.Key).Select(g => new FailedEvent(g.Key,
                string.Join("; ", g.Select(e => $"{e.Error.Code}: {e.Error.Message}")), InboxRow.MappingErrorCode))],
                $"Held: the {strategy.EntityType} inbox is stopped by a mapping error ({ex.Message})", ct);
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
            // retried. Records that earlier write requests of the batch saved are safe: the retry reads them and writes
            // only what still differs. Shutdown (ct) is not caught: the failed request wrote nothing, and the lease
            // simply expires.
            log.LogError(ex, "{Type} batch failed", strategy.EntityType);
            await inbox.FailAllAsync(claim, ex.Message, (ex as SalesforceResponseException)?.ErrorCode, ct);
            return claim.Events.Count;
        }

        // 4. Record every entity's outcome in the inbox
        LogFailures(strategy.EntityType, batch, results.Values);
        await inbox.RecordAsync(claim, batch.Outcomes(results), ct);
        return claim.Events.Count;
    }

    /// <summary>
    /// The Salesforce half of a batch, for any sObjects, in six steps: describe, normalize, read (one query per object),
    /// diff, send (one upsert per object, parents first, then one delete) and interpret the answers. Returns one outcome
    /// per unit. Nothing here touches the inbox.
    /// </summary>
    /// <remarks>
    /// A call that fails as a whole (the call, not records in it) throws, and the caller fails, holds or defers the whole
    /// claim, even when an earlier call of the batch saved its records: the failed call's outcome decides, so an unknown
    /// one holds every claimed entity. Nothing earlier calls learned is recorded, and nothing needs to be: every write is
    /// matched by external id, so the retry reads Salesforce, finds what they saved and writes only what still differs.
    /// </remarks>
    /// <param name="units">At most <see cref="SyncOptions.MaxEntitiesPerBatch"/>, keyed by entity key.</param>
    /// <param name="queries">The strategy's reads: the units' records and the records their foreign keys point at.</param>
    /// <param name="maxValuesPerQuery">The row type's <see cref="IInboxRow.MaxValuesPerQuery"/>.</param>
    /// <param name="maxRecordsPerWrite">The row type's <see cref="IInboxRow.MaxRecordsPerWrite"/>.</param>
    /// <param name="ct">Cancels the calls; shutdown, not a failure.</param>
    /// <exception cref="UnknownOutcomeException">The write call failed without proving that nothing was applied.</exception>
    internal async Task<List<SyncResult>> WriteToSalesforceAsync(IReadOnlyList<(string Key, SyncUnit Unit)> units,
        IReadOnlyList<SoqlQuery> queries, int maxValuesPerQuery, int maxRecordsPerWrite, CancellationToken ct)
    {
        if (units.Count > _opt.MaxEntitiesPerBatch)
        {
            throw new ArgumentOutOfRangeException(nameof(units), $"At most {_opt.MaxEntitiesPerBatch} units per call");
        }

        var results = new List<SyncResult>(units.Count);
        if (units.Count == 0)
        {
            return results;
        }

        var schemas = await _describer.DescribeAsync(units, queries, ct);

        // A unit Salesforce would reject fails on its own, before any call
        var (normalized, rejected) = _normalizer.Normalize(units, schemas);
        if (MappingErrors(rejected).Count > 0)
        {
            // The cached describe may just be out of date (a field was added to the org): describe again before stopping
            await _describer.InvalidateAsync(schemas.Keys, ct);
            schemas = await _describer.DescribeAsync(units, queries, ct);
            (normalized, rejected) = _normalizer.Normalize(units, schemas);
            await ThrowIfMappingErrorsAsync(rejected, schemas.Keys, ct);
        }

        results.AddRange(rejected);

        if (normalized.Count > 0)
        {
            var current = await _reader.ReadCurrentAsync(_reader.BuildReadQueries(normalized, queries, schemas, maxValuesPerQuery), schemas.Keys, ct);
            var (planned, decided) = Differ.Plan(normalized, current);
            await ThrowIfMappingErrorsAsync(decided, schemas.Keys, ct);
            results.AddRange(decided);

            try
            {
                results.AddRange(await _sender.WriteAsync(planned, maxRecordsPerWrite, ct));
            }
            catch (Exception ex) when (WriteOutcome.IsUnknown(ex))
            {
                // Everything before the first write only read from Salesforce, so its failures are safe to retry. From
                // here on Salesforce may have the write, and may still apply it after we stop waiting.
                throw new UnknownOutcomeException(ex);
            }
        }

        await _describer.InvalidateRecordTypesAsync(units, results, ct);
        return results;
    }

    // ---------- Mapping errors ----------

    /// <summary>
    /// A field the code sends that Salesforce doesn't have, or won't let us set: the mapping is wrong, for every event of
    /// that kind, so only a code change or a field in the org fixes it. Retrying or dead-lettering one entity at a time
    /// would only spend attempts, so the caller stops the inbox instead.
    /// </summary>
    private static bool IsMappingError(SalesforceError e)
    {
        return e.Code is "INVALID_FIELD" or "INVALID_FIELD_FOR_INSERT_UPDATE";
    }

    private static List<(string Key, SalesforceError Error)> MappingErrors(IEnumerable<SyncResult> results)
    {
        return [.. results.SelectMany(r => r.Errors.Where(IsMappingError).Select(e => (r.Key, e)))];
    }

    /// <summary>Stops the batch, before any write, if a unit has a mapping error; the described objects are described again next time.</summary>
    /// <exception cref="MappingErrorException">At least one unit has a mapping error.</exception>
    private async Task ThrowIfMappingErrorsAsync(IEnumerable<SyncResult> results, IEnumerable<string> sobjects, CancellationToken ct)
    {
        if (MappingErrors(results) is { Count: > 0 } found)
        {
            await _describer.InvalidateAsync(sobjects, ct);
            throw new MappingErrorException(found);
        }
    }

    private void LogFailures(string entityType, PreparedBatch batch, IEnumerable<SyncResult> results)
    {
        foreach (var invalid in batch.Invalid)
        {
            log.LogWarning("Dead-lettered event {Id} ({Code}): {Error}", invalid.EntityId, invalid.Code, invalid.Error);
        }

        foreach (var r in results.Where(r => r.Outcome is SyncOutcome.PermanentFailure or SyncOutcome.TransientFailure))
        {
            log.LogWarning("{Type} {Key}: {Outcome} ({Code}) {Errors}", entityType, r.Key, r.Outcome,
                r.PrimaryError?.Code, string.Join("; ", r.Errors));
        }
    }
}

/// <summary>Units whose mapping Salesforce rejects, with what to fix. Thrown before any write.</summary>
internal sealed class MappingErrorException(IReadOnlyList<(string Key, SalesforceError Error)> errors)
    : Exception($"{errors.Count} mapping error(s), e.g. {errors[0].Error.Code}: {errors[0].Error.Message}")
{
    public IReadOnlyList<(string Key, SalesforceError Error)> Errors => errors;
}
