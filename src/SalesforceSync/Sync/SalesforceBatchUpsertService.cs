using System.Text.Json;
using Microsoft.Extensions.Options;
using SalesforceSync.Data;
using SalesforceSync.Inbox;
using SalesforceSync.Inbox.Payloads;
using SalesforceSync.RateLimiting;
using SalesforceSync.Salesforce;
using SalesforceSync.Sync.Upsert;

namespace SalesforceSync.Sync;

/// <summary>
/// The one sync engine for every Salesforce object. <see cref="UpsertBatchAsync{TEvent}(UpsertStrategy{TEvent}, CancellationToken)"/>
/// runs one batch end to end; the strategy says where the type's events are, how to read one, how it maps to
/// Salesforce records and what to read from Salesforce.
/// <list type="number">
/// <item>Takes a global concurrency slot and respects a shared pause (rate limits, all pods).</item>
/// <item>Claims up to <see cref="SyncOptions.BatchSize"/> entities from the strategy's inbox table: per entity, the head
/// row is locked with FOR UPDATE SKIP LOCKED and its oldest pending rows are leased, so no two workers in any pod
/// ever process the same entity at once.</item>
/// <item>Per entity, reads the claimed events and keeps the one with the newest event time (events carry the full
/// record); the others are superseded, and so is the newest if it is not newer than what was already synced. Arrival
/// order plays no part, so events may come out of order, over any partitions. An event that can't be read or
/// mapped is dead-lettered on its own.</item>
/// <item>Writes the units to Salesforce in 2 API calls (<see cref="WriteToSalesforceAsync"/>), giving up before the
/// lease runs out: one Composite request reads the current records, one writes what differs with sObject Collections.</item>
/// <item>Records each entity's outcome in one transaction: done (and the watermark advanced), released for the
/// next claim, retried with backoff, dead-lettered, or held because the write's outcome is unknown.</item>
/// </list>
/// This class is the flow. The rules it applies are small classes without I/O (<see cref="BatchPreparer"/> and the
/// ones in <c>Sync/Upsert</c>); Salesforce is reached only through <see cref="SalesforceRestClient"/> and the inbox only
/// through <see cref="IInboxRepository"/>, whose every unit of work runs through <see cref="PostgresExecutionStrategy"/>.
/// </summary>
public sealed class SalesforceBatchUpsertService(
    ISalesforceBatchGate gate,
    IInboxRepository inbox,
    SalesforceRestClient salesforce,
    SalesforceSchemaCache schemaCache,
    SalesforceErrors errors,
    WriteEchoTracker echoTracker,
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
    private readonly WritePlanner _planner = new(salesforceOptions.Value, options.Value);
    private readonly WriteResultInterpreter _interpreter = new(errors);

    /// <summary>Claims, upserts and records one batch of the strategy's entity type.</summary>
    /// <returns>Inbox rows processed (0 = nothing to do, or no capacity right now).</returns>
    public async Task<int> UpsertBatchAsync<TEvent>(UpsertStrategy<TEvent> strategy, CancellationToken ct)
    {
        await using var admission = await gate.TryEnterAsync(ct);
        if (admission is null) return 0;

        // Give up on Salesforce before the lease runs out, so another worker never claims an entity while this one
        // can still write to it. Started before the claim, so the margin is measured from when the lease began.
        using var leaseCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        leaseCts.CancelAfter(_opt.Lease * LeaseUsableFraction);

        // 1. Claim events from the strategy's inbox table (short transaction)
        var claim = await inbox.ClaimAsync(strategy.InboxTable, strategy.EntityType, ct);
        if (claim.Events.Count == 0) return 0;

        // 2. Per entity: keep the newest event, drop it if it isn't newer than what is synced, map it to records
        var batch = BatchPreparer.Prepare(strategy, claim.Events, claim.SyncedTimes);

        // 3. Salesforce (outside any transaction): read, decide, send, interpret
        Dictionary<string, UpsertResult> results;
        try
        {
            results = batch.Work.Count == 0
                ? []
                : (await WriteToSalesforceAsync(batch.Units, strategy.Queries(batch.Work.Select(w => w.Unit).ToList()).ToList(),
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
            // The request failed before anything was written (describe, the read, or a write Salesforce provably did
            // not apply): every entity may succeed on retry. Shutdown (ct) is not caught: nothing was written, and
            // the lease simply expires.
            log.LogError(ex, "{Type} batch failed", strategy.EntityType);
            await inbox.FailAllAsync(claim, ex.Message, ex is SalesforceQueryException q ? q.Errors.FirstOrDefault()?.Code : null, ct);
            return claim.Events.Count;
        }

        // 4. Record every entity's outcome in the inbox
        var outcomes = batch.Outcomes(results);
        LogFailures(strategy.EntityType, batch, results.Values);
        if (outcomes.Held.Count == 0)
        {
            await inbox.RecordAsync(claim, outcomes, ct);
        }
        else
        {
            // Some entities are held (a later write's outcome is unknown): like a whole-claim hold, that must be
            // recorded even on shutdown
            using var recording = new CancellationTokenSource(HoldRecordingTimeout);
            await inbox.RecordAsync(claim, outcomes, recording.Token);
            ct.ThrowIfCancellationRequested();
        }

        return claim.Events.Count;
    }

    /// <summary>
    /// The Salesforce half of a batch, for any sObjects, in two API calls: one Composite request reads the current
    /// records, one writes what differs with sObject Collections (more than 5 collections are split over further
    /// requests). Returns one outcome per unit. Nothing here touches the inbox.
    /// </summary>
    /// <param name="units">At most <see cref="ReadQueryBuilder.MaxEntitiesPerBatch"/>, keyed by entity key.</param>
    /// <param name="queries">The strategy's reads: the units' records and the records their lookups point at.</param>
    /// <exception cref="UnknownOutcomeException">The write call failed without proving that nothing was applied.</exception>
    internal async Task<List<UpsertResult>> WriteToSalesforceAsync(IReadOnlyList<(string Key, UpsertUnit Unit)> units,
        IReadOnlyList<SoqlQuery> queries, CancellationToken ct)
    {
        if (units.Count > ReadQueryBuilder.MaxEntitiesPerBatch)
            throw new ArgumentOutOfRangeException(nameof(units), $"At most {ReadQueryBuilder.MaxEntitiesPerBatch} units per call");

        var results = new List<UpsertResult>(units.Count);
        if (units.Count == 0) return results;

        // 1. Field metadata of every object involved (cached per process, revalidated with If-Modified-Since)
        var schemas = await DescribeAsync(units, queries, ct);

        // 2. Normalize; a unit Salesforce would reject fails on its own without reaching the query
        var (normalized, rejected) = RecordNormalizer.Normalize(units, schemas);
        results.AddRange(rejected);
        if (normalized.Count == 0) return results;

        // 3. Read: one Composite call with the strategy's queries
        var current = await ReadCurrentAsync(ReadQueryBuilder.Build(normalized, queries, schemas), schemas.Keys, ct);

        // 4. Decide per record: unchanged, create or update (checked against the event's type), and how each lookup
        // is linked; the writes are grouped into collections and Composite requests
        var plan = _planner.Plan(normalized, current, schemas);
        results.AddRange(plan.NotSent);
        WarnAboutFlappingFields(plan.Rewrites);
        LogTypeMismatches(plan, normalized.Count);

        // 5. Send: normally one Composite call. 6. Per-record results -> one outcome per unit.
        var saved = new List<WriteEcho>();
        for (var i = 0; i < plan.Requests.Count; i++)
        {
            var request = plan.Requests[i];
            try
            {
                var answers = await SendAsync(request, ct);
                results.AddRange(_interpreter.Interpret(request, answers, saved));
            }
            // A failed first call fails the batch (the caller fails, holds or defers the whole claim): nothing of it
            // was written. A later call's failure costs only its own entities: the earlier calls' records are final.
            catch (Exception ex) when (i > 0)
            {
                var stop = LaterCallFailed(ex, request, plan.Requests.Skip(i + 1), results);
                if (stop) break;
            }
        }
        echoTracker.Record(saved);

        return results;
    }

    /// <summary>
    /// The outcomes of a write call after the first that threw: its entities are held if its outcome is unknown,
    /// deferred if it was rate limited (and so are the calls after it), otherwise retried with backoff.
    /// </summary>
    /// <returns>True if no further call may be sent.</returns>
    private static bool LaterCallFailed(Exception ex, CompositeWrite failed, IEnumerable<CompositeWrite> notSent, List<UpsertResult> results)
    {
        IEnumerable<UpsertResult> For(CompositeWrite request, SyncOutcome outcome, IReadOnlyList<SalesforceError> errors, TimeSpan? retryAfter = null)
            => request.Units.Select(u => new UpsertResult(u.Key, outcome, errors, retryAfter));

        switch (ex)
        {
            case UnknownOutcomeException:
                // Stop: nothing more is sent while Salesforce may still be applying this call
                results.AddRange(For(failed, SyncOutcome.OutcomeUnknown, [new SalesforceError(null, UnknownOutcomeException.Code, ex.Message)]));
                results.AddRange(notSent.SelectMany(r => For(r, SyncOutcome.NotAttempted, [])));
                return true;
            case RateLimitedException limited:
                results.AddRange(new[] { failed }.Concat(notSent).SelectMany(r => For(r, SyncOutcome.RateLimited, [], limited.RetryAfter)));
                return true;
            default:
                // Salesforce provably did not apply it
                results.AddRange(For(failed, SyncOutcome.TransientFailure, [new SalesforceError(null, "UNKNOWN_ERROR", ex.Message)]));
                return false;
        }
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
            schemas[sobject] = await schemaCache.GetAsync(sobject, _salesforceOpt.SchemaRefreshInterval,
                (since, token) => salesforce.DescribeAsync(sobject, since, token), ct);
        return schemas;
    }

    private async Task<CurrentRecords> ReadCurrentAsync(IReadOnlyList<ReadQuery> queries, IEnumerable<string> describedSObjects,
        CancellationToken ct)
    {
        if (queries.Count == 0) return CurrentRecords.None;
        try
        {
            var rows = await salesforce.QueryAsync(queries.Select(q => q.Query).ToList(), ct);
            return CurrentRecords.From(rows, queries);
        }
        catch (SalesforceQueryException ex) when (ex.Errors.Any(e => e.Code == "INVALID_FIELD"))
        {
            // The cached describe is out of date (a field was removed or renamed): describe again next time
            foreach (var sobject in describedSObjects) schemaCache.Invalidate(sobject);
            throw;
        }
    }

    private async Task<IReadOnlyList<CollectionResult>> SendAsync(CompositeWrite request, CancellationToken ct)
    {
        try
        {
            return await salesforce.SendCollectionsAsync(request.BuildRequests(), _salesforceOpt.AllOrNone, ct);
        }
        catch (Exception ex) when (WriteOutcome.IsUnknown(ex))
        {
            // Everything before this point only read from Salesforce, so its failures are safe to retry. From here
            // on Salesforce may have the write, and may still apply it after we stop waiting.
            throw new UnknownOutcomeException(ex);
        }
    }

    private void LogTypeMismatches(WritePlan plan, int total)
    {
        if (plan.TypeMismatches.Count == 0) return;

        var deadLettered = plan.MismatchHandling == EventTypeMismatch.DeadLetter;
        if (!deadLettered)
            foreach (var (key, message) in plan.TypeMismatches)
                log.LogDebug("{Key}: {Mismatch}; going by Salesforce", key, message);
        log.LogInformation(
            "{Count} of {Total} events had a type that disagrees with Salesforce (inserted but the record exists, or updated but it does not): {Action}",
            plan.TypeMismatches.Count, total, deadLettered ? "dead-lettered" : "reconciled with Salesforce");
    }

    /// <summary>Warns about a field that still differs after the same value was written to it before (<see cref="WriteEchoTracker"/>).</summary>
    private void WarnAboutFlappingFields(IReadOnlyList<FieldWrite> rewrites)
    {
        foreach (var (echo, stored, fieldType) in rewrites)
            if (echoTracker.IsFlapping(echo, stored!))
                log.LogWarning(
                    "{Object}.{Field} on {ExternalId} keeps differing after it is written: sent {Sent}, Salesforce stores {Stored}. " +
                    "Every event rewrites this field: org automation (a trigger or flow) changes the value, or FieldValues " +
                    "is missing a normalization rule for '{Type}' fields.",
                    echo.Object, echo.Field, echo.ExternalId, echo.Value, stored, fieldType);
    }

    private void LogFailures(string entityType, PreparedBatch batch, IEnumerable<UpsertResult> results)
    {
        foreach (var (id, error) in batch.Invalid)
            log.LogWarning("Dead-lettered event {Id} ({Code}): {Error}", id, SalesforceErrors.InvalidEvent, error);
        foreach (var r in results.Where(r => r.Outcome is SyncOutcome.PermanentFailure or SyncOutcome.TransientFailure or SyncOutcome.OutcomeUnknown))
            log.LogWarning("{Type} {Key}: {Outcome} ({Code}) {Errors}", entityType, r.Key, r.Outcome,
                r.PrimaryError?.Code, string.Join("; ", r.Errors));
        if (results.FirstOrDefault(r => r.Outcome == SyncOutcome.RateLimited) is { } limited)
            log.LogInformation("Rate limited; deferring {Count} {Type} events for {Delay}",
                results.Count(r => r.Outcome == SyncOutcome.RateLimited), entityType, limited.RetryAfter);
    }
}
