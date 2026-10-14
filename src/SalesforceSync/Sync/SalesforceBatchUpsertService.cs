using System.Text.Json;
using Dapper;
using Microsoft.Extensions.Options;
using SalesforceSync.Data;
using SalesforceSync.Inbox;
using SalesforceSync.Inbox.Payloads;
using SalesforceSync.RateLimiting;
using SalesforceSync.Salesforce;

namespace SalesforceSync.Sync;

/// <summary>A pending inbox row as claimed: the entity it belongs to and the raw event.</summary>
public sealed record InboxEvent(long Id, string EntityId, string Payload, int RetryCount);

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
/// <item>Upserts the units through <see cref="SalesforceRecordUpserter"/> (2 API calls, rate limited), giving up
/// before the lease runs out.</item>
/// <item>Records each entity's outcome in one transaction: done (and the watermark advanced), released for the
/// next claim, retried with backoff, dead-lettered, or held because the write's outcome is unknown.</item>
/// </list>
/// Every Postgres unit of work runs through <see cref="PostgresExecutionStrategy"/>.
/// </summary>
public sealed class SalesforceBatchUpsertService(
    PostgresExecutionStrategy pg,
    SalesforceRecordUpserter salesforce,
    SalesforceRateLimiter limiter,
    SalesforceConcurrencySlots slots,
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

    /// <summary>Same as the strategy overload, with the strategy's parts passed as delegates.</summary>
    public Task<int> UpsertBatchAsync<TEvent>(
        string entityType,
        string inboxTable,
        Func<TEvent, string?> entityId,
        Func<TEvent, DateTimeOffset?> eventTime,
        Func<TEvent, EventType?> eventType,
        Func<string, TEvent, UpsertUnit> map,
        Func<IReadOnlyCollection<UpsertUnit>, IEnumerable<SoqlQuery>> queries,
        CancellationToken ct)
        => UpsertBatchAsync(new UpsertStrategy<TEvent>(entityType, inboxTable, entityId, eventTime, eventType, map, queries), ct);

    /// <summary>Claims, upserts and records one batch of the strategy's entity type.</summary>
    /// <returns>Inbox rows processed (0 = nothing to do, or no capacity right now).</returns>
    public async Task<int> UpsertBatchAsync<TEvent>(UpsertStrategy<TEvent> strategy, CancellationToken ct)
    {
        await using var slot = await slots.TryAcquireAsync(ct);
        if (slot is null || await limiter.IsPausedAsync(ct)) return 0;

        var sql = InboxSql.For(strategy.InboxTable);

        // Give up on Salesforce before the lease runs out, so another worker never claims an entity while this one
        // can still write to it. Started before the claim, so the margin is measured from when the lease began.
        using var leaseCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        leaseCts.CancelAfter(_opt.Lease * LeaseUsableFraction);

        // 1. Claim (short transaction)
        var claim = await ClaimAsync(sql, strategy.EntityType, ct);
        if (claim.Events.Count == 0) return 0;

        // 2. Per entity: read its events, keep the newest, drop it if it isn't newer than what is synced, map it
        var batch = Prepare(strategy, claim.Events, claim.Synced);

        // 3. Salesforce (outside any transaction)
        Dictionary<string, UpsertResult> results;
        try
        {
            results = batch.Work.Count == 0
                ? []
                : (await salesforce.UpsertAsync(batch.Work.Select(w => (w.EntityId, w.Unit)).ToList(),
                    strategy.Queries(batch.Work.Select(w => w.Unit).ToList()).ToList(), leaseCts.Token))
                    .ToDictionary(r => r.Key);
        }
        catch (UnknownOutcomeException ex)
        {
            // Salesforce may still apply the write. Caught on shutdown too: an entity released now could be written
            // by another worker before the lost write lands. If the hold can't be recorded (or the process is killed
            // first), the lease is what still protects the entities.
            log.LogWarning(ex, "{Type} batch: holding {Count} events for {Hold}", strategy.EntityType, claim.Events.Count, _opt.UnknownOutcomeHold);
            using var recording = new CancellationTokenSource(HoldRecordingTimeout);
            await pg.ExecuteAsync((conn, t) => conn.ExecuteAsync(new CommandDefinition(sql.Hold, new
            {
                ids = claim.EventIds, token = claim.Token, hold = _opt.UnknownOutcomeHold,
                error = ex.Message, code = UnknownOutcomeException.Code
            }, cancellationToken: t)), recording.Token);
            ct.ThrowIfCancellationRequested();
            return claim.Events.Count;
        }
        catch (RateLimitedException ex)
        {
            log.LogInformation("Rate limited; deferring {Count} {Type} events for {Delay}", claim.Events.Count, strategy.EntityType, ex.RetryAfter);
            await pg.ExecuteAsync((conn, t) => conn.ExecuteAsync(new CommandDefinition(sql.Defer,
                new { ids = claim.EventIds, token = claim.Token, delay = ex.RetryAfter }, cancellationToken: t)), ct);
            return 0;
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            // The request failed before anything was written (describe, the read, or a write Salesforce provably did
            // not apply): every entity may succeed on retry. Shutdown (ct) is not caught: nothing was written, and
            // the lease simply expires.
            log.LogError(ex, "{Type} batch failed", strategy.EntityType);
            var all = new FailedRows();
            all.Add(claim.EventIds, ex.Message, ex is SalesforceQueryException q ? q.Errors.FirstOrDefault()?.Code : null);
            await pg.ExecuteAsync((conn, t) => conn.ExecuteAsync(new CommandDefinition(sql.Fail,
                FailArgs(all, claim.Token), cancellationToken: t)), ct);
            return claim.Events.Count;
        }

        // 4. Record outcomes
        await RecordOutcomesAsync(sql, strategy.EntityType, claim.Token, batch, results, ct);

        foreach (var (id, error) in batch.Invalid)
            log.LogWarning("Dead-lettered event {Id} ({Code}): {Error}", id, SalesforceErrors.InvalidEvent, error);
        foreach (var r in results.Values.Where(r => r.Outcome is SyncOutcome.PermanentFailure or SyncOutcome.TransientFailure))
            log.LogWarning("{Type} {Key}: {Outcome} ({Code}) {Errors}", strategy.EntityType, r.Key, r.Outcome,
                r.PrimaryError?.Code, string.Join("; ", r.Errors));

        return claim.Events.Count;
    }

    /// <summary>
    /// Locks and leases a batch, and reads the claimed entities' watermarks in the same transaction. A fresh token per
    /// attempt: if a commit's outcome is unknown and the claim is retried, rows leased by the lost attempt are not
    /// mistaken for this one's. The retry releases them first, so they don't wait for their lease to expire (and a
    /// lease nobody works on is never taken over).
    /// </summary>
    private Task<Claim> ClaimAsync(InboxSql sql, string entityType, CancellationToken ct)
    {
        var attempted = new List<Guid>();
        return pg.ExecuteInTransactionAsync(async (conn, tx, t) =>
        {
            if (attempted.Count > 0)
                await conn.ExecuteAsync(new CommandDefinition(sql.ReleaseLostClaims,
                    new { tokens = attempted.ToArray() }, tx, cancellationToken: t));

            var token = Guid.NewGuid();
            attempted.Add(token);
            var entityIds = (await conn.QueryAsync<string>(new CommandDefinition(sql.LockHeads, new
            {
                window = InboxSql.ClaimWindow,
                batchSize = Math.Min(_opt.BatchSize, SalesforceRestClient.MaxEntitiesPerBatch)
            }, tx, cancellationToken: t))).ToArray();
            if (entityIds.Length == 0) return new Claim(token, [], new Dictionary<string, long>());

            var events = (await conn.QueryAsync<InboxEvent>(new CommandDefinition(sql.ClaimEntities,
                new { entityIds, token, lease = _opt.Lease, maxPerEntity = _opt.MaxEventsPerEntity }, tx, cancellationToken: t))).ToList();
            var synced = (await conn.QueryAsync<(string EntityId, long Time)>(new CommandDefinition(InboxSql.SyncedTimes,
                    new { entityType, entityIds }, tx, cancellationToken: t)))
                .ToDictionary(r => r.EntityId, r => r.Time, StringComparer.Ordinal);
            return new Claim(token, events, synced);
        }, ct);
    }

    /// <summary>
    /// Decides, per claimed entity, what to do with its rows. Rows that can't be read (or whose event is about another
    /// entity than the row says) are invalid on their own. Of the rest only the newest event counts
    /// (<see cref="Coalesce{TEvent}"/>): the others are superseded, and the newest too if its time is not after the
    /// entity's watermark (a replay, a redelivery, or an older event that arrived late). Otherwise it is mapped to
    /// the unit to write; if it can't be mapped, it is invalid.
    /// </summary>
    /// <param name="synced">Entity id -> the time (microseconds since the epoch) of its newest synced event.</param>
    internal static PreparedBatch Prepare<TEvent>(UpsertStrategy<TEvent> strategy, IReadOnlyList<InboxEvent> rows,
        IReadOnlyDictionary<string, long> synced)
    {
        var superseded = new List<long>();
        var work = new List<Work>();
        var invalid = new List<(long Id, string Error)>();

        foreach (var entity in rows.GroupBy(e => e.EntityId, StringComparer.Ordinal))
        {
            var events = new List<(InboxEvent Row, ReadEvent<TEvent> Read)>();
            foreach (var row in entity)
            {
                if (!strategy.TryRead(row.Payload, out var read, out var problem))
                    invalid.Add((row.Id, $"{strategy.EntityType} {entity.Key}: {problem}"));
                else if (!string.Equals(read.EntityId, entity.Key, StringComparison.Ordinal))
                    // The lease is on the row's entity; writing another entity under it would bypass that entity's own
                    invalid.Add((row.Id, $"{strategy.EntityType} {entity.Key}: the event is about '{read.EntityId}', but the row's entity_id is '{entity.Key}'"));
                else
                    events.Add((row, read));
            }
            if (events.Count == 0) continue;

            var newest = Coalesce(events);
            superseded.AddRange(events.Where(e => e.Row.Id != newest.Row.Id).Select(e => e.Row.Id));

            var time = ToMicroseconds(newest.Read.Time);
            if (synced.TryGetValue(entity.Key, out var syncedTime) && time <= syncedTime)
            {
                superseded.Add(newest.Row.Id);
                continue;
            }

            try
            {
                work.Add(new Work(entity.Key, strategy.Map(entity.Key, newest.Read.Event) with { EventType = newest.Read.Type }, time, newest.Row.Id));
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                invalid.Add((newest.Row.Id, $"{strategy.EntityType} {entity.Key}: {ex.Message}"));
            }
        }
        return new PreparedBatch(superseded, work, invalid);
    }

    /// <summary>
    /// An entity's claimed events become one write: the event with the newest time. Every event carries the full
    /// record, so the newest one is the whole desired state and the older ones add nothing (merging them in would
    /// keep a field the newest event no longer has). Two events with the same time: the one stored later (the higher
    /// row id) counts as newer.
    /// </summary>
    public static (InboxEvent Row, ReadEvent<TEvent> Read) Coalesce<TEvent>(IReadOnlyList<(InboxEvent Row, ReadEvent<TEvent> Read)> events)
    {
        var newest = events[0];
        for (var i = 1; i < events.Count; i++)
        {
            var e = events[i];
            if (e.Read.Time > newest.Read.Time || (e.Read.Time == newest.Read.Time && e.Row.Id > newest.Row.Id)) newest = e;
        }
        return newest;
    }

    /// <summary>An event time as the watermark stores it: microseconds since the Unix epoch.</summary>
    public static long ToMicroseconds(DateTimeOffset time) => (time - DateTimeOffset.UnixEpoch).Ticks / TimeSpan.TicksPerMicrosecond;

    /// <summary>
    /// Records every entity's outcome in one transaction, one statement per kind of outcome (not per entity).
    /// Safe to repeat: every statement is conditional on the lease token.
    /// </summary>
    private Task RecordOutcomesAsync(InboxSql sql, string entityType, Guid token, PreparedBatch batch,
        IReadOnlyDictionary<string, UpsertResult> results, CancellationToken ct)
    {
        var done = new List<Work>();
        var released = new List<long>();
        var dead = new FailedRows();
        var failed = new FailedRows();
        foreach (var (id, error) in batch.Invalid) dead.Add([id], error, SalesforceErrors.InvalidEvent);
        foreach (var w in batch.Work)
        {
            var r = results[w.EntityId];
            switch (r.Outcome)
            {
                case SyncOutcome.Saved or SyncOutcome.Unchanged: done.Add(w); break;
                case SyncOutcome.NotAttempted: released.Add(w.EventId); break;
                case SyncOutcome.PermanentFailure: dead.Add([w.EventId], string.Join("; ", r.Errors), r.PrimaryError?.Code); break;
                default: failed.Add([w.EventId], string.Join("; ", r.Errors), r.PrimaryError?.Code); break;
            }
        }

        return pg.ExecuteInTransactionAsync(async (conn, tx, t) =>
        {
            Task Run(string statement, object args) => conn.ExecuteAsync(new CommandDefinition(statement, args, tx, cancellationToken: t));

            if (done.Count > 0)
            {
                // Watermarks first: Complete releases the lease that MarkSynced checks
                await Run(sql.MarkSynced, new
                {
                    entityType, token, entityIds = done.Select(w => w.EntityId).ToArray(), times = done.Select(w => w.Time).ToArray()
                });
                await Run(sql.Complete, new { ids = done.Select(w => w.EventId).ToArray(), token, status = "done" });
            }
            if (batch.SupersededIds.Count > 0)
                await Run(sql.Complete, new { ids = batch.SupersededIds.ToArray(), token, status = "superseded" });
            if (released.Count > 0)
                await Run(sql.Release, new { ids = released.ToArray(), token });
            if (dead.Count > 0)
                await Run(sql.Dead, new { ids = dead.Ids.ToArray(), errors = dead.ErrorsJson(), token });
            if (failed.Count > 0)
                await Run(sql.Fail, FailArgs(failed, token));
        }, ct);
    }

    private object FailArgs(FailedRows rows, Guid token) => new
    {
        ids = rows.Ids.ToArray(),
        errors = rows.ErrorsJson(),
        token,
        maxRetries = _opt.MaxRetries,
        baseDelaySecs = _opt.RetryBaseDelay.TotalSeconds,
        maxDelaySecs = _opt.MaxRetryDelay.TotalSeconds
    };

    /// <summary>Inbox rows with the error to store on each, as InboxSql.Fail / Dead take them.</summary>
    private sealed class FailedRows
    {
        private readonly Dictionary<string, string?[]> _errors = new(StringComparer.Ordinal);

        public List<long> Ids { get; } = [];
        public int Count => Ids.Count;

        public void Add(IEnumerable<long> ids, string error, string? code)
        {
            foreach (var id in ids)
            {
                Ids.Add(id);
                _errors[id.ToString(System.Globalization.CultureInfo.InvariantCulture)] = [error, code];
            }
        }

        /// <summary>{ "row id": [error, code] }</summary>
        public string ErrorsJson() => JsonSerializer.Serialize(_errors);
    }

    private sealed record Claim(Guid Token, List<InboxEvent> Events, Dictionary<string, long> Synced)
    {
        public long[] EventIds => Events.Select(e => e.Id).ToArray();
    }

    /// <summary>An entity to write: the unit mapped from its newest event (row <paramref name="EventId"/>) and that event's time.</summary>
    internal sealed record Work(string EntityId, UpsertUnit Unit, long Time, long EventId);

    /// <summary>What to do with a claim's rows: superseded ones, entities to write, and rows that are invalid on their own.</summary>
    internal sealed record PreparedBatch(List<long> SupersededIds, List<Work> Work, List<(long Id, string Error)> Invalid);
}
