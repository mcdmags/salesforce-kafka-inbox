using System.Text.Json;
using Dapper;
using Microsoft.Extensions.Options;
using SalesforceSync.Data;
using SalesforceSync.Inbox;
using SalesforceSync.RateLimiting;
using SalesforceSync.Salesforce;

namespace SalesforceSync.Sync;

/// <summary>
/// A pending inbox row as claimed: <paramref name="Key"/> and <paramref name="Version"/> are read from the raw
/// <paramref name="Payload"/> by sf_event_key / sf_event_version (meta.time in microseconds since the epoch).
/// </summary>
public sealed record InboxEvent(long Id, string Key, long Version, string Payload, int RetryCount);

/// <summary>
/// The one sync engine for every Salesforce object. <see cref="UpsertBatchAsync{TEvent}(UpsertStrategy{TEvent}, CancellationToken)"/>
/// runs one batch end to end; the strategy only says how an inbox event maps to Salesforce records and what to read.
/// <list type="number">
/// <item>Takes a global concurrency slot and respects a shared pause (rate limits, all pods).</item>
/// <item>Claims up to <see cref="SyncOptions.BatchSize"/> entities of the strategy's type: per entity, the head row
/// is locked with FOR UPDATE SKIP LOCKED and all its pending rows are leased, so no two workers in any pod ever
/// process the same entity at once.</item>
/// <item>Supersedes events at or below the entity's synced version (late or replayed), coalesces the rest to the
/// newest one (events carry the full record) and maps it through the strategy; a payload that can't be mapped is
/// dead-lettered on its own.</item>
/// <item>Upserts the units through <see cref="SalesforceRecordUpserter"/> (2 API calls, rate limited), giving up
/// before the lease runs out.</item>
/// <item>Records each entity's outcome in one transaction: done (and the watermark advanced), released for the
/// next claim, retried with backoff, or dead-lettered.</item>
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
    /// Share of the lease a batch may spend before its Salesforce call is cancelled; the rest is margin for a
    /// cancelled request still finishing in Salesforce and for recording the outcome.
    /// </summary>
    private const double LeaseUsableFraction = 0.8;

    private readonly SyncOptions _opt = options.Value;

    /// <summary>Same as the strategy overload, with the strategy's parts passed as delegates.</summary>
    public Task<int> UpsertBatchAsync<TEvent>(
        string entityType,
        Func<string, TEvent, UpsertUnit> map,
        Func<IReadOnlyCollection<UpsertUnit>, IEnumerable<SoqlQuery>> queries,
        CancellationToken ct)
        => UpsertBatchAsync(new UpsertStrategy<TEvent>(entityType, map, queries), ct);

    /// <summary>Claims, upserts and records one batch of the strategy's entity type.</summary>
    /// <returns>Inbox rows processed (0 = nothing to do, or no capacity right now).</returns>
    public async Task<int> UpsertBatchAsync<TEvent>(UpsertStrategy<TEvent> strategy, CancellationToken ct)
    {
        await using var slot = await slots.TryAcquireAsync(ct);
        if (slot is null || await limiter.IsPausedAsync(ct)) return 0;

        // Give up on Salesforce before the lease runs out, so another worker never claims an entity while this one
        // can still write to it. Started before the claim, so the margin is measured from when the lease began.
        using var leaseCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        leaseCts.CancelAfter(_opt.Lease * LeaseUsableFraction);

        // 1. Claim (short transaction)
        var claim = await ClaimAsync(strategy.EntityType, ct);
        if (claim.Events.Count == 0) return 0;

        // 2. Supersede stale events, keep the newest of the rest per entity, map it to Salesforce records
        var batch = Prepare(strategy, claim);

        // 3. Salesforce (outside any transaction)
        Dictionary<string, UpsertResult> results;
        try
        {
            results = batch.Work.Count == 0
                ? []
                : (await salesforce.UpsertAsync(batch.Work.Select(w => (w.Key, w.Unit)).ToList(),
                    strategy.Queries(batch.Work.Select(w => w.Unit).ToList()).ToList(), leaseCts.Token))
                    .ToDictionary(r => r.Key);
        }
        catch (RateLimitedException ex)
        {
            log.LogInformation("Rate limited; deferring {Count} {Type} events for {Delay}", claim.Events.Count, strategy.EntityType, ex.RetryAfter);
            await pg.ExecuteAsync((conn, t) => conn.ExecuteAsync(new CommandDefinition(InboxSql.Defer,
                new { ids = claim.EventIds, token = claim.Token, delay = ex.RetryAfter }, cancellationToken: t)), ct);
            return 0;
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            // The request as a whole failed (network, 5xx, timeout, lease margin reached): every entity may succeed
            // on retry. Shutdown (ct) is not caught: the lease is left to expire, because a cancelled request may
            // still be running in Salesforce.
            log.LogError(ex, "{Type} batch failed", strategy.EntityType);
            var all = new FailedRows();
            all.Add(claim.EventIds, ex.Message, ex is SalesforceQueryException q ? q.Errors.FirstOrDefault()?.Code : null);
            await pg.ExecuteAsync((conn, t) => conn.ExecuteAsync(new CommandDefinition(InboxSql.Fail,
                FailArgs(all, claim.Token), cancellationToken: t)), ct);
            return claim.Events.Count;
        }

        // 4. Record outcomes
        await RecordOutcomesAsync(strategy.EntityType, claim.Token, batch, results, ct);

        foreach (var (ids, error) in batch.Invalid)
            log.LogWarning("Dead-lettered {Count} events ({Code}): {Error}", ids.Length, SalesforceErrors.InvalidEvent, error);
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
    private Task<Claim> ClaimAsync(string entityType, CancellationToken ct)
    {
        var attempted = new List<Guid>();
        return pg.ExecuteInTransactionAsync(async (conn, tx, t) =>
        {
            if (attempted.Count > 0)
                await conn.ExecuteAsync(new CommandDefinition(InboxSql.ReleaseLostClaims,
                    new { entityType, tokens = attempted.ToArray() }, tx, cancellationToken: t));

            var token = Guid.NewGuid();
            attempted.Add(token);
            var keys = (await conn.QueryAsync<string>(new CommandDefinition(InboxSql.LockHeads,
                new { entityType, batchSize = Math.Min(_opt.BatchSize, SalesforceRestClient.MaxGraphsPerRequest) }, tx,
                cancellationToken: t))).ToArray();
            if (keys.Length == 0) return new Claim(token, [], new Dictionary<string, long>());

            var events = (await conn.QueryAsync<InboxEvent>(new CommandDefinition(InboxSql.ClaimKeys,
                new { entityType, keys, token, lease = _opt.Lease }, tx, cancellationToken: t))).ToList();
            var synced = (await conn.QueryAsync<(string Key, long Version)>(new CommandDefinition(InboxSql.SyncedVersions,
                    new { entityType, keys }, tx, cancellationToken: t)))
                .ToDictionary(r => r.Key, r => r.Version, StringComparer.Ordinal);
            return new Claim(token, events, synced);
        }, ct);
    }

    /// <summary>
    /// Splits the claimed events into stale ones (at or below the entity's watermark), entities whose newest payload
    /// maps to a unit, and entities whose payload can't be read or mapped (dead-lettered on their own).
    /// </summary>
    private static PreparedBatch Prepare<TEvent>(UpsertStrategy<TEvent> strategy, Claim claim)
    {
        var stale = new List<long>();
        var current = new List<InboxEvent>(claim.Events.Count);
        foreach (var e in claim.Events)
        {
            if (claim.Synced.TryGetValue(e.Key, out var synced) && e.Version <= synced) stale.Add(e.Id);
            else current.Add(e);
        }

        var work = new List<Work>();
        var invalid = new List<(long[] Ids, string Error)>();
        foreach (var group in current.GroupBy(e => e.Key, StringComparer.Ordinal))
        {
            var events = group.ToList();
            var ids = events.Select(e => e.Id).ToArray();
            try
            {
                var newest = Coalesce(events);
                var payload = JsonSerializer.Deserialize<TEvent>(newest.Payload, EventJson) ?? throw new JsonException("Payload is null");
                work.Add(new Work(group.Key, strategy.Map(group.Key, payload), newest.Version, ids));
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                invalid.Add((ids, $"{strategy.EntityType} {group.Key}: {ex.Message}"));
            }
        }
        return new PreparedBatch(stale.ToArray(), work, invalid);
    }

    /// <summary>
    /// Records every entity's outcome in one transaction, one statement per kind of outcome (not per entity).
    /// Safe to repeat: every statement is conditional on the lease token.
    /// </summary>
    private Task RecordOutcomesAsync(string entityType, Guid token, PreparedBatch batch,
        IReadOnlyDictionary<string, UpsertResult> results, CancellationToken ct)
    {
        var done = new List<Work>();
        var released = new List<long>();
        var dead = new FailedRows();
        var failed = new FailedRows();
        foreach (var (ids, error) in batch.Invalid) dead.Add(ids, error, SalesforceErrors.InvalidEvent);
        foreach (var w in batch.Work)
        {
            var r = results[w.Key];
            switch (r.Outcome)
            {
                case SyncOutcome.Saved or SyncOutcome.Unchanged: done.Add(w); break;
                case SyncOutcome.NotAttempted: released.AddRange(w.EventIds); break;
                case SyncOutcome.PermanentFailure: dead.Add(w.EventIds, string.Join("; ", r.Errors), r.PrimaryError?.Code); break;
                default: failed.Add(w.EventIds, string.Join("; ", r.Errors), r.PrimaryError?.Code); break;
            }
        }

        return pg.ExecuteInTransactionAsync(async (conn, tx, t) =>
        {
            Task Run(string sql, object args) => conn.ExecuteAsync(new CommandDefinition(sql, args, tx, cancellationToken: t));

            if (done.Count > 0)
            {
                // Watermarks first: Complete releases the lease that MarkSynced checks
                await Run(InboxSql.MarkSynced, new
                {
                    entityType, token, keys = done.Select(w => w.Key).ToArray(), versions = done.Select(w => w.Version).ToArray()
                });
                await Run(InboxSql.Complete, new { ids = done.SelectMany(w => w.EventIds).ToArray(), token, status = "done" });
            }
            if (batch.StaleIds.Length > 0)
                await Run(InboxSql.Complete, new { ids = batch.StaleIds, token, status = "superseded" });
            if (released.Count > 0)
                await Run(InboxSql.Release, new { ids = released.ToArray(), token });
            if (dead.Count > 0)
                await Run(InboxSql.Dead, new { ids = dead.Ids.ToArray(), errs = dead.Errors.ToArray(), codes = dead.Codes.ToArray(), token });
            if (failed.Count > 0)
                await Run(InboxSql.Fail, FailArgs(failed, token));
        }, ct);
    }

    /// <summary>
    /// An entity's pending events become one write: the newest event by (version, id). Every event carries the full
    /// record, so the newest one is the whole desired state and the older ones add nothing (merging them in would
    /// keep a field the newest event no longer has). Two events with the same time are both kept in the inbox; the
    /// one ingested later counts as newer. All of the entity's events are completed with this one.
    /// </summary>
    public static InboxEvent Coalesce(IReadOnlyList<InboxEvent> events)
    {
        var newest = events[0];
        for (var i = 1; i < events.Count; i++)
        {
            var e = events[i];
            if (e.Version > newest.Version || (e.Version == newest.Version && e.Id > newest.Id)) newest = e;
        }
        return newest;
    }

    private object FailArgs(FailedRows rows, Guid token) => new
    {
        ids = rows.Ids.ToArray(),
        errs = rows.Errors.ToArray(),
        codes = rows.Codes.ToArray(),
        token,
        maxRetries = _opt.MaxRetries,
        baseDelaySecs = _opt.RetryBaseDelay.TotalSeconds,
        maxDelaySecs = _opt.MaxRetryDelay.TotalSeconds
    };

    /// <summary>Inbox rows with the error to store on each, as the parallel arrays InboxSql.Fail / Dead take.</summary>
    private sealed class FailedRows
    {
        public List<long> Ids { get; } = [];
        public List<string> Errors { get; } = [];
        public List<string?> Codes { get; } = [];
        public int Count => Ids.Count;

        public void Add(IEnumerable<long> ids, string error, string? code)
        {
            foreach (var id in ids)
            {
                Ids.Add(id);
                Errors.Add(error);
                Codes.Add(code);
            }
        }
    }

    private sealed record Claim(Guid Token, List<InboxEvent> Events, Dictionary<string, long> Synced)
    {
        public long[] EventIds => Events.Select(e => e.Id).ToArray();
    }

    private sealed record Work(string Key, UpsertUnit Unit, long Version, long[] EventIds);

    private sealed record PreparedBatch(long[] StaleIds, List<Work> Work, List<(long[] Ids, string Error)> Invalid);
}
