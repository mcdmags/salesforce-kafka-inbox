using System.Text.Json;
using System.Text.Json.Nodes;
using Dapper;
using Microsoft.Extensions.Options;
using SalesforceSync.Data;
using SalesforceSync.Inbox;
using SalesforceSync.RateLimiting;
using SalesforceSync.Salesforce;

namespace SalesforceSync.Sync;

/// <summary>A pending inbox row as claimed.</summary>
public sealed record InboxEvent(long Id, string Key, long Version, string Payload, int RetryCount);

/// <summary>
/// The one sync engine for every Salesforce object. <see cref="UpsertBatchAsync{TEvent}(UpsertStrategy{TEvent}, CancellationToken)"/>
/// runs one batch end to end; the strategy only says how an inbox event maps to Salesforce records and what to read.
/// <list type="number">
/// <item>Takes a global concurrency slot and respects a shared pause (rate limits, all pods).</item>
/// <item>Claims up to <see cref="SyncOptions.BatchSize"/> entities of the strategy's type: per entity, the head row
/// is locked with FOR UPDATE SKIP LOCKED and all its pending rows are leased, so no two workers in any pod ever
/// process the same entity at once.</item>
/// <item>Supersedes events at or below the entity's synced version (late or replayed), merges the rest in version
/// order and maps them through the strategy; a payload that can't be mapped is dead-lettered on its own.</item>
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
    private static readonly JsonSerializerOptions EventJson = new(JsonSerializerDefaults.Web);

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

        // 1. Claim (short transaction)
        var (token, events) = await ClaimAsync(strategy.EntityType, ct);
        if (events.Count == 0) return 0;

        // 2. Supersede stale events, merge the rest per entity, map to Salesforce records
        var keys = events.Select(e => e.Key).Distinct().ToArray();
        var synced = await pg.ExecuteAsync(async (conn, t) =>
            (await conn.QueryAsync<(string Key, long Version)>(new CommandDefinition(InboxSql.SyncedVersions,
                new { entityType = strategy.EntityType, keys }, cancellationToken: t)))
            .ToDictionary(r => r.Key, r => r.Version), ct);

        var stale = events.Where(e => e.Version <= synced.GetValueOrDefault(e.Key, long.MinValue)).ToList();
        var work = new List<Work>();
        var invalid = new List<(long[] Ids, string Error)>();
        foreach (var group in events.Except(stale).GroupBy(e => e.Key))
        {
            var ordered = group.OrderBy(e => e.Version).ToList();
            var ids = ordered.Select(e => e.Id).ToArray();
            try
            {
                var payload = Coalesce(ordered).Deserialize<TEvent>(EventJson)
                              ?? throw new JsonException("Payload is null");
                work.Add(new Work(group.Key, strategy.Map(group.Key, payload), ordered[^1].Version, ids));
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                invalid.Add((ids, $"{strategy.EntityType} {group.Key}: {ex.Message}"));
            }
        }

        // 3. Salesforce (outside any transaction). Give up before the lease runs out, so another worker never
        //    claims the entity while this one can still write to it.
        Dictionary<string, UpsertResult> results;
        using var leaseCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        leaseCts.CancelAfter(_opt.Lease * 0.8);
        try
        {
            var units = work.Select(w => w.Unit).ToList();
            results = work.Count == 0
                ? []
                : (await salesforce.UpsertAsync(work.Select(w => (w.Key, w.Unit)).ToList(), strategy.Queries(units).ToList(), leaseCts.Token))
                    .ToDictionary(r => r.Key);
        }
        catch (RateLimitedException ex)
        {
            log.LogInformation("Rate limited; deferring {Count} {Type} events for {Delay}", events.Count, strategy.EntityType, ex.RetryAfter);
            await pg.ExecuteAsync((conn, t) => conn.ExecuteAsync(new CommandDefinition(InboxSql.Defer,
                new { token, delay = ex.RetryAfter }, cancellationToken: t)), ct);
            return 0;
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            // The request as a whole failed (network, 5xx, timeout): every entity may succeed on retry
            log.LogError(ex, "{Type} batch failed", strategy.EntityType);
            var code = ex is SalesforceQueryException q ? q.Errors.FirstOrDefault()?.Code : null;
            await pg.ExecuteAsync((conn, t) => conn.ExecuteAsync(new CommandDefinition(InboxSql.Fail,
                FailArgs(events.Select(e => e.Id).ToArray(), token, ex.Message, code), cancellationToken: t)), ct);
            return events.Count;
        }

        // 4. Record outcomes (one transaction; safe to repeat, every statement is conditional on the lease token)
        await pg.ExecuteInTransactionAsync(async (conn, tx, t) =>
        {
            Task Run(string sql, object args) => conn.ExecuteAsync(new CommandDefinition(sql, args, tx, cancellationToken: t));

            if (stale.Count > 0)
                await Run(InboxSql.Complete, new { ids = stale.Select(e => e.Id).ToArray(), token, status = "superseded" });

            foreach (var (ids, error) in invalid)
                await Run(InboxSql.Dead, new { ids, token, err = error, code = SalesforceErrors.InvalidEvent });

            foreach (var w in work)
            {
                var r = results[w.Key];
                var err = string.Join("; ", r.Errors);
                var code = r.PrimaryError?.Code;
                switch (r.Outcome)
                {
                    case SyncOutcome.Saved or SyncOutcome.Unchanged:
                        // Watermark first: Complete releases the lease that MarkSynced checks
                        await Run(InboxSql.MarkSynced, new { entityType = strategy.EntityType, key = w.Key, version = w.Version, token });
                        await Run(InboxSql.Complete, new { ids = w.EventIds, token, status = "done" });
                        break;
                    case SyncOutcome.NotAttempted:
                        await Run(InboxSql.Release, new { ids = w.EventIds, token });
                        break;
                    case SyncOutcome.PermanentFailure:
                        await Run(InboxSql.Dead, new { ids = w.EventIds, token, err, code });
                        break;
                    default:
                        await Run(InboxSql.Fail, FailArgs(w.EventIds, token, err, code));
                        break;
                }
            }
        }, ct);

        foreach (var (ids, error) in invalid)
            log.LogWarning("Dead-lettered {Count} events ({Code}): {Error}", ids.Length, SalesforceErrors.InvalidEvent, error);
        foreach (var r in results.Values.Where(r => r.Outcome is SyncOutcome.PermanentFailure or SyncOutcome.TransientFailure))
            log.LogWarning("{Type} {Key}: {Outcome} ({Code}) {Errors}", strategy.EntityType, r.Key, r.Outcome,
                r.PrimaryError?.Code, string.Join("; ", r.Errors));

        return events.Count;
    }

    /// <summary>
    /// Locks and leases a batch. A fresh token per attempt: if a commit's outcome is unknown and the claim is
    /// retried, rows leased by the lost attempt are not mistaken for this one's; they're reclaimed when their lease expires.
    /// </summary>
    private Task<(Guid Token, List<InboxEvent> Events)> ClaimAsync(string entityType, CancellationToken ct)
        => pg.ExecuteInTransactionAsync(async (conn, tx, t) =>
        {
            var token = Guid.NewGuid();
            var keys = (await conn.QueryAsync<string>(new CommandDefinition(InboxSql.LockHeads,
                new { entityType, batchSize = Math.Min(_opt.BatchSize, SalesforceRestClient.MaxGraphsPerRequest) }, tx,
                cancellationToken: t))).ToArray();
            if (keys.Length == 0) return (token, new List<InboxEvent>());

            var events = (await conn.QueryAsync<InboxEvent>(new CommandDefinition(InboxSql.ClaimKeys,
                new { entityType, keys, token, lease = _opt.Lease }, tx, cancellationToken: t))).ToList();
            return (token, events);
        }, ct);

    /// <summary>
    /// Merges events in version order: later values win, nested objects are merged field by field, so a field an
    /// event doesn't mention keeps the value of an earlier one (absent = unchanged, null = clear).
    /// </summary>
    public static JsonObject Coalesce(IReadOnlyList<InboxEvent> ordered)
    {
        var merged = new JsonObject();
        foreach (var e in ordered)
        {
            var obj = JsonNode.Parse(e.Payload)?.AsObject()
                      ?? throw new JsonException($"Inbox event {e.Id} has an empty payload");
            Merge(merged, obj);
        }
        return merged;
    }

    private static void Merge(JsonObject target, JsonObject source)
    {
        foreach (var (key, value) in source)
        {
            if (value is JsonObject src && target[key] is JsonObject existing) Merge(existing, src);
            else target[key] = value?.DeepClone();
        }
    }

    private object FailArgs(long[] ids, Guid token, string err, string? code) => new
    {
        ids,
        token,
        err,
        code,
        maxRetries = _opt.MaxRetries,
        baseDelaySecs = _opt.RetryBaseDelay.TotalSeconds,
        maxDelaySecs = _opt.MaxRetryDelay.TotalSeconds
    };

    private sealed record Work(string Key, UpsertUnit Unit, long Version, long[] EventIds);
}
