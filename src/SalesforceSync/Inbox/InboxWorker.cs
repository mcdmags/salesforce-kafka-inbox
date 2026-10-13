using System.Text.Json;
using System.Text.Json.Nodes;
using Dapper;
using Microsoft.Extensions.Options;
using Npgsql;
using SalesforceSync.RateLimiting;
using SalesforceSync.Salesforce;

namespace SalesforceSync.Inbox;

public sealed record InboxEvent(long Id, string AccountExtId, long Version, string Payload, int RetryCount);

/// <summary>Coalesced events of one account; <see cref="Version"/> is the newest of them.</summary>
public sealed record WorkUnit(SyncItem Item, long Version, long[] EventIds);

/// <summary>One iteration: claim accounts -> drop stale events -> coalesce -> diff + Composite Graph -> record outcomes.</summary>
public sealed class InboxWorker(
    NpgsqlDataSource db,
    AccountProfileSync sf,
    SalesforceRateLimiter limiter,
    SalesforceConcurrencySlots slots,
    IOptions<SyncOptions> options,
    ILogger<InboxWorker> log)
{
    private readonly SyncOptions _opt = options.Value;

    /// <returns>Number of inbox rows processed (0 = idle).</returns>
    public async Task<int> RunOnceAsync(CancellationToken ct)
    {
        await using var slot = await slots.TryAcquireAsync(ct);
        if (slot is null || await limiter.IsPausedAsync(ct)) return 0;

        var token = Guid.NewGuid();
        await using var conn = await db.OpenConnectionAsync(ct);

        // 1. Claim (short transaction)
        var events = await ClaimAsync(conn, token, ct);
        if (events.Count == 0) return 0;

        // Events at or below the account's synced version arrived late (replay / out-of-order producer):
        // Salesforce already holds newer data, so they are superseded without a write.
        var synced = (await conn.QueryAsync<(string AccountExtId, long Version)>(InboxSql.SyncedVersions,
                new { accts = events.Select(e => e.AccountExtId).Distinct().ToArray() }))
            .ToDictionary(r => r.AccountExtId, r => r.Version);
        var stale = events.Where(e => e.Version <= synced.GetValueOrDefault(e.AccountExtId, long.MinValue)).ToList();
        var work = events.Except(stale)
            .GroupBy(e => e.AccountExtId)
            .Select(g => Coalesce(g.OrderBy(e => e.Version).ToList()))
            .ToList();

        // 2. Salesforce (outside any transaction). Give up before the lease runs out, so another worker never
        //    claims the account while this one can still write to it.
        Dictionary<string, SyncResult> results;
        using var leaseCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        leaseCts.CancelAfter(_opt.Lease * 0.8);
        try
        {
            results = work.Count == 0
                ? []
                : (await sf.SyncAsync(work.Select(w => w.Item).ToList(), leaseCts.Token))
                    .ToDictionary(r => r.AccountExternalId);
        }
        catch (RateLimitedException ex)
        {
            log.LogInformation("Rate limited; deferring {Count} events for {Delay}", events.Count, ex.RetryAfter);
            await conn.ExecuteAsync(InboxSql.Defer, new { token, delay = ex.RetryAfter });
            return 0;
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            log.LogError(ex, "Batch failed");
            await conn.ExecuteAsync(InboxSql.Fail, new
            {
                ids = events.Select(e => e.Id).ToArray(),
                token,
                err = ex.Message,
                maxRetries = _opt.MaxRetries
            });
            return events.Count;
        }

        // 3. Record outcomes
        await using var tx = await conn.BeginTransactionAsync(ct);
        if (stale.Count > 0)
            await conn.ExecuteAsync(InboxSql.Complete,
                new { ids = stale.Select(e => e.Id).ToArray(), token, status = "superseded" }, tx);

        foreach (var w in work)
        {
            var acct = w.Item.AccountExternalId;
            var r = results[acct];

            if (r.Outcome != SyncOutcome.Failed)
            {
                // Watermark first: Complete releases the lease that MarkSynced checks
                await conn.ExecuteAsync(InboxSql.MarkSynced, new { acct, version = w.Version, token }, tx);
                await conn.ExecuteAsync(InboxSql.Complete, new { ids = w.EventIds, token, status = "done" }, tx);
            }
            else
            {
                log.LogWarning("Sync failed for {Acct}: {Errors}", acct, string.Join("; ", r.Errors));
                await conn.ExecuteAsync(InboxSql.Fail, new
                {
                    ids = w.EventIds,
                    token,
                    err = string.Join("; ", r.Errors),
                    maxRetries = _opt.MaxRetries
                }, tx);
            }
        }
        await tx.CommitAsync(ct);

        return events.Count;
    }

    private async Task<List<InboxEvent>> ClaimAsync(NpgsqlConnection conn, Guid token, CancellationToken ct)
    {
        await using var tx = await conn.BeginTransactionAsync(ct);

        var accts = (await conn.QueryAsync<string>(InboxSql.LockHeads,
            new { batchSize = Math.Min(_opt.BatchSize, 75) }, tx)).ToArray();
        if (accts.Length == 0)
        {
            await tx.CommitAsync(ct);
            return [];
        }

        var events = (await conn.QueryAsync<InboxEvent>(InboxSql.ClaimAccounts,
            new { accts, token, lease = _opt.Lease }, tx)).ToList();

        await tx.CommitAsync(ct);
        return events;
    }

    /// <summary>
    /// Merges events in version order (later fields win). Expected payload shape:
    /// { "profileExternalId": "...", "account": { ...SF fields... }, "profile": { ...SF fields... } }
    /// </summary>
    internal static WorkUnit Coalesce(List<InboxEvent> ordered)
    {
        var merged = new JsonObject();
        foreach (var e in ordered)
        {
            var obj = JsonNode.Parse(e.Payload)?.AsObject()
                      ?? throw new InvalidOperationException($"Inbox event {e.Id} has an empty payload");
            foreach (var (key, value) in obj)
            {
                if (value is JsonObject src && merged[key] is JsonObject existing) MergeInto(existing, src);
                else merged[key] = value?.DeepClone();
            }
        }

        var last = ordered[^1];
        var item = MapToSyncItem(last.AccountExtId, merged);
        return new WorkUnit(item, last.Version, ordered.Select(e => e.Id).ToArray());
    }

    private static void MergeInto(JsonObject target, JsonObject src)
    {
        foreach (var (k, v) in src) target[k] = v?.DeepClone();
    }

    /// <summary>Map raw event JSON to Salesforce API field names / CLR types. Adjust to your event schema.</summary>
    private static SyncItem MapToSyncItem(string acctExtId, JsonObject merged)
    {
        var profileExtId = merged["profileExternalId"]?.GetValue<string>() ?? acctExtId;
        return new SyncItem(
            acctExtId,
            ToFields(merged["account"] as JsonObject),
            profileExtId,
            ToFields(merged["profile"] as JsonObject));
    }

    private static Dictionary<string, object?> ToFields(JsonObject? obj)
    {
        var fields = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        if (obj is null) return fields;
        foreach (var (k, v) in obj) fields[k] = ToClr(v);
        return fields;
    }

    private static object? ToClr(JsonNode? node) => node?.GetValueKind() switch
    {
        null or JsonValueKind.Null => null,
        JsonValueKind.String => node.GetValue<string>(),
        JsonValueKind.Number => node.GetValue<decimal>(),
        JsonValueKind.True => true,
        JsonValueKind.False => false,
        _ => node.ToJsonString()
    };
}
