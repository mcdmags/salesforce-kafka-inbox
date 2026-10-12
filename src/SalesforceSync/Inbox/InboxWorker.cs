using System.Text.Json;
using System.Text.Json.Nodes;
using Dapper;
using Microsoft.Extensions.Options;
using Npgsql;
using SalesforceSync.RateLimiting;
using SalesforceSync.Salesforce;

namespace SalesforceSync.Inbox;

public sealed record InboxEvent(long Id, string AccountExtId, long Version, string Payload, int RetryCount);

public sealed record WorkUnit(SyncItem Item, long[] EventIds);

/// <summary>One iteration: claim accounts -> coalesce events -> Composite Graph -> record outcomes.</summary>
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

        var work = events
            .GroupBy(e => e.AccountExtId)
            .Select(g => Coalesce(g.OrderBy(e => e.Version).ToList()))
            .ToList();

        // 2. Salesforce (outside any transaction)
        Dictionary<string, SyncResult> results;
        try
        {
            results = (await sf.SyncAsync(work.Select(w => w.Item).ToList(), ct))
                .ToDictionary(r => r.AccountExternalId);
        }
        catch (RateLimitedException ex)
        {
            log.LogInformation("Rate limited; deferring {Count} events for {Delay}", events.Count, ex.RetryAfter);
            await conn.ExecuteAsync(InboxSql.Defer, new { token, delay = ex.RetryAfter });
            return 0;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
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
        foreach (var w in work)
        {
            var acct = w.Item.AccountExternalId;
            var r = results[acct];
            var stale = r.Outcome == SyncOutcome.Superseded
                        || r.Errors.Any(e => e.Contains("STALE_SYNC_VERSION", StringComparison.Ordinal));

            if (r.Outcome != SyncOutcome.Failed || stale)
            {
                await conn.ExecuteAsync(InboxSql.Complete,
                    new { ids = w.EventIds, token, status = stale ? "superseded" : "done" }, tx);
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
        var item = MapToSyncItem(last.AccountExtId, last.Version, merged);
        return new WorkUnit(item, ordered.Select(e => e.Id).ToArray());
    }

    private static void MergeInto(JsonObject target, JsonObject src)
    {
        foreach (var (k, v) in src) target[k] = v?.DeepClone();
    }

    /// <summary>Map raw event JSON to Salesforce API field names / CLR types. Adjust to your event schema.</summary>
    private static SyncItem MapToSyncItem(string acctExtId, long version, JsonObject merged)
    {
        var profileExtId = merged["profileExternalId"]?.GetValue<string>() ?? acctExtId;
        return new SyncItem(
            acctExtId,
            ToFields(merged["account"] as JsonObject),
            profileExtId,
            ToFields(merged["profile"] as JsonObject),
            version);
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
