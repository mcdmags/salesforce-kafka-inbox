using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;
using SalesforceSync.Data;
using SalesforceSync.Data.Inbox;

namespace SalesforceSync.Benchmarks.ClaimComparison;

/// <summary>
/// The "simple claim" repository (PartySyncRepository) behind our <see cref="IRepository{TRow}"/>, so the same
/// workers drive both. Its four statements are copied as written: Upsert, ClaimNextJobs, MarkJobComplete,
/// MarkJobFailed. Changes, each needed to run it here:
/// <list type="bullet">
/// <item>The lease is a <see cref="TimeSpan"/>, not whole minutes, so a scenario can wait it out in seconds.</item>
/// <item>The clock is a parameter (<see cref="Clock"/>), the app's <c>DateTimeOffset.UtcNow</c> by default, so a
/// scenario can run one worker with a skewed clock.</item>
/// <item>MarkJobComplete and MarkJobFailed throw when no row matches. Here that is counted
/// (<see cref="NoMatchingJob"/>) and the loop goes on to the next entity, as a caller that logs and continues would.</item>
/// <item>It has no release and no permanent failure: <see cref="ReleaseAsync"/> and a permanent failure are a
/// MarkJobFailed, the closest it has.</item>
/// </list>
/// </summary>
public sealed class SimpleClaimRepository(IDbContextFactory<SyncDbContext> contexts, string table, TimeSpan lease, int batchSize, int maxAttempts)
    : IRepository<AccountInboxRow>
{
    /// <summary>The app clock the claim reads; another pod's clock may differ.</summary>
    public Func<DateTimeOffset> Clock { get; init; } = () => DateTimeOffset.UtcNow;

    private int _noMatchingJob;

    /// <summary>MarkJobComplete or MarkJobFailed found no row with the event it sent (its InvalidOperationException).</summary>
    public int NoMatchingJob => _noMatchingJob;

    /// <summary>
    /// The table as its statements need it. Not shown in the screenshots: <c>last_synced_event_timestamp</c> needs a
    /// value before the first sync (NULL &lt; x is never true, so a new row would never be claimed); '-infinity' here.
    /// </summary>
    public static string CreateTableSql(string table)
    {
        return $"""
            DROP TABLE IF EXISTS {table};
            CREATE TABLE {table} (
              entity_id text PRIMARY KEY,
              data jsonb NOT NULL,
              event_timestamp timestamptz NOT NULL,
              last_synced_event_timestamp timestamptz NOT NULL DEFAULT '-infinity',
              processing_timeout timestamptz,
              process_count int NOT NULL DEFAULT 0);
            """;
    }

    /// <summary>The index a careful owner would add: only rows still to sync, in claim order.</summary>
    public static string PartialIndexSql(string table)
    {
        return $"CREATE INDEX {table}_due ON {table} (event_timestamp) WHERE last_synced_event_timestamp < event_timestamp";
    }

    public Task AddAsync(string entityId, DateTimeOffset eventTime, string payload, CancellationToken ct)
    {
        return Upsert(entityId, payload, eventTime, ct);
    }

    public async Task<InboxClaim> ClaimAsync(CancellationToken ct)
    {
        var jobs = await ClaimNextJobs(lease, batchSize, maxAttempts, ct);
        return new InboxClaim([.. jobs.Select(j => new InboxEvent(j.EntityId, j.Data, j.EventTimestamp, j.ProcessingTimeout!.Value))]);
    }

    public async Task MarkDoneAsync(InboxClaim claim, IReadOnlyList<string> entityIds, CancellationToken ct)
    {
        foreach (var e in claim.Of(entityIds))
        {
            await NoMatchCountedAsync(() => MarkJobComplete(e.EntityId, e.EventTime, ct));
        }
    }

    public async Task MarkFailedAsync(InboxClaim claim, IReadOnlyList<FailedEvent> failures, CancellationToken ct)
    {
        foreach (var e in claim.Of(failures.Select(f => f.EntityId)))
        {
            await NoMatchCountedAsync(() => MarkJobFailed(e.EntityId, e.EventTime, ct));
        }
    }

    public Task ReleaseAsync(InboxClaim claim, Release release, CancellationToken ct)
    {
        return MarkFailedAsync(claim, [.. claim.EntityIds.Select(id => new FailedEvent(id, "released"))], ct);
    }

    private async Task NoMatchCountedAsync(Func<Task> mark)
    {
        try
        {
            await mark();
        }
        catch (InvalidOperationException)
        {
            Interlocked.Increment(ref _noMatchingJob);
        }
    }

    // ---------- As written in PartySyncRepository ----------

    private Task Upsert(string entityId, string data, DateTimeOffset eventTimestamp, CancellationToken ct)
    {
        return contexts.RunAsync((db, t) =>
        {
            var sql = $$"""
                INSERT INTO {{table}} (entity_id, data, event_timestamp, process_count)
                VALUES ({0}, {1}::jsonb, {2}, 0)
                ON CONFLICT (entity_id) DO UPDATE
                SET data = EXCLUDED.data,
                  event_timestamp = EXCLUDED.event_timestamp,
                  process_count = 0
                WHERE {{table}}.event_timestamp < EXCLUDED.event_timestamp
                """;
            return db.Database.ExecuteSqlRawAsync(sql, [entityId, data, eventTimestamp], t);
        }, ct);
    }

    private Task MarkJobComplete(string entityId, DateTimeOffset eventTimestamp, CancellationToken ct)
    {
        return contexts.RunAsync(async (db, t) =>
        {
            var sql = $$"""
                UPDATE {{table}}
                SET last_synced_event_timestamp = {0},
                  processing_timeout = NULL,
                  process_count = 0
                WHERE entity_id = {1} AND event_timestamp = {2}
                """;
            var rowsAffected = await db.Database.ExecuteSqlRawAsync(sql, [eventTimestamp, entityId, eventTimestamp], t);
            if (rowsAffected == 0)
            {
                throw new InvalidOperationException($"No matching job found for entityId '{entityId}' at eventTimestamp '{eventTimestamp}'.");
            }
        }, ct);
    }

    private Task MarkJobFailed(string entityId, DateTimeOffset eventTimestamp, CancellationToken ct)
    {
        return contexts.RunAsync(async (db, t) =>
        {
            var sql = $$"""
                UPDATE {{table}}
                SET processing_timeout = NULL
                WHERE entity_id = {0} AND event_timestamp = {1}
                """;
            var rowsAffected = await db.Database.ExecuteSqlRawAsync(sql, [entityId, eventTimestamp], t);
            if (rowsAffected == 0)
            {
                throw new InvalidOperationException($"No matching job found for entityId '{entityId}' at eventTimestamp '{eventTimestamp}'.");
            }
        }, ct);
    }

    private Task<List<SimpleJob>> ClaimNextJobs(TimeSpan timeoutSpan, int batchSize, int maxAttempts, CancellationToken ct)
    {
        return contexts.RunAsync((db, t) =>
        {
            var now = Clock();
            var timeout = now.Add(timeoutSpan);

            var sql = $$"""
                WITH claimed AS (
                  SELECT entity_id
                  FROM {{table}}
                  WHERE last_synced_event_timestamp < event_timestamp
                    AND (processing_timeout IS NULL OR processing_timeout <= {1})
                    AND process_count < {2}
                  ORDER BY event_timestamp
                  LIMIT {3}
                  FOR UPDATE SKIP LOCKED
                )
                UPDATE {{table}} t
                SET processing_timeout = {0},
                  process_count = COALESCE(t.process_count, 0) + 1
                FROM claimed
                WHERE t.entity_id = claimed.entity_id
                RETURNING t.*
                """;
            return db.Database.SqlQueryRaw<SimpleJob>(sql, timeout, now, maxAttempts, batchSize).ToListAsync(t);
        }, ct);
    }
}

/// <summary>A row as ClaimNextJobs returns it (RETURNING t.*).</summary>
public sealed class SimpleJob
{
    [Column("entity_id")]
    public string EntityId { get; set; } = "";

    [Column("data")]
    public string Data { get; set; } = "";

    [Column("event_timestamp")]
    public DateTimeOffset EventTimestamp { get; set; }

    [Column("last_synced_event_timestamp")]
    public DateTimeOffset LastSyncedEventTimestamp { get; set; }

    [Column("processing_timeout")]
    public DateTimeOffset? ProcessingTimeout { get; set; }

    [Column("process_count")]
    public int ProcessCount { get; set; }
}
