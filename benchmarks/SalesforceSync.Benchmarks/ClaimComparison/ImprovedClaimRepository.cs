using System.Collections.Concurrent;
using Microsoft.EntityFrameworkCore;
using SalesforceSync.Data;
using SalesforceSync.Data.Inbox;

namespace SalesforceSync.Benchmarks.ClaimComparison;

/// <summary>
/// The simple claim repository (<see cref="SimpleClaimRepository"/>) improved, with its four methods' signatures
/// unchanged. What changed:
/// <list type="number">
/// <item>Partial index on the rows still to sync (<see cref="PartialIndexSql"/>), so a claim never reads synced rows.</item>
/// <item>Backoff: a failure sets <c>processing_timeout</c> to the next try, doubling per attempt, instead of NULL.</item>
/// <item>Database clock: the claim uses <c>now()</c>, so a pod whose clock is off can't take another pod's rows.</item>
/// <item>A result counts only for the claim that made it: the claim's lease end (<c>processing_timeout</c>) is kept
/// per entity in this instance, and MarkJobComplete/MarkJobFailed need the row to still have it. A worker whose lease
/// ran out can't free or finish a row another worker has taken over.</item>
/// <item>A newer event stored during the send no longer waits for the lease: the result still applies (the row is
/// still this claim's), and since last_synced_event_timestamp is older than the new event, the row is due at once.</item>
/// <item>The error on the row (<c>last_error</c>): a <see cref="MarkJobFailed(string, DateTimeOffset, string?, CancellationToken)"/>
/// overload stores it, so a row that used up its attempts shows why. Cleared by success and by a newer event.</item>
/// </list>
/// The claim and its results must go through the same instance, used by one worker at a time (a scoped repository
/// in a worker's scope does that). Passing the lease end to the Mark methods would remove that rule, but changes their
/// signatures.
/// </summary>
public sealed class ImprovedClaimRepository(IDbContextFactory<SyncDbContext> contexts, string table, TimeSpan retryBaseDelay, TimeSpan maxRetryDelay)
{
    // Lease end each entity's claim set: proof the row is still this claim's when its result comes back
    private readonly ConcurrentDictionary<string, DateTimeOffset> _leases = new(StringComparer.Ordinal);

    /// <summary>Only rows still to sync, in claim order. Synced rows are most of the table and never claimed.</summary>
    public static string PartialIndexSql(string table)
    {
        return SimpleClaimRepository.PartialIndexSql(table);
    }

    public Task Upsert(string entityId, string data, DateTimeOffset eventTimestamp, CancellationToken ct)
    {
        return contexts.RunAsync((db, t) =>
        {
            // Unchanged: a row being sent keeps its lease, so the newer event is sent after this send, never alongside it
            var sql = $$"""
                INSERT INTO {{table}} (entity_id, data, event_timestamp, process_count)
                VALUES ({0}, {1}::jsonb, {2}, 0)
                ON CONFLICT (entity_id) DO UPDATE
                SET data = EXCLUDED.data,
                  event_timestamp = EXCLUDED.event_timestamp,
                  process_count = 0,
                  last_error = NULL
                WHERE {{table}}.event_timestamp < EXCLUDED.event_timestamp
                """;
            return db.Database.ExecuteSqlRawAsync(sql, [entityId, data, eventTimestamp], t);
        }, ct);
    }

    public async Task<IReadOnlyList<SimpleJob>> ClaimNextJobs(int timeoutMinutes, int batchSize, int maxAttempts, CancellationToken ct)
    {
        var jobs = await contexts.RunAsync((db, t) =>
        {
            var sql = $$"""
                WITH claimed AS (
                  SELECT entity_id
                  FROM {{table}}
                  WHERE last_synced_event_timestamp < event_timestamp
                    AND (processing_timeout IS NULL OR processing_timeout <= now())
                    AND process_count < {1}
                  ORDER BY event_timestamp
                  LIMIT {2}
                  FOR UPDATE SKIP LOCKED
                )
                UPDATE {{table}} t
                SET processing_timeout = now() + make_interval(mins => {0}),
                  process_count = t.process_count + 1
                FROM claimed
                WHERE t.entity_id = claimed.entity_id
                RETURNING t.*
                """;
            return db.Database.SqlQueryRaw<SimpleJob>(sql, timeoutMinutes, maxAttempts, batchSize).ToListAsync(t);
        }, ct);
        foreach (var job in jobs)
        {
            _leases[job.EntityId] = job.ProcessingTimeout!.Value;
        }

        return jobs;
    }

    public Task MarkJobComplete(string entityId, DateTimeOffset eventTimestamp, CancellationToken ct)
    {
        // A newer event stored meanwhile stays due: last_synced_event_timestamp is still older than it
        var sql = $$"""
            UPDATE {{table}}
            SET last_synced_event_timestamp = {0},
              processing_timeout = NULL,
              process_count = 0,
              last_error = NULL
            WHERE entity_id = {1} AND processing_timeout = {2}
            """;
        return OwnRowAsync(entityId, eventTimestamp, lease => [eventTimestamp, entityId, lease], sql, ct);
    }

    public Task MarkJobFailed(string entityId, DateTimeOffset eventTimestamp, CancellationToken ct)
    {
        return MarkJobFailed(entityId, eventTimestamp, null, ct);
    }

    /// <summary>MarkJobFailed with why it failed: kept on the row (last_error) until it is synced or a newer event arrives.</summary>
    public Task MarkJobFailed(string entityId, DateTimeOffset eventTimestamp, string? error, CancellationToken ct)
    {
        // The sent event: wait before the next try, longer after each. A newer one stored meanwhile: try it now
        var sql = $$"""
            UPDATE {{table}}
            SET processing_timeout = CASE WHEN event_timestamp = {1}
                THEN now() + make_interval(secs => least({3}, {4} * power(2, greatest(process_count - 1, 0))))
              END,
              last_error = {5}
            WHERE entity_id = {0} AND processing_timeout = {2}
            """;
        return OwnRowAsync(entityId, eventTimestamp,
            lease => [entityId, eventTimestamp, lease, maxRetryDelay.TotalSeconds, retryBaseDelay.TotalSeconds, error ?? (object)DBNull.Value], sql, ct);
    }

    /// <summary>Runs a result statement on the row if this instance's claim still holds it; throws if not, as before.</summary>
    private Task OwnRowAsync(string entityId, DateTimeOffset eventTimestamp, Func<DateTimeOffset, object[]> args, string sql, CancellationToken ct)
    {
        return !_leases.TryRemove(entityId, out var lease)
            ? throw new InvalidOperationException($"No job claimed by this repository for entityId '{entityId}'.")
            : contexts.RunAsync(async (db, t) =>
            {
                var rowsAffected = await db.Database.ExecuteSqlRawAsync(sql, args(lease), t);
                if (rowsAffected == 0)
                {
                    throw new InvalidOperationException($"No matching job found for entityId '{entityId}' at eventTimestamp '{eventTimestamp}'.");
                }
            }, ct);
    }
}

/// <summary>
/// <see cref="ImprovedClaimRepository"/> behind <see cref="IRepository{TRow}"/>, as <see cref="SimpleClaimRepository"/>
/// is: a lease rounded up to whole minutes, a thrown "no matching job" counted.
/// </summary>
public sealed class ImprovedClaimAdapter(ImprovedClaimRepository repository, TimeSpan lease, int batchSize, int maxAttempts) : IRepository<AccountInboxRow>
{
    private int _noMatchingJob;

    public int NoMatchingJob => _noMatchingJob;

    /// <summary>The lease the claim gets: whole minutes, at least one.</summary>
    public static int Minutes(TimeSpan lease)
    {
        return Math.Max(1, (int)Math.Ceiling(lease.TotalMinutes));
    }

    public Task AddAsync(string entityId, DateTimeOffset eventTime, string payload, CancellationToken ct)
    {
        return repository.Upsert(entityId, payload, eventTime, ct);
    }

    public async Task<InboxClaim> ClaimAsync(CancellationToken ct)
    {
        var jobs = await repository.ClaimNextJobs(Minutes(lease), batchSize, maxAttempts, ct);
        return new InboxClaim([.. jobs.Select(j => new InboxEvent(j.EntityId, j.Data, j.EventTimestamp, j.ProcessingTimeout!.Value))]);
    }

    public async Task MarkDoneAsync(InboxClaim claim, IReadOnlyList<string> entityIds, CancellationToken ct)
    {
        foreach (var e in claim.RowsOf(entityIds))
        {
            await NoMatchCountedAsync(() => repository.MarkJobComplete(e.EntityId, e.EventTime, ct));
        }
    }

    public async Task MarkFailedAsync(InboxClaim claim, IReadOnlyList<FailedEvent> failures, CancellationToken ct)
    {
        var errors = failures.ToDictionary(f => f.EntityId, f => f.Error);
        foreach (var e in claim.RowsOf(failures.Select(f => f.EntityId)))
        {
            await NoMatchCountedAsync(() => repository.MarkJobFailed(e.EntityId, e.EventTime, errors[e.EntityId], ct));
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
}
