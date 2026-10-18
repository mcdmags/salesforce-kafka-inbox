using System.Globalization;
using System.Reflection;
using System.Text.RegularExpressions;
using Microsoft.EntityFrameworkCore;

namespace SalesforceSync.Data.RateLimiting;

/// <summary>
/// The rate limiter's rows in Postgres, shared by every worker in every process: the one <c>sf_rate_limit</c> row,
/// locked with the database clock read in the same statement (<see cref="RateLimitSql.Lock"/>), and the calls counted
/// per usage bucket in <c>sf_api_usage</c>. It decides nothing: the caller's function reads the locked row and changes
/// it, and the change is saved in the same transaction. Every change queues for the row lock in every process, so each
/// unit holds it for as few round trips as possible, and is retried as a whole on transient errors (a retry after an
/// unknown commit can only over-count usage, the safe direction).
/// </summary>
public sealed class RateLimitRepository(IDbContextFactory<SyncDbContext> contexts)
{
    /// <summary>
    /// Locks the row, reads the usage in the window, runs <paramref name="decide"/> and saves what it changed (the
    /// row, and the calls it counted), all in one transaction: the lock, the usage, the write and the commit are the
    /// only round trips.
    /// </summary>
    /// <param name="usageBucket">The size of a usage bucket.</param>
    /// <param name="countedWindow">How far back the calls are counted.</param>
    /// <param name="decide">Changes <see cref="LockedRateLimit.Row"/> and calls <see cref="LockedRateLimit.Count"/>.</param>
    /// <param name="ct">Cancels the transaction.</param>
    public Task<T> DecideAsync<T>(TimeSpan usageBucket, TimeSpan countedWindow, Func<LockedRateLimit, T> decide, CancellationToken ct)
    {
        return contexts.RunInTransactionAsync(async (db, t) =>
        {
            var (row, now) = await LockAsync(db, t);
            // Read after the lock, in a statement of its own: it sees every usage increment committed by whoever held
            // the lock before. With the calls already counted in the current bucket, null if none.
            var since = now - countedWindow;
            var bucket = ApiUsageBucket.BucketOf(now, usageBucket);
            var usage = await db.Set<ApiUsageBucket>().Where(b => b.Bucket >= since)
                .GroupBy(_ => 1)
                .Select(g => new
                {
                    Used = g.Sum(b => b.Calls),
                    Oldest = g.Min(b => b.Bucket),
                    Current = g.Max(b => b.Bucket == bucket ? (long?)b.Calls : null),
                })
                .SingleOrDefaultAsync(t);

            var locked = new LockedRateLimit(row, now, usage?.Used ?? 0, usage?.Oldest);
            var result = decide(locked);
            if (locked.Counted > 0)
            {
                // The row lock serializes every change in every process, so no one can insert this bucket between the
                // read and the write: no ON CONFLICT needed. One SaveChanges, one batch.
                var counted = new ApiUsageBucket { Bucket = bucket, Calls = (usage?.Current ?? 0) + locked.Counted };
                if (usage?.Current is null)
                {
                    db.Add(counted);
                }
                else
                {
                    db.Update(counted);
                }
            }

            await db.SaveChangesAsync(t);
            return result;
        }, ct);
    }

    /// <summary>
    /// Locks the row and applies <paramref name="change"/> (given the row and the database clock), and commits; first
    /// deletes the usage buckets that have left <paramref name="countedWindow"/>.
    /// </summary>
    public Task UpdateAsync(TimeSpan countedWindow, Action<RateLimitState, DateTimeOffset> change, CancellationToken ct)
    {
        return contexts.RunInTransactionAsync(async (db, t) =>
        {
            // Before the lock, so as not to hold it longer: the other changes only read and write buckets in the window
            await db.Set<ApiUsageBucket>()
                .Where(b => b.Bucket + countedWindow < RateLimitSql.ClockTimestamp())
                .ExecuteDeleteAsync(t);
            var (row, now) = await LockAsync(db, t);
            change(row, now);
            await db.SaveChangesAsync(t);
        }, ct);
    }

    /// <summary>
    /// Runs <c>sql/apply-rate-limits.sql</c>, the deploy's script for the limits, with these values (psql's
    /// <c>-v name=value</c>, substituted as numbers): writes them to the shared row if they differ from what it holds.
    /// </summary>
    /// <returns>Whether the row was changed.</returns>
    public async Task<bool> ApplyLimitsAsync(double capacity, double ratePerSec, long allocation, double share, CancellationToken ct)
    {
        var values = new Dictionary<string, string>
        {
            ["capacity"] = capacity.ToString("R", CultureInfo.InvariantCulture),
            ["rate"] = ratePerSec.ToString("R", CultureInfo.InvariantCulture),
            ["allocation"] = allocation.ToString(CultureInfo.InvariantCulture),
            ["share"] = share.ToString("R", CultureInfo.InvariantCulture)
        };
        await using var script = Assembly.GetExecutingAssembly().GetManifestResourceStream("apply-rate-limits.sql")!;
        using var reader = new StreamReader(script);
        // ":capacity::double precision" is a variable followed by a cast; "::" itself is not a variable
        var sql = Regex.Replace(await reader.ReadToEndAsync(ct), @"(?<!:):(capacity|rate|allocation|share)\b", m => values[m.Groups[1].Value]);
        var changed = await contexts.RunAsync((db, t) => db.Database.ExecuteAsync(sql, t), ct);
        return changed > 0;
    }

    /// <summary>Whether a shared pause is in force now.</summary>
    public Task<bool> IsPausedAsync(CancellationToken ct)
    {
        return contexts.RunAsync((db, t) => db.Database.QuerySingleAsync<bool>(RateLimitSql.IsPaused, t), ct);
    }

    /// <summary>The shared row, locked until the transaction ends, and the database clock once it is locked: one statement.</summary>
    private static async Task<(RateLimitState Row, DateTimeOffset Now)> LockAsync(SyncDbContext db, CancellationToken t)
    {
        var locked = await db.Set<RateLimitState>().FromSqlRaw(RateLimitSql.Lock)
            .Select(r => new { Row = r, Now = RateLimitSql.ClockTimestamp() })
            .SingleAsync(t);
        return (locked.Row, locked.Now);
    }
}

/// <summary>The locked rate-limit row and what a decision about it needs, handed to <see cref="RateLimitRepository.DecideAsync{T}"/>'s function.</summary>
public sealed class LockedRateLimit(RateLimitState row, DateTimeOffset now, long used, DateTimeOffset? oldest)
{
    /// <summary>The shared row, tracked: what the function changes is saved with the transaction.</summary>
    public RateLimitState Row { get; } = row;

    /// <summary>The database clock, read with the lock.</summary>
    public DateTimeOffset Now { get; } = now;

    /// <summary>Calls counted in the window.</summary>
    public long Used { get; } = used;

    /// <summary>The oldest usage bucket in the window, null if there is none.</summary>
    public DateTimeOffset? Oldest { get; } = oldest;

    internal long Counted { get; private set; }

    /// <summary>Counts <paramref name="calls"/> in the current usage bucket when the transaction saves.</summary>
    public void Count(int calls)
    {
        Counted += calls;
    }
}
