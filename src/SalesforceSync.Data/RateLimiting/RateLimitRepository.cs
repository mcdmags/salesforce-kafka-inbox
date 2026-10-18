using System.Globalization;
using System.Reflection;
using System.Text.RegularExpressions;
using Microsoft.EntityFrameworkCore;

namespace SalesforceSync.Data.RateLimiting;

/// <summary>
/// The rate limiter's rows in Postgres, shared by every worker in every process: the one <c>sf_rate_limit</c> row,
/// locked with the database clock read in the same statement (<see cref="RateLimitSql.Lock"/>). It decides nothing:
/// the caller's function reads the locked row and changes it, and the change is saved in the same transaction. Every
/// change queues for the row lock in every process, so each unit holds it for as few round trips as possible, and is
/// retried as a whole on transient errors.
/// </summary>
public sealed class RateLimitRepository(IDbContextFactory<SyncDbContext> contexts)
{
    /// <summary>
    /// Locks the row, runs <paramref name="decide"/> and saves what it changed, all in one transaction: the lock, the
    /// write and the commit are the only round trips.
    /// </summary>
    /// <param name="decide">Reads and changes <see cref="LockedRateLimit.Row"/>.</param>
    /// <param name="ct">Cancels the transaction.</param>
    public Task<T> DecideAsync<T>(Func<LockedRateLimit, T> decide, CancellationToken ct)
    {
        return contexts.RunInTransactionAsync(async (db, t) =>
        {
            var (row, now) = await LockAsync(db, t);
            var result = decide(new LockedRateLimit(row, now));
            await db.SaveChangesAsync(t);
            return result;
        }, ct);
    }

    /// <summary>Locks the row and applies <paramref name="change"/> (given the row and the database clock), and commits.</summary>
    public Task UpdateAsync(Action<RateLimitState, DateTimeOffset> change, CancellationToken ct)
    {
        return contexts.RunInTransactionAsync(async (db, t) =>
        {
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
    public async Task<bool> ApplyLimitsAsync(double capacity, double ratePerSec, CancellationToken ct)
    {
        var values = new Dictionary<string, string>
        {
            ["capacity"] = capacity.ToString("R", CultureInfo.InvariantCulture),
            ["rate"] = ratePerSec.ToString("R", CultureInfo.InvariantCulture)
        };
        await using var script = Assembly.GetExecutingAssembly().GetManifestResourceStream("apply-rate-limits.sql")!;
        using var reader = new StreamReader(script);
        // ":capacity::double precision" is a variable followed by a cast; "::" itself is not a variable
        var sql = Regex.Replace(await reader.ReadToEndAsync(ct), @"(?<!:):(capacity|rate)\b", m => values[m.Groups[1].Value]);
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

/// <summary>The locked rate-limit row and the database clock, handed to <see cref="RateLimitRepository.DecideAsync{T}"/>'s function.</summary>
public sealed class LockedRateLimit(RateLimitState row, DateTimeOffset now)
{
    /// <summary>The shared row, tracked: what the function changes is saved with the transaction.</summary>
    public RateLimitState Row { get; } = row;

    /// <summary>The database clock, read with the lock.</summary>
    public DateTimeOffset Now { get; } = now;
}
