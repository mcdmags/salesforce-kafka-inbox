using SalesforceSync.Data;

namespace SalesforceSync.RateLimiting;

/// <summary>
/// The statements of <see cref="SalesforceRateLimiter"/> that LINQ can't express: locking the shared
/// <c>sf_rate_limit</c> row and reading the database clock, which every pod shares (theirs may differ).
/// </summary>
internal static class RateLimitSql
{
    /// <summary>
    /// The row, tracked and locked until the transaction ends: every change to it is serialized. LINQ over it wraps it
    /// in a subquery, so the clock (<see cref="ClockTimestamp"/>) is read in the same statement, once the row is locked.
    /// </summary>
    public const string Lock = "select * from sf_rate_limit where name = 'salesforce' for update";

    /// <summary>
    /// Postgres' <c>clock_timestamp()</c> in LINQ (mapped in <see cref="SyncDbContext"/>): the time it is read, not the
    /// time the transaction began (<c>now()</c>), which may be before the lock was granted.
    /// </summary>
    public static DateTimeOffset ClockTimestamp()
    {
        throw new NotSupportedException("Translated to SQL only");
    }

    public const string IsPaused = """
        select coalesce(paused_until > clock_timestamp(), false)
        from sf_rate_limit where name = 'salesforce'
        """;
}