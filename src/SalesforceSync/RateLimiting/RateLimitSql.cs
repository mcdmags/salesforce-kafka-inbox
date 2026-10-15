namespace SalesforceSync.RateLimiting;

/// <summary>
/// The statements of <see cref="SalesforceRateLimiter"/> that LINQ can't express: locking the shared
/// <c>sf_rate_limit</c> row and reading the database clock, which every pod shares (theirs may differ).
/// </summary>
internal static class RateLimitSql
{
    /// <summary>The row, tracked and locked until the transaction ends: every change to it is serialized.</summary>
    public const string Lock = "select * from sf_rate_limit where name = 'salesforce' for update";

    public const string Now = "select clock_timestamp()";

    public const string IsPaused = """
        select coalesce(paused_until > clock_timestamp(), false)
        from sf_rate_limit where name = 'salesforce'
        """;
}