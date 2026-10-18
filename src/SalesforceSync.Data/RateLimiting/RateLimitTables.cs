namespace SalesforceSync.Data.RateLimiting;

/// <summary>
/// A row of sf_rate_limit: the token bucket and daily budget all pods share, and what Salesforce last reported in
/// Sforce-Limit-Info. Read and written by <c>SalesforceRateLimiter</c>, always under the row lock
/// (<see cref="RateLimitSql.Lock"/>).
/// </summary>
public sealed class RateLimitState
{
    public string Name { get; set; } = "";

    public double Tokens { get; set; }

    /// <summary>Max burst.</summary>
    public double Capacity { get; set; }

    /// <summary>The normal rate.</summary>
    public double BaseRefillPerSec { get; set; }

    /// <summary>The current rate (reduced near the daily limit).</summary>
    public double RefillPerSec { get; set; }

    public DateTimeOffset UpdatedAt { get; set; }

    public DateTimeOffset? PausedUntil { get; set; }

    /// <summary>Sforce-Limit-Info: api-usage (org daily, rolling 24h).</summary>
    public long? ApiUsed { get; set; }

    public long? ApiMax { get; set; }

    /// <summary>Sforce-Limit-Info: api-bursts.</summary>
    public long? BurstUsed { get; set; }

    public long? BurstMax { get; set; }

    /// <summary>Sforce-Limit-Info: per-app-api-usage (connected app allocation).</summary>
    public long? AppUsed { get; set; }

    public long? AppMax { get; set; }

    public string? AppName { get; set; }

    public DateTimeOffset? UsageUpdatedAt { get; set; }

    /// <summary>
    /// This integration's calls per rolling RateLimit:BudgetWindow: RateLimit:DailyBudgetShare x the lower of the
    /// configured allocation and api_max.
    /// </summary>
    public long? DailyBudget { get; set; }

    /// <summary>
    /// The leases of the concurrency slots (<see cref="SalesforceConcurrencySlots"/>): a JSON array of
    /// <c>{ "token": ..., "until": ... }</c>, one element per batch calling Salesforce right now. Changed only by the
    /// slot statements (<see cref="SlotSql"/>), never by the rate limiter, which doesn't touch this column.
    /// </summary>
    public string Slots { get; set; } = "[]";
}

/// <summary>
/// A row of sf_api_usage: this integration's own Salesforce calls in one bucket of RateLimit:UsageBucket (10 minutes),
/// counted over the rolling RateLimit:BudgetWindow that <see cref="RateLimitState.DailyBudget"/> is enforced against
/// (the org-wide total comes from Sforce-Limit-Info).
/// </summary>
public sealed class ApiUsageBucket
{
    public DateTimeOffset Bucket { get; set; }

    public long Calls { get; set; }

    /// <summary>The start of the usage bucket <paramref name="now"/> falls in.</summary>
    public static DateTimeOffset BucketOf(DateTimeOffset now, TimeSpan bucketSize)
    {
        return new DateTimeOffset(now.UtcTicks - (now.UtcTicks % bucketSize.Ticks), TimeSpan.Zero);
    }
}