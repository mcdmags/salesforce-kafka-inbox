namespace SalesforceSync.RateLimiting;

public sealed class RateLimitOptions
{
    public const string Section = "RateLimit";

    /// <summary>Max time a request waits for a token before giving up (keep well under HttpClient.Timeout).</summary>
    public TimeSpan MaxWait { get; set; } = TimeSpan.FromSeconds(20);

    /// <summary>Sustained Salesforce calls per second across all workers in all processes (token bucket refill).</summary>
    public double MaxRequestsPerSecond { get; set; } = 2;

    /// <summary>Calls that may be made back to back after an idle period (token bucket capacity).</summary>
    public double BurstCapacity { get; set; } = 5;

    /// <summary>
    /// Fraction of the org's daily API allocation this integration may use per rolling 24 hours. The allocation is
    /// <see cref="SalesforceLimitsOptions.DailyApiAllocation"/>, or the limit Salesforce reports in
    /// <c>Sforce-Limit-Info: api-usage=used/limit</c> when that is lower. The allocation is shared by every integration and user of the
    /// org, so leave room for them. Enforced as a hard cap across all processes.
    /// </summary>
    public double DailyBudgetShare { get; set; } = 0.5;

    /// <summary>
    /// The rolling period <see cref="DailyBudgetShare"/> is counted over: this integration makes at most that many
    /// calls in any window of this length. 24 hours, as Salesforce counts the allocation. A shorter window doesn't
    /// scale the budget down, so it allows more calls per 24 hours.
    /// </summary>
    public TimeSpan BudgetWindow { get; set; } = TimeSpan.FromHours(24);

    /// <summary>
    /// The granularity of the usage count (rows of sf_api_usage). Every bucket that overlaps the window counts in full,
    /// so usage is over- rather than under-counted by up to one bucket.
    /// </summary>
    public TimeSpan UsageBucket { get; set; } = TimeSpan.FromMinutes(10);

    /// <summary>
    /// How far back calls are counted: the window plus the whole bucket that straddles its start. Older buckets are
    /// deleted.
    /// </summary>
    internal TimeSpan CountedWindow => BudgetWindow + UsageBucket;

    /// <summary>
    /// Pause after REQUEST_LIMIT_EXCEEDED for too many concurrent long-running requests
    /// (clears as soon as one of them finishes).
    /// </summary>
    public TimeSpan ConcurrentLimitPause { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Daily usage ratio at which to slow down: the highest of the org's usage, the connected app's usage
    /// (both from Sforce-Limit-Info) and this integration's own usage of its <see cref="DailyBudgetShare"/>.
    /// </summary>
    public double DailySoftRatio { get; set; } = 0.80;

    /// <summary>Speed multiplier applied above <see cref="DailySoftRatio"/>.</summary>
    public double DailySoftSpeed { get; set; } = 0.25;

    /// <summary>Daily usage ratio at which to stop completely.</summary>
    public double DailyHardRatio { get; set; } = 0.95;

    /// <summary>
    /// Pause at <see cref="DailyHardRatio"/>, and after REQUEST_LIMIT_EXCEEDED for the org's total requests
    /// (usage is a rolling 24 hours, so it frees up gradually, not at midnight).
    /// </summary>
    public TimeSpan DailyHardPause { get; set; } = TimeSpan.FromMinutes(15);

    /// <summary>
    /// Minimum gap between writes of header values to Postgres per process.
    /// A change in throttle level is always written immediately.
    /// </summary>
    public TimeSpan ReportInterval { get; set; } = TimeSpan.FromSeconds(5);
}