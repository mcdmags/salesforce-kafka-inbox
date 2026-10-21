namespace SalesforceSync.Infrastructure.RateLimiting;

public sealed class RateLimitOptions
{
    public const string Section = "RateLimit";

    /// <summary>Max time a request waits for a pause to end before giving up (keep well under HttpClient.Timeout).</summary>
    public TimeSpan MaxWait { get; set; } = TimeSpan.FromSeconds(20);

    /// <summary>
    /// Pause of the process that meets REQUEST_LIMIT_EXCEEDED for too many concurrent long-running requests
    /// (clears as soon as one of them finishes).
    /// </summary>
    public TimeSpan ConcurrentLimitPause { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Share of the org's daily allocation at which this process stops calling (1.0: at the allocation itself). Lower
    /// it when other integrations share the org, to leave them a reserve. README.md, "Choosing DailyHardRatio".
    /// </summary>
    public double DailyHardRatio { get; set; } = 1.0;

    /// <summary>
    /// Pause at <see cref="DailyHardRatio"/>, and after REQUEST_LIMIT_EXCEEDED for the org's total requests
    /// (usage is a rolling 24 hours, so it frees up gradually, not at midnight).
    /// </summary>
    public TimeSpan DailyHardPause { get; set; } = TimeSpan.FromMinutes(15);
}
