namespace SalesforceSync.RateLimiting;

public sealed class RateLimitOptions
{
    public const string Section = "RateLimit";

    /// <summary>Max time a request waits for a token before giving up (keep well under HttpClient.Timeout).</summary>
    public TimeSpan MaxWait { get; set; } = TimeSpan.FromSeconds(20);

    /// <summary>Daily usage ratio (org or per-app, whichever is higher) at which to slow down.</summary>
    public double DailySoftRatio { get; set; } = 0.80;

    /// <summary>Speed multiplier applied above <see cref="DailySoftRatio"/>.</summary>
    public double DailySoftSpeed { get; set; } = 0.25;

    /// <summary>Daily usage ratio at which to stop completely.</summary>
    public double DailyHardRatio { get; set; } = 0.95;

    public TimeSpan DailyHardPause { get; set; } = TimeSpan.FromMinutes(15);

    /// <summary>Burst usage ratio at which to back off briefly.</summary>
    public double BurstSoftRatio { get; set; } = 0.80;

    public double BurstSoftSpeed { get; set; } = 0.5;

    public TimeSpan BurstPause { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Minimum gap between writes of header values to Postgres per process.
    /// A change in throttle level is always written immediately.
    /// </summary>
    public TimeSpan ReportInterval { get; set; } = TimeSpan.FromSeconds(5);
}
