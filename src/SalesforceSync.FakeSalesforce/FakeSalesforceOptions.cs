namespace SalesforceSync.FakeSalesforce;

public sealed class FakeSalesforceOptions
{
    public const string Section = "FakeSalesforce";

    /// <summary>Simulated network + server time per HTTP call.</summary>
    public TimeSpan Latency { get; set; } = TimeSpan.Zero;

    /// <summary>Reported as the max in <c>Sforce-Limit-Info: api-usage=used/max</c>.</summary>
    public long DailyApiLimit { get; set; } = 1_000_000;

    /// <summary>When true, calls beyond <see cref="DailyApiLimit"/> fail with 403 REQUEST_LIMIT_EXCEEDED.</summary>
    public bool EnforceDailyLimit { get; set; } = true;

    /// <summary>
    /// Requests whose simulated <see cref="Latency"/> is at least this long count as long-running
    /// (Salesforce: 20 seconds).
    /// </summary>
    public TimeSpan LongRunningThreshold { get; set; } = TimeSpan.FromSeconds(20);

    /// <summary>
    /// Max concurrent long-running requests; more fail with 403 REQUEST_LIMIT_EXCEEDED
    /// (Salesforce: 25 for production and sandboxes, 5 for Developer Edition and trials).
    /// </summary>
    public int ConcurrentLongRunningLimit { get; set; } = 25;
}