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

    /// <summary>Salesforce stops processing remaining graphs after this many failures in one request.</summary>
    public int MaxGraphFailuresPerRequest { get; set; } = 14;
}
