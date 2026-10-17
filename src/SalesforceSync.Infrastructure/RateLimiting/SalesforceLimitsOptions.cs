using Microsoft.Extensions.Options;

namespace SalesforceSync.Infrastructure.RateLimiting;

/// <summary>
/// The org's Platform API limits, from "API Request Limits and Allocations" in the Salesforce Developer Limits and
/// Allocations Quick Reference. <see cref="DailyApiAllocation"/> is the maximum: when Salesforce reports a lower limit
/// in <c>Sforce-Limit-Info</c>, the budget follows that.
/// </summary>
public sealed class SalesforceLimitsOptions
{
    public const string Section = "SalesforceLimits";

    /// <summary>Salesforce stops REST calls (a composite request as a whole) after this long.</summary>
    public static readonly TimeSpan MaxApiTimeout = TimeSpan.FromMinutes(10);

    /// <summary>
    /// The org's total calls per rolling 24 hours (all integrations and users together), as shown in Setup (System
    /// Overview, API Requests, Last 24 Hours). Required and positive.
    /// </summary>
    public long DailyApiAllocation { get; set; }

    /// <summary>
    /// The org's max concurrent inbound requests lasting <see cref="LongRunningThreshold"/> or longer: 25 for
    /// production orgs and sandboxes (the default), 5 for Developer Edition and trial orgs. Must be positive.
    /// </summary>
    public int ConcurrentLongRunningLimit { get; set; } = 25;

    /// <summary>Requests at least this long count toward <see cref="ConcurrentLongRunningLimit"/>.</summary>
    public TimeSpan LongRunningThreshold { get; set; } = TimeSpan.FromSeconds(20);

    /// <summary>
    /// Client timeout per Salesforce call, including the wait for a rate-limit token. Salesforce stops REST calls
    /// (a composite request as a whole) after 10 minutes, so this must not exceed that.
    /// </summary>
    public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(90);
}
