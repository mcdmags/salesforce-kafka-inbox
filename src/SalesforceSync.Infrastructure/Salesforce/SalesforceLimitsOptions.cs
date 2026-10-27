namespace SalesforceSync.Infrastructure.Salesforce;

/// <summary>
/// The org's Platform API limits, from "API Request Limits and Allocations" in the Salesforce Developer Limits and
/// Allocations Quick Reference. The daily allocation is not configured: Salesforce reports it in <c>Sforce-Limit-Info</c>.
/// </summary>
public sealed class SalesforceLimitsOptions
{
    public const string Section = "SalesforceLimits";

    /// <summary>Salesforce stops REST calls after this long.</summary>
    public static readonly TimeSpan MaxApiTimeout = TimeSpan.FromMinutes(10);

    /// <summary>
    /// The org's max concurrent inbound requests lasting 20 seconds or longer: 25 for production orgs and sandboxes (the
    /// default), 5 for Developer Edition and trial orgs. Must be positive; <c>Sync:Workers</c> must not exceed it.
    /// </summary>
    public int ConcurrentLongRunningLimit { get; set; } = 25;

    /// <summary>
    /// Client timeout per Salesforce call. Salesforce stops REST calls after 10 minutes, so this must not exceed that.
    /// </summary>
    public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(90);
}
