using Microsoft.Extensions.Options;
using SalesforceSync.Inbox;

namespace SalesforceSync.RateLimiting;

public enum SalesforceOrgType { Production, Sandbox, DeveloperEdition, Trial }

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

    /// <summary>Decides the default <see cref="ConcurrentLongRunningLimit"/>.</summary>
    public SalesforceOrgType OrgType { get; set; } = SalesforceOrgType.Production;

    /// <summary>
    /// The org's total calls per rolling 24 hours (all integrations and users together), as shown in Setup (System
    /// Overview, API Requests, Last 24 Hours). Required and positive.
    /// </summary>
    public long DailyApiAllocation { get; set; }

    /// <summary>
    /// Max concurrent inbound requests lasting <see cref="LongRunningThreshold"/> or longer, across the whole org.
    /// Defaults to 25 for production orgs and sandboxes, 5 for Developer Edition and trial orgs.
    /// </summary>
    public int? ConcurrentLongRunningLimit { get; set; }

    /// <summary>Requests at least this long count toward <see cref="ConcurrentLongRunningLimit"/>.</summary>
    public TimeSpan LongRunningThreshold { get; set; } = TimeSpan.FromSeconds(20);

    /// <summary>
    /// Client timeout per Salesforce call, including the wait for a rate-limit token. Salesforce stops REST calls
    /// (a composite request as a whole) after 10 minutes, so this must not exceed that.
    /// </summary>
    public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(90);

    public int EffectiveConcurrentLongRunningLimit => ConcurrentLongRunningLimit ?? OrgType switch
    {
        SalesforceOrgType.DeveloperEdition or SalesforceOrgType.Trial => 5,
        _ => 25
    };
}

/// <summary>Fails startup when the configuration can't work against the org's published limits.</summary>
public sealed class SalesforceLimitsValidator(IOptions<SyncOptions> sync, IOptions<RateLimitOptions> rate)
    : IValidateOptions<SalesforceLimitsOptions>
{
    public ValidateOptionsResult Validate(string? name, SalesforceLimitsOptions o)
    {
        var errors = new List<string>();

        if (o.DailyApiAllocation <= 0)
        {
            errors.Add($"{SalesforceLimitsOptions.Section}:DailyApiAllocation ({o.DailyApiAllocation}) must be set to the org's daily API " +
                       "allocation, a positive number (Setup: System Overview, API Requests, Last 24 Hours).");
        }

        var concurrent = o.EffectiveConcurrentLongRunningLimit;
        if (sync.Value.MaxConcurrentRequests > concurrent)
        {
            errors.Add($"Sync:MaxConcurrentRequests ({sync.Value.MaxConcurrentRequests}) exceeds the org's limit of " +
                       $"{concurrent} concurrent long-running requests ({o.OrgType}). Other integrations share that " +
                       "limit, so keep it well below.");
        }

        if (o.RequestTimeout <= TimeSpan.Zero || o.RequestTimeout > SalesforceLimitsOptions.MaxApiTimeout)
        {
            errors.Add($"{SalesforceLimitsOptions.Section}:RequestTimeout ({o.RequestTimeout}) must be > 0 and at most " +
                       $"the Salesforce API timeout of {SalesforceLimitsOptions.MaxApiTimeout}.");
        }

        if (sync.Value.Lease <= o.RequestTimeout)
        {
            errors.Add($"{SyncOptions.Section}:Lease ({sync.Value.Lease}) must be longer than {SalesforceLimitsOptions.Section}:" +
                       $"RequestTimeout ({o.RequestTimeout}), so a call that is merely slow can't outlive the lease on the entities it writes.");
        }

        if (o.RequestTimeout <= rate.Value.MaxWait)
        {
            errors.Add($"{SalesforceLimitsOptions.Section}:RequestTimeout ({o.RequestTimeout}) must be longer than " +
                       $"{RateLimitOptions.Section}:MaxWait ({rate.Value.MaxWait}), which it includes.");
        }

        if (rate.Value.MaxRequestsPerSecond <= 0)
        {
            errors.Add($"{RateLimitOptions.Section}:MaxRequestsPerSecond must be positive.");
        }

        if (rate.Value.BurstCapacity < 1)
        {
            errors.Add($"{RateLimitOptions.Section}:BurstCapacity must be at least 1.");
        }

        var share = rate.Value.DailyBudgetShare;
        if (share is <= 0 or > 1)
        {
            errors.Add($"{RateLimitOptions.Section}:DailyBudgetShare ({share}) must be in (0, 1].");
        }

        return errors.Count == 0 ? ValidateOptionsResult.Success : ValidateOptionsResult.Fail(errors);
    }
}