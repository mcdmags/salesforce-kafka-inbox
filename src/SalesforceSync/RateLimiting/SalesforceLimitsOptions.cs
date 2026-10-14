using Microsoft.Extensions.Options;
using SalesforceSync.Inbox;

namespace SalesforceSync.RateLimiting;

public enum SalesforceOrgType { Production, Sandbox, FullSandbox, DeveloperEdition, Trial }

public enum SalesforceEdition { Enterprise, Professional, Unlimited, Performance }

/// <summary>
/// The org's Platform API limits, from "API Request Limits and Allocations" in the Salesforce Developer Limits and
/// Allocations Quick Reference. Describe the org (type, edition, licenses, add-ons) and the limits are derived from
/// the published tables; any derived value can be overridden with the number shown in Setup.
/// </summary>
public sealed class SalesforceLimitsOptions
{
    public const string Section = "SalesforceLimits";

    /// <summary>Hard limits published by Salesforce.</summary>
    public const int DeveloperEditionDailyAllocation = 15_000;
    public const int FullSandboxDailyAllocation = 5_000_000;
    public const int BaseDailyAllocation = 100_000;
    public static readonly TimeSpan MaxApiTimeout = TimeSpan.FromMinutes(10);

    public SalesforceOrgType OrgType { get; set; } = SalesforceOrgType.Production;

    /// <summary>Selects the calls-per-license table (Enterprise/Professional vs Unlimited/Performance).</summary>
    public SalesforceEdition Edition { get; set; } = SalesforceEdition.Enterprise;

    /// <summary>License type name -> number of licenses, e.g. { "Salesforce": 15, "Partner Community": 200 }.</summary>
    public Dictionary<string, int> Licenses { get; set; } = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>Adds license types or overrides the calls per license of the built-in table.</summary>
    public Dictionary<string, int> CallsPerLicense { get; set; } = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>Purchased API Call Add-Ons (calls per 24 hours).</summary>
    public long PurchasedApiCalls { get; set; }

    /// <summary>
    /// The org's total calls per rolling 24 hours, if you'd rather copy it from Setup (System Overview) than describe
    /// licenses. Required for Sandbox and Trial orgs, whose allocation Salesforce doesn't publish, and for a Full
    /// Sandbox created from a template, whose limits come from the template.
    /// </summary>
    public long? DailyApiAllocation { get; set; }

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

    /// <summary>
    /// Total calls per rolling 24 hours for the whole org (all integrations and users together): Developer Edition
    /// 15,000; Full Sandbox 5,000,000; production orgs 100,000 + licenses x calls per license type + add-ons.
    /// </summary>
    public long EffectiveDailyApiAllocation
    {
        get
        {
            if (DailyApiAllocation is { } fixedAllocation)
            {
                return fixedAllocation;
            }
            return OrgType switch
            {
                SalesforceOrgType.DeveloperEdition => DeveloperEditionDailyAllocation,
                SalesforceOrgType.FullSandbox => FullSandboxDailyAllocation,
                SalesforceOrgType.Production => BaseDailyAllocation
                                                + Licenses.Sum(l => (long)l.Value * CallsPerLicenseFor(l.Key))
                                                + PurchasedApiCalls,
                _ => throw new InvalidOperationException(
                    $"Salesforce doesn't publish the daily API allocation of {OrgType} orgs. Set {Section}:DailyApiAllocation " +
                    "to the number in Setup (System Overview, API Requests, Last 24 Hours).")
            };
        }
    }

    public int CallsPerLicenseFor(string licenseType)
    {
        if (CallsPerLicense.TryGetValue(licenseType, out var custom)) return custom;
        var table = Edition is SalesforceEdition.Unlimited or SalesforceEdition.Performance
            ? UnlimitedPerformanceCalls
            : EnterpriseProfessionalCalls;
        return table.TryGetValue(licenseType, out var calls)
            ? calls
            : throw new InvalidOperationException(
                $"Unknown Salesforce license type '{licenseType}'. Known: {string.Join(", ", table.Keys)}. " +
                $"Add it under {Section}:CallsPerLicense.");
    }

    // API calls per license type per 24 hours (Enterprise Edition / Professional Edition with API access enabled)
    private static readonly Dictionary<string, int> EnterpriseProfessionalCalls = new(StringComparer.OrdinalIgnoreCase)
    {
        ["Salesforce"] = 1_000,
        ["Salesforce Platform"] = 1_000,
        ["Lightning Platform - One App"] = 200,
        ["Customer Community"] = 0,
        ["Customer Community Login"] = 0,
        ["Customer Community Plus"] = 200,
        ["Customer Community Plus Login"] = 10,
        ["External Identity 25,000"] = 70_000,
        ["External Identity 250,000"] = 750_000,
        ["External Identity 1,000,000"] = 4_000_000,
        ["Partner Community"] = 200,
        ["Partner Community Login"] = 10,
        ["Lightning Platform Starter"] = 200,
        ["Lightning Platform Plus"] = 1_000
    };

    // Unlimited Edition / Performance Edition
    private static readonly Dictionary<string, int> UnlimitedPerformanceCalls =
        new(EnterpriseProfessionalCalls, StringComparer.OrdinalIgnoreCase)
        {
            ["Salesforce"] = 5_000,
            ["Salesforce Platform"] = 5_000,
            ["Lightning Platform Plus"] = 5_000
        };
}

/// <summary>Fails startup when the configuration can't work against the org's published limits.</summary>
public sealed class SalesforceLimitsValidator(IOptions<SyncOptions> sync, IOptions<RateLimitOptions> rate)
    : IValidateOptions<SalesforceLimitsOptions>
{
    public ValidateOptionsResult Validate(string? name, SalesforceLimitsOptions o)
    {
        var errors = new List<string>();

        try
        {
            if (o.EffectiveDailyApiAllocation <= 0) errors.Add("The daily API allocation must be positive.");
        }
        catch (InvalidOperationException ex)
        {
            errors.Add(ex.Message);
        }

        var concurrent = o.EffectiveConcurrentLongRunningLimit;
        if (sync.Value.MaxConcurrentRequests > concurrent)
            errors.Add($"Sync:MaxConcurrentRequests ({sync.Value.MaxConcurrentRequests}) exceeds the org's limit of " +
                       $"{concurrent} concurrent long-running requests ({o.OrgType}). Other integrations share that " +
                       "limit, so keep it well below.");

        if (o.RequestTimeout <= TimeSpan.Zero || o.RequestTimeout > SalesforceLimitsOptions.MaxApiTimeout)
            errors.Add($"{SalesforceLimitsOptions.Section}:RequestTimeout ({o.RequestTimeout}) must be > 0 and at most " +
                       $"the Salesforce API timeout of {SalesforceLimitsOptions.MaxApiTimeout}.");
        if (sync.Value.Lease <= o.RequestTimeout)
            errors.Add($"{SyncOptions.Section}:Lease ({sync.Value.Lease}) must be longer than {SalesforceLimitsOptions.Section}:" +
                       $"RequestTimeout ({o.RequestTimeout}), so a call that is merely slow can't outlive the lease on the entities it writes.");
        if (o.RequestTimeout <= rate.Value.MaxWait)
            errors.Add($"{SalesforceLimitsOptions.Section}:RequestTimeout ({o.RequestTimeout}) must be longer than " +
                       $"{RateLimitOptions.Section}:MaxWait ({rate.Value.MaxWait}), which it includes.");

        if (rate.Value.MaxRequestsPerSecond <= 0)
            errors.Add($"{RateLimitOptions.Section}:MaxRequestsPerSecond must be positive.");
        if (rate.Value.BurstCapacity < 1)
            errors.Add($"{RateLimitOptions.Section}:BurstCapacity must be at least 1.");

        var share = rate.Value.DailyBudgetShare;
        if (share is <= 0 or > 1)
            errors.Add($"{RateLimitOptions.Section}:DailyBudgetShare ({share}) must be in (0, 1].");

        return errors.Count == 0 ? ValidateOptionsResult.Success : ValidateOptionsResult.Fail(errors);
    }
}
