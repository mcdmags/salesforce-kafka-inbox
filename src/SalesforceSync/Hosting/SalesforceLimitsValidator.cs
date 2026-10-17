using Microsoft.Extensions.Options;
using SalesforceSync.Data.Inbox;
using SalesforceSync.Infrastructure.RateLimiting;

namespace SalesforceSync.Hosting;

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

        if (o.ConcurrentLongRunningLimit <= 0)
        {
            errors.Add($"{SalesforceLimitsOptions.Section}:ConcurrentLongRunningLimit ({o.ConcurrentLongRunningLimit}) must be the " +
                       "org's concurrent long-running request limit, a positive number (25 for production orgs and sandboxes).");
        }
        else if (sync.Value.MaxConcurrentRequests > o.ConcurrentLongRunningLimit)
        {
            errors.Add($"Sync:MaxConcurrentRequests ({sync.Value.MaxConcurrentRequests}) exceeds the org's limit of " +
                       $"{o.ConcurrentLongRunningLimit} concurrent long-running requests. Other integrations share that " +
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

        if (rate.Value.UsageBucket <= TimeSpan.Zero || rate.Value.BudgetWindow <= rate.Value.UsageBucket)
        {
            errors.Add($"{RateLimitOptions.Section}:UsageBucket ({rate.Value.UsageBucket}) must be > 0 and shorter than " +
                       $"{RateLimitOptions.Section}:BudgetWindow ({rate.Value.BudgetWindow}).");
        }

        return errors.Count == 0 ? ValidateOptionsResult.Success : ValidateOptionsResult.Fail(errors);
    }
}