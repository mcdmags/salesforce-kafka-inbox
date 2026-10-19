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

        if (o.ConcurrentLongRunningLimit <= 0)
        {
            errors.Add($"{SalesforceLimitsOptions.Section}:ConcurrentLongRunningLimit ({o.ConcurrentLongRunningLimit}) must be the " +
                       "org's concurrent long-running request limit, a positive number (25 for production orgs and sandboxes).");
        }
        else if (sync.Value.Workers > o.ConcurrentLongRunningLimit)
        {
            errors.Add($"Sync:Workers ({sync.Value.Workers}) exceeds the org's limit of {o.ConcurrentLongRunningLimit} " +
                       "concurrent long-running requests. Every pod has that many, and other integrations share the limit, " +
                       "so keep workers x pods well below it.");
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

        return errors.Count == 0 ? ValidateOptionsResult.Success : ValidateOptionsResult.Fail(errors);
    }
}