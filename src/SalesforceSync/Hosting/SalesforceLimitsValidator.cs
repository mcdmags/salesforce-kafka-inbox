using FluentValidation;
using Microsoft.Extensions.Options;
using SalesforceSync.Data.Inbox;
using SalesforceSync.Infrastructure.RateLimiting;

namespace SalesforceSync.Hosting;

/// <summary>Fails startup when the configuration can't work against the org's published limits.</summary>
public sealed class SalesforceLimitsValidator : AbstractValidator<SalesforceLimitsOptions>
{
    private const string Section = SalesforceLimitsOptions.Section;

    public SalesforceLimitsValidator(IOptions<SyncOptions> syncOptions, IOptions<RateLimitOptions> rateOptions)
    {
        var sync = syncOptions.Value;
        var rate = rateOptions.Value;

        RuleFor(o => o.ConcurrentLongRunningLimit)
            .GreaterThan(0)
            .WithMessage(o => $"{Section}:ConcurrentLongRunningLimit ({o.ConcurrentLongRunningLimit}) must be the org's concurrent " +
                              "long-running request limit, a positive number (25 for production orgs and sandboxes).")
            .DependentRules(() => RuleFor(o => o.ConcurrentLongRunningLimit)
                .GreaterThanOrEqualTo(sync.Workers)
                .WithMessage(o => $"Sync:Workers ({sync.Workers}) exceeds the org's limit of {o.ConcurrentLongRunningLimit} concurrent " +
                                  "long-running requests. Every pod has that many, and other integrations share the limit, so keep " +
                                  "workers x pods well below it."));

        RuleFor(o => o.RequestTimeout)
            .Must(t => t > TimeSpan.Zero && t <= SalesforceLimitsOptions.MaxApiTimeout)
            .WithMessage(o => $"{Section}:RequestTimeout ({o.RequestTimeout}) must be > 0 and at most the Salesforce API timeout of " +
                              $"{SalesforceLimitsOptions.MaxApiTimeout}.");
        RuleFor(o => o.RequestTimeout)
            .LessThan(sync.Lease)
            .WithMessage(o => $"{SyncOptions.Section}:Lease ({sync.Lease}) must be longer than {Section}:RequestTimeout " +
                              $"({o.RequestTimeout}), so a call that is merely slow can't outlive the lease on the entities it writes.");
        RuleFor(o => o.RequestTimeout)
            .GreaterThan(rate.MaxWait)
            .WithMessage(o => $"{Section}:RequestTimeout ({o.RequestTimeout}) must be longer than {RateLimitOptions.Section}:MaxWait " +
                              $"({rate.MaxWait}), which it includes.");
    }
}
