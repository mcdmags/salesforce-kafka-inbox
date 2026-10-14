using Microsoft.Extensions.Options;
using SalesforceSync.Salesforce;

namespace SalesforceSync.Inbox;

/// <summary>
/// Fails startup on values that would make the workers silently do nothing (no slots, no loops) or exceed what
/// one batch can carry.
/// </summary>
public sealed class SyncOptionsValidator : IValidateOptions<SyncOptions>
{
    public ValidateOptionsResult Validate(string? name, SyncOptions o)
    {
        var errors = new List<string>();
        void AtLeastOne(string setting, int value)
        {
            if (value < 1) errors.Add($"{SyncOptions.Section}:{setting} must be at least 1.");
        }
        void Positive(string setting, TimeSpan value)
        {
            if (value <= TimeSpan.Zero) errors.Add($"{SyncOptions.Section}:{setting} must be positive.");
        }

        AtLeastOne(nameof(o.Workers), o.Workers);
        AtLeastOne(nameof(o.MaxConcurrentRequests), o.MaxConcurrentRequests);
        if (o.BatchSize is < 1 or > SalesforceRestClient.MaxEntitiesPerBatch)
            errors.Add($"{SyncOptions.Section}:BatchSize ({o.BatchSize}) must be 1 to {SalesforceRestClient.MaxEntitiesPerBatch} " +
                       "(a batch is read with one Composite request: 5 queries of 100 external ids).");
        AtLeastOne(nameof(o.MaxEventsPerEntity), o.MaxEventsPerEntity);
        Positive(nameof(o.Lease), o.Lease);
        Positive(nameof(o.UnknownOutcomeHold), o.UnknownOutcomeHold);
        AtLeastOne(nameof(o.MaxRetries), o.MaxRetries);
        if (o.RetryBaseDelay < TimeSpan.Zero || o.MaxRetryDelay < o.RetryBaseDelay)
            errors.Add($"{SyncOptions.Section}:RetryBaseDelay must be >= 0 and at most MaxRetryDelay.");
        return errors.Count == 0 ? ValidateOptionsResult.Success : ValidateOptionsResult.Fail(errors);
    }
}
