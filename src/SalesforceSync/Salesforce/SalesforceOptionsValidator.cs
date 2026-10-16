using Microsoft.Extensions.Options;
using SalesforceSync.Inbox;

namespace SalesforceSync.Salesforce;

/// <summary>Fails startup on a Salesforce client configuration that can't work against the API or the sync's lease.</summary>
public sealed class SalesforceOptionsValidator(IOptions<SyncOptions> sync) : IValidateOptions<SalesforceOptions>
{
    public ValidateOptionsResult Validate(string? name, SalesforceOptions o)
    {
        var errors = new List<string>();

        if (!Uri.IsWellFormedUriString(o.InstanceUrl, UriKind.Absolute))
        {
            errors.Add($"{SalesforceOptions.Section}:InstanceUrl must be an absolute URL");
        }

        if (!string.IsNullOrWhiteSpace(o.TokenUrl) && !Uri.IsWellFormedUriString(o.TokenUrl, UriKind.Absolute))
        {
            errors.Add($"{SalesforceOptions.Section}:TokenUrl must be an absolute URL (or empty: the instance's own)");
        }

        if (o.RequestTimeout <= TimeSpan.Zero || o.RequestTimeout > SalesforceOptions.MaxApiTimeout)
        {
            errors.Add($"{SalesforceOptions.Section}:RequestTimeout ({o.RequestTimeout}) must be > 0 and at most " +
                       $"the Salesforce API timeout of {SalesforceOptions.MaxApiTimeout}.");
        }

        if (sync.Value.Lease <= o.RequestTimeout)
        {
            errors.Add($"{SyncOptions.Section}:Lease ({sync.Value.Lease}) must be longer than {SalesforceOptions.Section}:" +
                       $"RequestTimeout ({o.RequestTimeout}), so a call that is merely slow can't outlive the lease on the entities it writes.");
        }

        return errors.Count == 0 ? ValidateOptionsResult.Success : ValidateOptionsResult.Fail(errors);
    }
}