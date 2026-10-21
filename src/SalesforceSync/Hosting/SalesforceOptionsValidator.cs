using System.Text.RegularExpressions;
using FluentValidation;
using SalesforceSync.Infrastructure.Salesforce;

namespace SalesforceSync.Hosting;

/// <summary>Fails startup when the org's address or the API path the calls are built from is missing or malformed.</summary>
public sealed partial class SalesforceOptionsValidator : AbstractValidator<SalesforceOptions>
{
    private const string Section = SalesforceOptions.Section;

    [GeneratedRegex(@"^v\d+\.\d$")]
    private static partial Regex ApiVersionFormat();

    public SalesforceOptionsValidator()
    {
        RuleFor(o => o.InstanceUrl).Must(IsAbsoluteUrl).WithMessage($"{Section}:InstanceUrl must be an absolute URL");
        RuleFor(o => o.TokenUrl).Must(IsAbsoluteUrl).WithMessage($"{Section}:TokenUrl must be an absolute URL (or empty)");
        RuleFor(o => o.BaseApiPath).Must(p => p.StartsWith('/'))
            .WithMessage($"{Section}:BaseApiPath must be set, starting with '/' (e.g. /services/data)");
        RuleFor(o => o.ApiVersion).Must(v => ApiVersionFormat().IsMatch(v)).WithMessage($"{Section}:ApiVersion must be set, like v62.0");
    }

    private static bool IsAbsoluteUrl(string url)
    {
        return Uri.IsWellFormedUriString(url, UriKind.Absolute);
    }
}
