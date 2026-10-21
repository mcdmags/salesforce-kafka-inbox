using FluentValidation;
using Microsoft.Extensions.Options;

namespace SalesforceSync.Hosting;

/// <summary>
/// Runs a FluentValidation validator as an options check, so a wrong setting fails startup (with
/// <c>ValidateOnStart</c>) and lists every problem at once.
/// </summary>
public sealed class FluentOptionsValidator<TOptions>(IValidator<TOptions> validator) : IValidateOptions<TOptions>
    where TOptions : class
{
    public ValidateOptionsResult Validate(string? name, TOptions options)
    {
        var result = validator.Validate(options);
        return result.IsValid ? ValidateOptionsResult.Success : ValidateOptionsResult.Fail(result.Errors.Select(e => e.ErrorMessage));
    }
}
