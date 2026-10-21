using FluentValidation;
using SalesforceSync.Data;

namespace SalesforceSync.Hosting;

/// <summary>Fails startup on retry settings Postgres calls can't use.</summary>
public sealed class PostgresRetryOptionsValidator : AbstractValidator<PostgresRetryOptions>
{
    private const string Section = PostgresRetryOptions.Section;

    public PostgresRetryOptionsValidator()
    {
        RuleFor(o => o.MaxRetryCount).GreaterThanOrEqualTo(0).WithMessage($"{Section}:MaxRetryCount must be 0 or more.");
        RuleFor(o => o.BaseDelay).GreaterThanOrEqualTo(TimeSpan.Zero).WithMessage($"{Section}:BaseDelay must be 0 or more.");
        RuleFor(o => o.MaxRetryDelay)
            .GreaterThanOrEqualTo(o => o.BaseDelay)
            .WithMessage($"{Section}:MaxRetryDelay must be at least BaseDelay.");
    }
}
