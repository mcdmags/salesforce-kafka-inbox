using FluentValidation;
using SalesforceSync.Infrastructure.RateLimiting;

namespace SalesforceSync.Hosting;

/// <summary>Fails startup on pauses or waits that would let calls through when they shouldn't, or never.</summary>
public sealed class RateLimitOptionsValidator : AbstractValidator<RateLimitOptions>
{
    private const string Section = RateLimitOptions.Section;

    public RateLimitOptionsValidator()
    {
        RuleFor(o => o.MaxWait).GreaterThanOrEqualTo(TimeSpan.Zero).WithMessage($"{Section}:MaxWait must be 0 or more.");
        RuleFor(o => o.ConcurrentLimitPause).GreaterThan(TimeSpan.Zero).WithMessage($"{Section}:ConcurrentLimitPause must be positive.");
        RuleFor(o => o.DailyHardPause).GreaterThan(TimeSpan.Zero).WithMessage($"{Section}:DailyHardPause must be positive.");
        RuleFor(o => o.DailyHardRatio)
            .GreaterThan(0)
            .WithMessage(o => $"{Section}:DailyHardRatio ({o.DailyHardRatio}) must be a share of the daily allocation above 0 " +
                              "(1.0: the allocation itself).");
    }
}
