using FluentValidation;
using SalesforceSync.Data.Inbox;

namespace SalesforceSync.Hosting;

/// <summary>
/// Fails startup on values that would make the workers silently do nothing (no loops) or exceed what one batch can
/// carry.
/// </summary>
public sealed class SyncOptionsValidator : AbstractValidator<SyncOptions>
{
    private const string Section = SyncOptions.Section;

    public SyncOptionsValidator()
    {
        RuleFor(o => o.Workers).GreaterThanOrEqualTo(1).WithMessage($"{Section}:Workers must be at least 1.");
        RuleFor(o => o.MaxEntitiesPerBatch).GreaterThanOrEqualTo(1).WithMessage($"{Section}:MaxEntitiesPerBatch must be at least 1.");
        RuleFor(o => o.BatchSize)
            .Must((o, size) => size >= 1 && size <= o.MaxEntitiesPerBatch)
            .WithMessage(o => $"{Section}:BatchSize ({o.BatchSize}) must be 1 to {Section}:MaxEntitiesPerBatch ({o.MaxEntitiesPerBatch}).");
        RuleFor(o => o.Lease).GreaterThan(TimeSpan.Zero).WithMessage($"{Section}:Lease must be positive.");
        RuleFor(o => o.UnknownOutcomeHold).GreaterThan(TimeSpan.Zero).WithMessage($"{Section}:UnknownOutcomeHold must be positive.");
        RuleFor(o => o.MappingErrorHold).GreaterThan(TimeSpan.Zero).WithMessage($"{Section}:MappingErrorHold must be positive.");
        RuleFor(o => o.StorageFullHold).GreaterThan(TimeSpan.Zero).WithMessage($"{Section}:StorageFullHold must be positive.");
        RuleFor(o => o.RequestLimitHold).GreaterThan(TimeSpan.Zero).WithMessage($"{Section}:RequestLimitHold must be positive.");
        RuleFor(o => o.MaxRetries).GreaterThanOrEqualTo(1).WithMessage($"{Section}:MaxRetries must be at least 1.");
        RuleFor(o => o.RetryBaseDelay)
            .Must((o, delay) => delay >= TimeSpan.Zero && delay <= o.MaxRetryDelay)
            .WithMessage($"{Section}:RetryBaseDelay must be >= 0 and at most MaxRetryDelay.");
    }
}
