using Microsoft.Extensions.Options;
using SalesforceSync.Sync.Upsert;

namespace SalesforceSync.Inbox;

public sealed class SyncOptions
{
    public const string Section = "Sync";

    /// <summary>Parallel worker loops in this process.</summary>
    public int Workers { get; set; } = 4;

    /// <summary>
    /// Entities per batch, 1 to <see cref="ReadQueryBuilder.MaxEntitiesPerBatch"/> (100): a batch is read with one
    /// Composite request, which carries 5 queries of up to 100 external ids (very long ids split the read over more).
    /// </summary>
    public int BatchSize { get; set; } = 75;

    /// <summary>
    /// Pending events of one entity taken per claim (the oldest first). An event is the full record, so only the
    /// newest of them is written; the bound keeps a deep backlog for one entity from being claimed and loaded whole.
    /// What is left is taken by the following claims.
    /// </summary>
    public int MaxEventsPerEntity { get; set; } = 20;

    /// <summary>
    /// How long a claim owns its entities (sf_sync_state.locked_until; lock_token says who). Between the claim
    /// transaction and the outcome transaction nothing else holds them: no transaction or row lock spans the Salesforce
    /// calls. The worker stops calling Salesforce at 80% of it, and a worker that crashes needs no cleanup: the lease
    /// expires and the entities are claimable again. Must be longer than <c>Salesforce:RequestTimeout</c>.
    /// </summary>
    public TimeSpan Lease { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// How long an entity is left alone after a write to Salesforce whose outcome never arrived (timeout, lost
    /// connection, gateway error): Salesforce may still apply that write, and syncing a newer event before it lands
    /// would let the late write overwrite newer data. The default is Salesforce's API request timeout (10 minutes),
    /// after which the lost request has completed or been stopped.
    /// </summary>
    public TimeSpan UnknownOutcomeHold { get; set; } = TimeSpan.FromMinutes(10);

    /// <summary>
    /// Claims of an event (its attempt_count) before it is dead-lettered on a transient failure (permanent errors:
    /// immediately).
    /// </summary>
    public int MaxRetries { get; set; } = 8;

    /// <summary>
    /// The entity's backoff after the first transient failure; doubles per attempt (of its most-attempted failed event)
    /// up to <see cref="MaxRetryDelay"/>.
    /// </summary>
    public TimeSpan RetryBaseDelay { get; set; } = TimeSpan.FromSeconds(30);

    public TimeSpan MaxRetryDelay { get; set; } = TimeSpan.FromHours(1);
    public TimeSpan IdleDelay { get; set; } = TimeSpan.FromSeconds(1);
}

/// <summary>
/// Fails startup on values that would make the workers silently do nothing (no loops) or exceed what
/// one batch can carry.
/// </summary>
public sealed class SyncOptionsValidator : IValidateOptions<SyncOptions>
{
    public ValidateOptionsResult Validate(string? name, SyncOptions o)
    {
        var errors = new List<string>();
        void AtLeastOne(string setting, int value)
        {
            if (value < 1)
            {
                errors.Add($"{SyncOptions.Section}:{setting} must be at least 1.");
            }
        }
        void Positive(string setting, TimeSpan value)
        {
            if (value <= TimeSpan.Zero)
            {
                errors.Add($"{SyncOptions.Section}:{setting} must be positive.");
            }
        }

        AtLeastOne(nameof(o.Workers), o.Workers);
        if (o.BatchSize is < 1 or > ReadQueryBuilder.MaxEntitiesPerBatch)
        {
            errors.Add($"{SyncOptions.Section}:BatchSize ({o.BatchSize}) must be 1 to {ReadQueryBuilder.MaxEntitiesPerBatch} " +
                       "(a batch is read with one Composite request: 5 queries of up to 100 external ids).");
        }

        AtLeastOne(nameof(o.MaxEventsPerEntity), o.MaxEventsPerEntity);
        Positive(nameof(o.Lease), o.Lease);
        Positive(nameof(o.UnknownOutcomeHold), o.UnknownOutcomeHold);
        AtLeastOne(nameof(o.MaxRetries), o.MaxRetries);
        if (o.RetryBaseDelay < TimeSpan.Zero || o.MaxRetryDelay < o.RetryBaseDelay)
        {
            errors.Add($"{SyncOptions.Section}:RetryBaseDelay must be >= 0 and at most MaxRetryDelay.");
        }

        return errors.Count == 0 ? ValidateOptionsResult.Success : ValidateOptionsResult.Fail(errors);
    }
}