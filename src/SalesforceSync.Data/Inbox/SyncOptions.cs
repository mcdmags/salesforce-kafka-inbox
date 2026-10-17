using Microsoft.Extensions.Options;

namespace SalesforceSync.Data.Inbox;

public sealed class SyncOptions
{
    public const string Section = "Sync";

    /// <summary>Parallel worker loops in this process.</summary>
    public int Workers { get; set; } = 4;

    /// <summary>Max in-flight Salesforce batches across ALL processes.</summary>
    public int MaxConcurrentRequests { get; set; } = 6;

    /// <summary>
    /// Entities per batch, 1 to <see cref="MaxEntitiesPerBatch"/>: with the default cap a batch is read with one
    /// Composite request, which carries 5 queries of up to 100 external ids (very long ids split the read over more).
    /// </summary>
    public int BatchSize { get; set; } = 75;

    /// <summary>
    /// The most entities one batch may hold, the ceiling of <see cref="BatchSize"/>. Not a Salesforce limit of its own:
    /// with the row types' 100 external ids per query, 100 keeps a batch's read to one Composite request while the ids
    /// are short (5 queries of 100 ids, where an entity can need up to 3 ids: a relationship, itself and two Accounts).
    /// Raise it to batch more entities per claim: ids then split the read over more calls, not a failure, and the write
    /// side would allow more (5 collections of the row types' 180 records).
    /// </summary>
    public int MaxEntitiesPerBatch { get; set; } = 100;

    /// <summary>
    /// How long a claim owns its entities (until its inbox row's next_attempt_at; lock_token says who). Between the claim
    /// transaction and the outcome transaction nothing else holds them: no transaction or row lock spans the Salesforce
    /// calls. The worker stops calling Salesforce at 80% of it, and a worker that crashes needs no cleanup: the lease
    /// expires and the entities are claimable again. Must be longer than <c>SalesforceLimits:RequestTimeout</c>.
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
    /// Transient failures of an entity's event (its row's attempt_count, reset by each newer event) at which it is
    /// dead-lettered (permanent errors: immediately).
    /// </summary>
    public int MaxRetries { get; set; } = 8;

    /// <summary>
    /// The entity's backoff after the first transient failure; doubles per failure (its row's attempt_count) up to
    /// <see cref="MaxRetryDelay"/>.
    /// </summary>
    public TimeSpan RetryBaseDelay { get; set; } = TimeSpan.FromSeconds(30);

    public TimeSpan MaxRetryDelay { get; set; } = TimeSpan.FromHours(1);

    public TimeSpan IdleDelay { get; set; } = TimeSpan.FromSeconds(1);
}

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
        AtLeastOne(nameof(o.MaxConcurrentRequests), o.MaxConcurrentRequests);
        AtLeastOne(nameof(o.MaxEntitiesPerBatch), o.MaxEntitiesPerBatch);
        if (o.BatchSize < 1 || o.BatchSize > o.MaxEntitiesPerBatch)
        {
            errors.Add($"{SyncOptions.Section}:BatchSize ({o.BatchSize}) must be 1 to {SyncOptions.Section}:MaxEntitiesPerBatch " +
                       $"({o.MaxEntitiesPerBatch}).");
        }

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