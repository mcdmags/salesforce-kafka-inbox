using Microsoft.Extensions.Options;
using SalesforceSync.Salesforce;

namespace SalesforceSync.Inbox;

public sealed class SyncOptions
{
    public const string Section = "Sync";

    /// <summary>Parallel worker loops in this process.</summary>
    public int Workers { get; set; } = 4;

    /// <summary>Max in-flight Salesforce batches across ALL processes.</summary>
    public int MaxConcurrentRequests { get; set; } = 6;

    /// <summary>Entities per batch (= graphs per Composite Graph request, max 75).</summary>
    public int BatchSize { get; set; } = 75;

    /// <summary>
    /// Pending events of one entity taken per claim (the oldest first). An event is the full record, so only the
    /// newest of them is written; the bound keeps a deep backlog for one entity from being leased and loaded whole.
    /// What is left is taken by the following claims.
    /// </summary>
    public int MaxEventsPerEntity { get; set; } = 20;

    public TimeSpan Lease { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// How long an entity is left alone after a write to Salesforce whose outcome never arrived (timeout, lost
    /// connection, gateway error): Salesforce may still apply that write, and syncing a newer event before it lands
    /// would let the late write overwrite newer data. The default is Salesforce's API request timeout (10 minutes),
    /// after which the lost request has completed or been stopped.
    /// </summary>
    public TimeSpan UnknownOutcomeHold { get; set; } = TimeSpan.FromMinutes(10);

    /// <summary>Attempts before a transiently failing entity is dead-lettered (permanent errors: immediately).</summary>
    public int MaxRetries { get; set; } = 8;

    /// <summary>Backoff after the first transient failure; doubles per attempt up to <see cref="MaxRetryDelay"/>.</summary>
    public TimeSpan RetryBaseDelay { get; set; } = TimeSpan.FromSeconds(30);

    public TimeSpan MaxRetryDelay { get; set; } = TimeSpan.FromHours(1);
    public TimeSpan IdleDelay { get; set; } = TimeSpan.FromSeconds(1);
}

/// <summary>
/// Fails startup on values that would make the workers silently do nothing (no slots, no loops) or exceed what
/// one Composite Graph request can carry.
/// </summary>
public sealed class SyncOptionsValidator : IValidateOptions<SyncOptions>
{
    public ValidateOptionsResult Validate(string? name, SyncOptions o)
    {
        var errors = new List<string>();
        if (o.Workers < 1) errors.Add($"{SyncOptions.Section}:Workers must be at least 1.");
        if (o.MaxConcurrentRequests < 1) errors.Add($"{SyncOptions.Section}:MaxConcurrentRequests must be at least 1.");
        if (o.BatchSize is < 1 or > SalesforceRestClient.MaxGraphsPerRequest)
            errors.Add($"{SyncOptions.Section}:BatchSize ({o.BatchSize}) must be 1 to {SalesforceRestClient.MaxGraphsPerRequest} " +
                       "(one Composite Graph per entity).");
        if (o.MaxEventsPerEntity < 1) errors.Add($"{SyncOptions.Section}:MaxEventsPerEntity must be at least 1.");
        if (o.Lease <= TimeSpan.Zero) errors.Add($"{SyncOptions.Section}:Lease must be positive.");
        if (o.UnknownOutcomeHold <= TimeSpan.Zero) errors.Add($"{SyncOptions.Section}:UnknownOutcomeHold must be positive.");
        if (o.MaxRetries < 1) errors.Add($"{SyncOptions.Section}:MaxRetries must be at least 1.");
        if (o.RetryBaseDelay < TimeSpan.Zero || o.MaxRetryDelay < o.RetryBaseDelay)
            errors.Add($"{SyncOptions.Section}:RetryBaseDelay must be >= 0 and at most MaxRetryDelay.");
        return errors.Count == 0 ? ValidateOptionsResult.Success : ValidateOptionsResult.Fail(errors);
    }
}
