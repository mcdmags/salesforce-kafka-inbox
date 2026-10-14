namespace SalesforceSync.Inbox;

/// <summary>
/// The inbox as the sync engine uses it: claim a batch of entities, then record what became of them. Every recording
/// is conditional on the claim's lease token, so a worker whose lease expired records nothing.
/// </summary>
public interface IInboxRepository
{
    /// <summary>Leases up to <see cref="SyncOptions.BatchSize"/> entities of the table; no events = nothing to do.</summary>
    Task<InboxClaim> ClaimAsync(string inboxTable, string entityType, CancellationToken ct);

    /// <summary>Records every entity's outcome in one transaction, including entities held or deferred on their own.</summary>
    Task RecordAsync(InboxClaim claim, BatchOutcomes outcomes, CancellationToken ct);

    /// <summary>
    /// The write's outcome is unknown: holds every claimed row for <see cref="SyncOptions.UnknownOutcomeHold"/>, with no
    /// token and no retry spent.
    /// </summary>
    Task HoldAsync(InboxClaim claim, string error, CancellationToken ct);

    /// <summary>Rate limited: hands every claimed row back for <paramref name="delay"/> without spending a retry.</summary>
    Task DeferAsync(InboxClaim claim, TimeSpan delay, CancellationToken ct);

    /// <summary>The batch failed as a whole before anything was written: every claimed row is retried with backoff.</summary>
    Task FailAllAsync(InboxClaim claim, string error, string? code, CancellationToken ct);
}
