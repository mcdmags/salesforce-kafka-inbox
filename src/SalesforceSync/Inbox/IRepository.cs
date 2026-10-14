namespace SalesforceSync.Inbox;

/// <summary>
/// An inbox table as the sync engine uses it: store an event, claim a batch of entities, then record what became of
/// them. The row type selects the table (see <see cref="SyncDbContext"/>), so one implementation serves every object.
/// Every recording is conditional on the claim's lease token, so a worker whose lease expired records nothing.
/// </summary>
public interface IRepository<TEntity> where TEntity : InboxRow, new()
{
    /// <summary>Stores one event as a new pending row. Nothing makes a row unique: see <see cref="InboxRepository{TEntity}.AddAsync"/>.</summary>
    Task AddAsync(string entityId, string payload, CancellationToken ct);

    /// <summary>Leases up to <see cref="SyncOptions.BatchSize"/> entities of the table; no events = nothing to do.</summary>
    Task<InboxClaim> ClaimAsync(string entityType, CancellationToken ct);

    /// <summary>Records every entity's outcome in one transaction, including entities held on their own.</summary>
    Task RecordAsync(InboxClaim claim, BatchOutcomes outcomes, CancellationToken ct);

    /// <summary>
    /// The write's outcome is unknown: holds every claimed row for <see cref="SyncOptions.UnknownOutcomeHold"/>, with no
    /// token and no retry spent.
    /// </summary>
    Task HoldAsync(InboxClaim claim, string error, CancellationToken ct);

    /// <summary>The batch failed as a whole before anything was written: every claimed row is retried with backoff.</summary>
    Task FailAllAsync(InboxClaim claim, string error, string? code, CancellationToken ct);
}