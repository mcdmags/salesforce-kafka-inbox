namespace SalesforceSync.Inbox;

/// <summary>
/// An inbox table as the sync engine uses it: store an event, read the next batch, then record what became of it.
/// The row type selects the table (see <see cref="SyncDbContext"/>), so one implementation serves every object.
/// </summary>
public interface IRepository<TEntity> where TEntity : InboxRow, new()
{
    /// <summary>Stores one event as a new pending row. Nothing makes a row unique: see <see cref="InboxRepository{TEntity}.AddAsync"/>.</summary>
    Task AddAsync(string entityId, string payload, CancellationToken ct);

    /// <summary>Reads up to <see cref="SyncOptions.BatchSize"/> entities of the table; no events = nothing to do.</summary>
    Task<InboxBatch> ReadBatchAsync(string entityType, CancellationToken ct);

    /// <summary>Records every entity's outcome in one transaction.</summary>
    Task RecordAsync(InboxBatch batch, BatchOutcomes outcomes, CancellationToken ct);

    /// <summary>
    /// The write's outcome is unknown: holds every row of the batch for <see cref="SyncOptions.UnknownOutcomeHold"/>,
    /// with no retry spent.
    /// </summary>
    Task HoldAsync(InboxBatch batch, string error, CancellationToken ct);

    /// <summary>Rate limited: puts every row of the batch off for <paramref name="delay"/> without spending a retry.</summary>
    Task DeferAsync(InboxBatch batch, TimeSpan delay, CancellationToken ct);

    /// <summary>
    /// The batch failed as a whole (a Salesforce call failed that provably wasn't applied): every row of the batch is
    /// retried with backoff.
    /// </summary>
    Task FailAllAsync(InboxBatch batch, string error, string? code, CancellationToken ct);
}