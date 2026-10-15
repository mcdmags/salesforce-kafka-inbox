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
    Task<InboxClaim> ClaimAsync(CancellationToken ct);

    /// <summary>Records every entity's outcome in one transaction.</summary>
    Task RecordAsync(InboxClaim claim, BatchOutcomes outcomes, CancellationToken ct);

    /// <summary>
    /// The write's outcome is unknown: holds every claimed row for <see cref="SyncOptions.UnknownOutcomeHold"/>, with no
    /// token and no retry spent.
    /// </summary>
    Task HoldAsync(InboxClaim claim, string error, CancellationToken ct);

    /// <summary>
    /// The batch failed as a whole (a Salesforce call failed that provably wasn't applied): every claimed row is
    /// retried with backoff.
    /// </summary>
    Task FailAllAsync(InboxClaim claim, string error, string? code, CancellationToken ct);
}

/// <summary>A batch of entities leased to this worker: their claimed rows, and what was already synced for them.</summary>
/// <param name="Token">Identifies the lease; every outcome statement is conditional on it.</param>
/// <param name="SyncedTimes">
/// Entity id -> the time (microseconds since the epoch) of its newest synced event: the newest synced_time of its done
/// rows.
/// </param>
public sealed record InboxClaim(
    Guid Token,
    IReadOnlyList<InboxEvent> Events,
    IReadOnlyDictionary<string, long> SyncedTimes)
{
    public long[] EventIds => [.. Events.Select(e => e.Id)];
}

/// <summary>A pending inbox row as claimed: the entity it belongs to and the raw event.</summary>
public sealed record InboxEvent(long Id, string EntityId, string Payload, int RetryCount);

/// <summary>
/// An entity whose newest event (row <paramref name="EventId"/>) is in Salesforce: the row ends done with
/// <paramref name="EventTime"/> as its synced_time, the entity's watermark.
/// </summary>
public sealed record SyncedEntity(string EntityId, long EventTime, long EventId);

/// <summary>An inbox row with the error to store on it.</summary>
public sealed record FailedEvent(long EventId, string Error, string? Code);

/// <summary>What to record for the rows of a claim, by kind of outcome.</summary>
/// <param name="Synced">Done; the entity's watermark advances.</param>
/// <param name="Superseded">Not written: an event of the same entity is newer, or as new was already synced.</param>
/// <param name="Released">Back to the inbox for the next claim, no retry spent.</param>
/// <param name="Dead">Dead-lettered at once.</param>
/// <param name="Failed">Retried with backoff, dead-lettered after <see cref="SyncOptions.MaxRetries"/>.</param>
public sealed record BatchOutcomes(
    IReadOnlyList<SyncedEntity> Synced,
    IReadOnlyList<long> Superseded,
    IReadOnlyList<long> Released,
    IReadOnlyList<FailedEvent> Dead,
    IReadOnlyList<FailedEvent> Failed);