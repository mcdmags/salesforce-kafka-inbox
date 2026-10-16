namespace SalesforceSync.Inbox;

/// <summary>
/// An inbox table as the sync engine uses it: store an event, claim a batch of entities, then record what became of
/// them. The row type selects the table (see <see cref="Data.SyncDbContext"/>), so one implementation serves every object.
/// Every recording is conditional on the claim's lease token (on the entities' rows), so a worker whose lease expired
/// records nothing.
/// </summary>
public interface IRepository<TEntity> where TEntity : InboxRow, IInboxRow, new()
{
    /// <summary>
    /// Stores one event, with its time (meta.time), in its entity's row, unless the row holds a newer one, and makes its
    /// entity due: see <see cref="InboxRepository{TEntity}.AddAsync"/>.
    /// </summary>
    Task AddAsync(string entityId, DateTimeOffset eventTime, string payload, CancellationToken ct);

    /// <summary>Leases up to <see cref="SyncOptions.BatchSize"/> entities of the table; no events = nothing to do.</summary>
    Task<InboxClaim> ClaimAsync(CancellationToken ct);

    /// <summary>Records every entity's outcome in one transaction.</summary>
    Task RecordAsync(InboxClaim claim, BatchOutcomes outcomes, CancellationToken ct);

    /// <summary>
    /// The write's outcome is unknown: holds every claimed entity for <see cref="SyncOptions.UnknownOutcomeHold"/>, with
    /// no token and no attempt spent.
    /// </summary>
    Task HoldAsync(InboxClaim claim, string error, CancellationToken ct);

    /// <summary>Rate limited: hands every claimed entity back for <paramref name="delay"/> without spending an attempt.</summary>
    Task DeferAsync(InboxClaim claim, TimeSpan delay, CancellationToken ct);

    /// <summary>
    /// The batch failed as a whole (a Salesforce call failed that provably wasn't applied): every claimed row is
    /// retried with backoff.
    /// </summary>
    Task FailAllAsync(InboxClaim claim, string error, string? code, CancellationToken ct);
}

/// <summary>A batch of entities leased to this worker: their claimed rows.</summary>
/// <param name="Token">Identifies the lease; every outcome statement is conditional on it.</param>
/// <param name="Events">The claimed row of every leased entity: one per entity.</param>
public sealed record InboxClaim(Guid Token, IReadOnlyList<InboxEvent> Events)
{
    /// <summary>The inbox row ids of the claimed rows.</summary>
    public long[] RowIds => [.. Events.Select(e => e.RowId)];

    /// <summary>The event_time of each claimed row as the claim read it, in <see cref="RowIds"/> order.</summary>
    public DateTimeOffset[] EventTimes => [.. Events.Select(e => e.EventTime)];

    /// <summary>The event_time the claim read for each of <paramref name="rowIds"/>, in their order.</summary>
    public DateTimeOffset[] TimesOf(IEnumerable<long> rowIds)
    {
        var times = Events.ToDictionary(e => e.RowId, e => e.EventTime);
        return [.. rowIds.Select(id => times[id])];
    }
}

/// <summary>A pending inbox row as claimed: the entity it belongs to and the raw event.</summary>
/// <param name="RowId">The inbox row's primary key (its id column; one row per entity). Not the entity id.</param>
/// <param name="EntityId">The entity the event is about: the Kafka message key.</param>
/// <param name="Payload">The event as received, JSON.</param>
/// <param name="EventTime">
/// The row's event_time as the claim read it: every outcome is recorded only while the row still has it (a newer event
/// stored meanwhile has overwritten the row; see <see cref="InboxSql.Template.StillClaimed"/>).
/// </param>
public sealed record InboxEvent(long RowId, string EntityId, string Payload, DateTimeOffset EventTime);

/// <summary>An inbox row with the error to store on it.</summary>
/// <param name="RowId">The inbox row id (not the entity id).</param>
/// <param name="Error">The error to store.</param>
/// <param name="Code">The Salesforce (or sync) error code, if any.</param>
public sealed record FailedEvent(long RowId, string Error, string? Code);

/// <summary>What to record for the rows of a claim, by kind of outcome: inbox row ids (not entity ids).</summary>
/// <param name="Synced">In Salesforce: done.</param>
/// <param name="Dead">Dead-lettered at once.</param>
/// <param name="Failed">Retried with backoff, dead-lettered after <see cref="SyncOptions.MaxRetries"/>.</param>
public sealed record BatchOutcomes(
    IReadOnlyList<long> Synced,
    IReadOnlyList<FailedEvent> Dead,
    IReadOnlyList<FailedEvent> Failed);