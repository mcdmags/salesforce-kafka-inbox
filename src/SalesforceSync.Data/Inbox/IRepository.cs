namespace SalesforceSync.Data.Inbox;

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
    /// Hands every claimed entity back without spending an attempt, due again after <see cref="Release.Delay"/>: rate
    /// limited (no error), the write's outcome unknown (held, with the error), or a mapping error (the inbox stopped).
    /// </summary>
    Task ReleaseAsync(InboxClaim claim, Release release, CancellationToken ct);

    /// <summary>
    /// The batch failed as a whole (a Salesforce call failed that provably wasn't applied): every claimed row is
    /// retried with backoff.
    /// </summary>
    Task FailAllAsync(InboxClaim claim, string error, CancellationToken ct);
}

/// <summary>How a claim is handed back without an attempt spent (<see cref="IRepository{TEntity}.ReleaseAsync"/>).</summary>
/// <param name="Delay">How long the entities wait: the rate limiter's delay, the hold for an unknown outcome, the stop for a mapping error.</param>
/// <param name="Error">The error to leave on every released row, or null to leave the row's error alone (rate limited).</param>
/// <param name="OwnErrors">Entities with an error of their own, which replaces <paramref name="Error"/> on their rows.</param>
/// <param name="StopInbox">Every other row of the inbox that is due waits <paramref name="Delay"/> too (a mapping error is wrong for all of them).</param>
public sealed record Release(TimeSpan Delay, string? Error = null, IReadOnlyList<FailedEvent>? OwnErrors = null, bool StopInbox = false);

/// <summary>A batch of entities leased to this worker: their claimed rows.</summary>
/// <param name="Token">Identifies the lease; every outcome statement is conditional on it.</param>
/// <param name="Events">The claimed row of every leased entity: one per entity.</param>
public sealed record InboxClaim(Guid Token, IReadOnlyList<InboxEvent> Events)
{
    /// <summary>The entity ids of the claimed rows: a row's key (one row per entity).</summary>
    public string[] EntityIds => [.. Events.Select(e => e.EntityId)];

    /// <summary>The event_time of each claimed row as the claim read it, in <see cref="EntityIds"/> order.</summary>
    public DateTimeOffset[] EventTimes => [.. Events.Select(e => e.EventTime)];

    /// <summary>The event_time the claim read for each of <paramref name="entityIds"/>, in their order.</summary>
    public DateTimeOffset[] TimesOf(IEnumerable<string> entityIds)
    {
        var times = Events.ToDictionary(e => e.EntityId, e => e.EventTime);
        return [.. entityIds.Select(id => times[id])];
    }
}

/// <summary>A pending inbox row as claimed: the entity it belongs to and the raw event.</summary>
/// <param name="EntityId">The entity the event is about: the Kafka message key, and the row's primary key (one row per entity).</param>
/// <param name="Payload">The event as received, JSON.</param>
/// <param name="EventTime">
/// The row's event_time as the claim read it: every outcome is recorded only while the row still has it (a newer event
/// stored meanwhile has overwritten the row; see <c>InboxRepository.StillClaimed</c>).
/// </param>
public sealed record InboxEvent(string EntityId, string Payload, DateTimeOffset EventTime);

/// <summary>An inbox row with the error to store on it.</summary>
/// <param name="EntityId">The entity whose row it is: its primary key.</param>
/// <param name="Error">The error to store; it names the Salesforce (or sync) error code, e.g. <c>acct: DEPENDENCY_NOT_SYNCED …</c>.</param>
public sealed record FailedEvent(string EntityId, string Error);

/// <summary>What to record for the rows of a claim, by kind of outcome: the entities' ids, which are their rows' keys.</summary>
/// <param name="Synced">In Salesforce: done.</param>
/// <param name="Dead">Dead-lettered at once.</param>
/// <param name="Failed">Retried with backoff, dead-lettered after <see cref="SyncOptions.MaxRetries"/>.</param>
public sealed record BatchOutcomes(
    IReadOnlyList<string> Synced,
    IReadOnlyList<FailedEvent> Dead,
    IReadOnlyList<FailedEvent> Failed);