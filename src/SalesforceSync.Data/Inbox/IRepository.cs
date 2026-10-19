namespace SalesforceSync.Data.Inbox;

/// <summary>
/// An inbox table: store events, claim a batch, record outcomes. The row type picks the table. Every outcome needs the
/// row still under the claim's lease with the event the claim read, so a worker whose lease ran out, or whose entity
/// another worker took over, records nothing.
/// </summary>
public interface IRepository<TRow> where TRow : InboxRow
{
    /// <summary>Stores the event in its entity's row unless the row holds a newer one.</summary>
    Task AddAsync(string entityId, DateTimeOffset eventTime, string payload, CancellationToken ct);

    /// <summary>Leases up to <see cref="SyncOptions.BatchSize"/> due entities.</summary>
    Task<InboxClaim> ClaimAsync(CancellationToken ct);

    /// <summary>In Salesforce: done. One statement for all of them.</summary>
    Task MarkDoneAsync(InboxClaim claim, IReadOnlyList<string> entityIds, CancellationToken ct);

    /// <summary>
    /// Retried with backoff, dead-lettered at <see cref="SyncOptions.MaxRetries"/>; a
    /// <see cref="FailedEvent.Permanent"/> failure is dead-lettered now. One statement for all of them.
    /// </summary>
    Task MarkFailedAsync(InboxClaim claim, IReadOnlyList<FailedEvent> failures, CancellationToken ct);

    /// <summary>Hands the claim back without spending an attempt: rate limited, unknown write outcome, or mapping error.</summary>
    Task ReleaseAsync(InboxClaim claim, Release release, CancellationToken ct);
}

/// <summary>How a claim is handed back (<see cref="IRepository{TRow}.ReleaseAsync"/>).</summary>
/// <param name="Delay">How long the entities wait.</param>
/// <param name="Error">Error left on every row; null leaves the row's error alone (rate limited is not an error).</param>
/// <param name="OwnErrors">Rows with their own error, which replaces <paramref name="Error"/>.</param>
/// <param name="StopInbox">Every other due row waits too: a mapping error is wrong for all of them.</param>
public sealed record Release(TimeSpan Delay, string? Error = null, IReadOnlyList<FailedEvent>? OwnErrors = null, bool StopInbox = false);

/// <summary>The rows leased to this worker, one per entity.</summary>
public sealed record InboxClaim(IReadOnlyList<InboxEvent> Events)
{
    public string[] EntityIds => [.. Events.Select(e => e.EntityId)];

    public DateTimeOffset[] EventTimes => [.. Events.Select(e => e.EventTime)];

    public DateTimeOffset[] LeaseEnds => [.. Events.Select(e => e.LeaseEnd)];

    /// <summary>The claimed rows of <paramref name="entityIds"/>, in their order.</summary>
    public InboxEvent[] Of(IEnumerable<string> entityIds)
    {
        var rows = Events.ToDictionary(e => e.EntityId);
        return [.. entityIds.Select(id => rows[id])];
    }
}

/// <param name="EntityId">Row key.</param>
/// <param name="Payload">The event, JSON.</param>
/// <param name="EventTime">
/// As the claim read it. Outcomes apply only while the row still has it: a newer event stored meanwhile must not be
/// marked done (<c>InboxRepository.StillClaimed</c>).
/// </param>
/// <param name="LeaseEnd">
/// next_attempt_at as the claim set it. Only another claim changes it while the lease runs (a store keeps it), so a
/// row that still has it is still this claim's: a retried or late statement can't end another worker's lease.
/// </param>
public sealed record InboxEvent(string EntityId, string Payload, DateTimeOffset EventTime, DateTimeOffset LeaseEnd);

/// <param name="EntityId">Row key.</param>
/// <param name="Error">Stored as the row's last_error.</param>
/// <param name="Permanent">Retry can't fix it: dead-lettered at once (<see cref="IRepository{TRow}.MarkFailedAsync"/>).</param>
public sealed record FailedEvent(string EntityId, string Error, bool Permanent = false);
