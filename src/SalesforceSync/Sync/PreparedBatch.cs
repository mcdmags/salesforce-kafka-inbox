using SalesforceSync.Data.Inbox;
using SalesforceSync.Infrastructure.Salesforce;

namespace SalesforceSync.Sync;

/// <summary>An entity to write: the unit mapped from its claimed event.</summary>
/// <param name="EntityId">The entity: the Kafka message key.</param>
/// <param name="Unit">The records to upsert for it.</param>
/// <param name="RowId">The entity's inbox row id.</param>
internal sealed record Work(string EntityId, SyncUnit Unit, long RowId);

/// <summary>
/// What to do with a claim's rows: entities to write, and rows that are invalid on their own. Decided
/// (<see cref="Prepare{TEvent}"/>) before Salesforce is involved. No I/O.
/// </summary>
/// <param name="Work">The entities to write, one per entity.</param>
/// <param name="Invalid">The rows whose events are invalid on their own, to dead-letter as <see cref="SalesforceErrors.InvalidEvent"/>.</param>
internal sealed record PreparedBatch(List<Work> Work, List<FailedEvent> Invalid)
{
    /// <summary>
    /// Decides what to do with each claimed entity's row, which holds its newest event (the inbox keeps one row per
    /// entity: a newer event overwrites it, and an event not newer than a synced one is never stored). A row that can't
    /// be read (or whose event is about another entity than the row says) is invalid. Otherwise it is mapped to the
    /// unit to write; if it can't be mapped, it is invalid.
    /// </summary>
    /// <param name="strategy">Reads, keys and maps the object's events.</param>
    /// <param name="rows">The claimed rows, one per entity.</param>
    public static PreparedBatch Prepare<TEvent>(SyncStrategy<TEvent> strategy, IReadOnlyList<InboxEvent> rows)
    {
        var batch = new PreparedBatch([], []);

        foreach (var row in rows)
        {
            var entityId = row.EntityId;
            if (!strategy.TryRead(row.Payload, out var read, out var problem))
            {
                Reject(row, problem);
                continue;
            }
            if (!string.Equals(read.EntityId, entityId, StringComparison.Ordinal))
            {
                // The lease is on the row's entity; writing another entity under it would bypass that entity's own
                Reject(row, $"the event is about '{read.EntityId}', but the row's entity_id is '{entityId}'");
                continue;
            }

            try
            {
                batch.Work.Add(new Work(entityId, strategy.Map(entityId, read.Event), row.RowId));
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                Reject(row, ex.Message);
            }
        }
        return batch;

        void Reject(InboxEvent row, string why)
        {
            batch.Invalid.Add(new FailedEvent(row.RowId, $"{strategy.EntityType} {row.EntityId}: {why}", SalesforceErrors.InvalidEvent));
        }
    }

    /// <summary>The units to write, keyed by entity id, as the Salesforce step takes them.</summary>
    public List<(string Key, SyncUnit Unit)> Units => [.. Work.Select(w => (w.EntityId, w.Unit))];

    /// <summary>
    /// What to record in the inbox once Salesforce has answered: saved or unchanged entities are done, permanent
    /// failures and invalid rows are dead-lettered, the rest is retried. Entities not attempted get no outcome: their
    /// leases end and they are due again, with no attempt spent.
    /// </summary>
    /// <param name="results">The outcome of every entity in <see cref="Work"/>, by entity id.</param>
    public BatchOutcomes Outcomes(IReadOnlyDictionary<string, SyncResult> results)
    {
        var synced = new List<long>();
        var dead = new List<FailedEvent>(Invalid);
        var failed = new List<FailedEvent>();

        foreach (var work in Work)
        {
            var result = results[work.EntityId];
            switch (result.Outcome)
            {
                case SyncOutcome.Saved or SyncOutcome.Unchanged: synced.Add(work.RowId); break;
                case SyncOutcome.NotAttempted: break;
                case SyncOutcome.PermanentFailure: dead.Add(Failure(work, result)); break;
                default: failed.Add(Failure(work, result)); break;
            }
        }
        return new BatchOutcomes(synced, dead, failed);
    }

    private static FailedEvent Failure(Work work, SyncResult result)
    {
        return new(work.RowId, string.Join("; ", result.Errors), result.PrimaryError?.Code);
    }
}