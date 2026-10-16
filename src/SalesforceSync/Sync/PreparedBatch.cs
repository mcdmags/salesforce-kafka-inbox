using SalesforceSync.Inbox;
using SalesforceSync.Salesforce;

namespace SalesforceSync.Sync;

/// <summary>An entity to write: the unit mapped from its claimed event.</summary>
/// <param name="EntityId">The entity: the Kafka message key.</param>
/// <param name="Unit">The records to upsert for it.</param>
/// <param name="RowId">The entity's inbox row id.</param>
internal sealed record Work(string EntityId, UpsertUnit Unit, long RowId);

/// <summary>
/// What to do with a claim's rows: superseded ones, entities to write, and rows that are invalid on their own. Decided
/// (<see cref="Prepare{TEvent}"/>) before Salesforce is involved. No I/O.
/// </summary>
/// <param name="SupersededRowIds">Inbox row ids (not entity ids) of the events that are not written.</param>
/// <param name="Work">The entities to write, one per entity.</param>
/// <param name="Invalid">Inbox row ids of the events that are invalid on their own, with why.</param>
internal sealed record PreparedBatch(List<long> SupersededRowIds, List<Work> Work, List<(long RowId, string Error)> Invalid)
{
    /// <summary>
    /// Decides what to do with each claimed entity's row, which holds its newest event (the inbox keeps one row per
    /// entity: a newer event overwrites it). A row that can't be read (or whose event is about another entity than the
    /// row says) is invalid. One whose time is not after the entity's watermark (a replay or a redelivery) is
    /// superseded. Otherwise it is mapped to the unit to write; if it can't be mapped, it is invalid.
    /// </summary>
    /// <param name="strategy">Reads, keys and maps the object's events.</param>
    /// <param name="rows">The claimed rows, one per entity, each with its entity's watermark.</param>
    public static PreparedBatch Prepare<TEvent>(UpsertStrategy<TEvent> strategy, IReadOnlyList<InboxEvent> rows)
    {
        var batch = new PreparedBatch([], [], []);

        foreach (var row in rows)
        {
            var entityId = row.EntityId;
            if (!strategy.TryRead(row.Payload, out var read, out var problem))
            {
                batch.Invalid.Add((row.RowId, $"{strategy.EntityType} {entityId}: {problem}"));
                continue;
            }
            if (!string.Equals(read.EntityId, entityId, StringComparison.Ordinal))
            {
                // The lease is on the row's entity; writing another entity under it would bypass that entity's own
                batch.Invalid.Add((row.RowId, $"{strategy.EntityType} {entityId}: the event is about '{read.EntityId}', but the row's entity_id is '{entityId}'"));
                continue;
            }

            if (row.SyncedTime is { } syncedTime && ToMicroseconds(read.Time) <= syncedTime)
            {
                batch.SupersededRowIds.Add(row.RowId);
                continue;
            }

            try
            {
                batch.Work.Add(new Work(entityId, strategy.Map(entityId, read.Event), row.RowId));
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                batch.Invalid.Add((row.RowId, $"{strategy.EntityType} {entityId}: {ex.Message}"));
            }
        }
        return batch;
    }

    /// <summary>An event time as the watermark stores it: microseconds since the Unix epoch.</summary>
    public static long ToMicroseconds(DateTimeOffset time)
    {
        return (time - DateTimeOffset.UnixEpoch).Ticks / TimeSpan.TicksPerMicrosecond;
    }

    /// <summary>
    /// An event time cut to whole microseconds the way <see cref="ToMicroseconds"/> cuts it, in UTC: how the inbox stores
    /// it (event_time). Postgres keeps microseconds and would round finer input, so a time with sub-microsecond ticks
    /// (meta.time may have up to 9 fraction digits) could be stored 1 µs after the time the engine compares with the
    /// watermark; an event as new as the watermark is superseded.
    /// </summary>
    public static DateTimeOffset ToWholeMicroseconds(DateTimeOffset time)
    {
        return DateTimeOffset.UnixEpoch.AddTicks(ToMicroseconds(time) * TimeSpan.TicksPerMicrosecond);
    }

    /// <summary>The units to write, keyed by entity id, as the Salesforce step takes them.</summary>
    public List<(string Key, UpsertUnit Unit)> Units => [.. Work.Select(w => (w.EntityId, w.Unit))];

    /// <summary>
    /// What to record in the inbox once Salesforce has answered: saved or unchanged entities are done, entities not
    /// attempted are released, permanent failures and invalid rows are dead-lettered, the rest is retried.
    /// </summary>
    /// <param name="results">The outcome of every entity in <see cref="Work"/>, by entity id.</param>
    public BatchOutcomes Outcomes(IReadOnlyDictionary<string, UpsertResult> results)
    {
        var synced = new List<long>();
        var released = new List<long>();
        var dead = Invalid.Select(i => new FailedEvent(i.RowId, i.Error, SalesforceErrors.InvalidEvent)).ToList();
        var failed = new List<FailedEvent>();

        foreach (var work in Work)
        {
            var result = results[work.EntityId];
            switch (result.Outcome)
            {
                case SyncOutcome.Saved or SyncOutcome.Unchanged: synced.Add(work.RowId); break;
                case SyncOutcome.NotAttempted: released.Add(work.RowId); break;
                case SyncOutcome.PermanentFailure: dead.Add(Failure(work, result)); break;
                default: failed.Add(Failure(work, result)); break;
            }
        }
        return new BatchOutcomes(synced, SupersededRowIds, released, dead, failed);
    }

    private static FailedEvent Failure(Work work, UpsertResult result)
    {
        return new(work.RowId, string.Join("; ", result.Errors), result.PrimaryError?.Code);
    }
}