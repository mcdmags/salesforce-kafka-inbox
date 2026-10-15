using SalesforceSync.Inbox;
using SalesforceSync.Salesforce;

namespace SalesforceSync.Sync;

/// <summary>An entity to write: the unit mapped from its newest event (row <paramref name="EventId"/>) and that event's time.</summary>
internal sealed record Work(string EntityId, UpsertUnit Unit, long Time, long EventId);

/// <summary>
/// What to do with a claim's rows: superseded ones, entities to write, and rows that are invalid on their own. Decided
/// (<see cref="Prepare{TEvent}"/>) before Salesforce is involved. No I/O.
/// </summary>
internal sealed record PreparedBatch(List<long> SupersededIds, List<Work> Work, List<(long Id, string Error)> Invalid)
{
    /// <summary>
    /// Decides, per claimed entity, what to do with its rows. Rows that can't be read (or whose event is about another
    /// entity than the row says) are invalid on their own. Of the rest only the newest event counts
    /// (<see cref="Coalesce{TEvent}"/>): the others are superseded, and the newest too if its time is not after the
    /// entity's watermark (a replay, a redelivery, or an older event that arrived late). Otherwise it is mapped to
    /// the unit to write; if it can't be mapped, it is invalid.
    /// </summary>
    /// <param name="synced">Entity id -> the time (microseconds since the epoch) of its newest synced event.</param>
    public static PreparedBatch Prepare<TEvent>(UpsertStrategy<TEvent> strategy, IReadOnlyList<InboxEvent> rows,
        IReadOnlyDictionary<string, long> synced)
    {
        var batch = new PreparedBatch([], [], []);

        foreach (var entity in rows.GroupBy(e => e.EntityId, StringComparer.Ordinal))
        {
            var events = ReadEvents(strategy, entity.Key, entity, batch.Invalid);
            if (events.Count == 0)
            {
                continue;
            }

            var (Row, Read) = Coalesce(events);
            batch.SupersededIds.AddRange(events.Where(e => e.Row.Id != Row.Id).Select(e => e.Row.Id));

            var time = ToMicroseconds(Read.Time);
            if (synced.TryGetValue(entity.Key, out var syncedTime) && time <= syncedTime)
            {
                batch.SupersededIds.Add(Row.Id);
                continue;
            }

            try
            {
                var unit = strategy.Map(entity.Key, Read.Event);
                batch.Work.Add(new Work(entity.Key, unit, time, Row.Id));
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                batch.Invalid.Add((Row.Id, $"{strategy.EntityType} {entity.Key}: {ex.Message}"));
            }
        }
        return batch;
    }

    /// <summary>The entity's rows that are valid events about that entity; the others go to <paramref name="invalid"/>.</summary>
    private static List<(InboxEvent Row, ReadEvent<TEvent> Read)> ReadEvents<TEvent>(UpsertStrategy<TEvent> strategy,
        string entityId, IEnumerable<InboxEvent> rows, List<(long Id, string Error)> invalid)
    {
        var events = new List<(InboxEvent Row, ReadEvent<TEvent> Read)>();
        foreach (var row in rows)
        {
            if (!strategy.TryRead(row.Payload, out var read, out var problem))
            {
                invalid.Add((row.Id, $"{strategy.EntityType} {entityId}: {problem}"));
            }
            else if (!string.Equals(read.EntityId, entityId, StringComparison.Ordinal))
            {
                // The lease is on the row's entity; writing another entity under it would bypass that entity's own
                invalid.Add((row.Id, $"{strategy.EntityType} {entityId}: the event is about '{read.EntityId}', but the row's entity_id is '{entityId}'"));
            }
            else
            {
                events.Add((row, read));
            }
        }
        return events;
    }

    /// <summary>
    /// An entity's claimed events become one write: the event with the newest time. Every event carries the full
    /// record, so the newest one is the whole desired state and the older ones add nothing (merging them in would
    /// keep a field the newest event no longer has). Two events with the same time: the one stored later (the higher
    /// row id) counts as newer.
    /// </summary>
    public static (InboxEvent Row, ReadEvent<TEvent> Read) Coalesce<TEvent>(IReadOnlyList<(InboxEvent Row, ReadEvent<TEvent> Read)> events)
    {
        var newest = events[0];
        for (var i = 1; i < events.Count; i++)
        {
            var e = events[i];
            if (e.Read.Time > newest.Read.Time || (e.Read.Time == newest.Read.Time && e.Row.Id > newest.Row.Id))
            {
                newest = e;
            }
        }
        return newest;
    }

    /// <summary>An event time as the watermark stores it: microseconds since the Unix epoch.</summary>
    public static long ToMicroseconds(DateTimeOffset time)
    {
        return (time - DateTimeOffset.UnixEpoch).Ticks / TimeSpan.TicksPerMicrosecond;
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
        var synced = new List<SyncedEntity>();
        var released = new List<long>();
        var dead = Invalid.Select(i => new FailedEvent(i.Id, i.Error, SalesforceErrors.InvalidEvent)).ToList();
        var failed = new List<FailedEvent>();

        foreach (var work in Work)
        {
            var result = results[work.EntityId];
            switch (result.Outcome)
            {
                case SyncOutcome.Saved or SyncOutcome.Unchanged: synced.Add(new SyncedEntity(work.EntityId, work.Time, work.EventId)); break;
                case SyncOutcome.NotAttempted: released.Add(work.EventId); break;
                case SyncOutcome.PermanentFailure: dead.Add(Failure(work, result)); break;
                default: failed.Add(Failure(work, result)); break;
            }
        }
        return new BatchOutcomes(synced, SupersededIds, released, dead, failed);
    }

    private static FailedEvent Failure(Work work, UpsertResult result)
    {
        return new(work.EventId, string.Join("; ", result.Errors), result.PrimaryError?.Code);
    }
}