using SalesforceSync.Inbox;
using SalesforceSync.Salesforce;

namespace SalesforceSync.Sync;

/// <summary>An entity to write: the unit mapped from its newest event (row <paramref name="EventId"/>) and that event's time.</summary>
internal sealed record Work(string EntityId, UpsertUnit Unit, long Time, long EventId);

/// <summary>What to do with a batch's rows: superseded ones, entities to write, and rows that are invalid on their own.</summary>
internal sealed record PreparedBatch(List<long> SupersededIds, List<Work> Work, List<(long Id, string Error)> Invalid)
{
    /// <summary>The units to write, keyed by entity id, as the Salesforce step takes them.</summary>
    public List<(string Key, UpsertUnit Unit)> Units => [.. Work.Select(w => (w.EntityId, w.Unit))];

    /// <summary>
    /// What to record in the inbox once Salesforce has answered: saved or unchanged entities are done, entities not
    /// attempted stay pending as they are, permanent failures and invalid rows are dead-lettered, the rest is retried.
    /// </summary>
    /// <param name="results">The outcome of every entity in <see cref="Work"/>, by entity id.</param>
    public BatchOutcomes Outcomes(IReadOnlyDictionary<string, UpsertResult> results)
    {
        var synced = new List<SyncedEntity>();
        var dead = Invalid.Select(i => new FailedEvent(i.Id, i.Error, SalesforceErrors.InvalidEvent)).ToList();
        var failed = new List<FailedEvent>();

        foreach (var work in Work)
        {
            var result = results[work.EntityId];
            switch (result.Outcome)
            {
                case SyncOutcome.Saved or SyncOutcome.Unchanged: synced.Add(new SyncedEntity(work.EntityId, work.Time, work.EventId)); break;
                case SyncOutcome.NotAttempted: break;
                case SyncOutcome.PermanentFailure: dead.Add(Failure(work, result)); break;
                default: failed.Add(Failure(work, result)); break;
            }
        }
        return new BatchOutcomes(synced, SupersededIds, dead, failed);
    }

    private static FailedEvent Failure(Work work, UpsertResult result)
    {
        return new(work.EventId, string.Join("; ", result.Errors), result.PrimaryError?.Code);
    }
}