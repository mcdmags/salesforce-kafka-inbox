using SalesforceSync.Infrastructure.Salesforce;
using static SalesforceSync.Sync.Differ;

namespace SalesforceSync.Sync;

/// <summary>
/// Step 5. Sends the planned changes, one call per object and one for the deletes, and reads the answers into one outcome
/// per entity.
/// </summary>
internal sealed class Sender(ISalesforceRestClient salesforce, Interpreter interpreter)
{
    /// <summary>
    /// Sends the planned changes, one call per object, and reads the answers into one outcome per entity:
    /// <list type="number">
    /// <item>Upserts, object by object, parents first: an object comes after every object its records point at, because
    /// the org rejects a child whose parent doesn't exist yet. One call per object
    /// (<see cref="ISalesforceRestClient.UpsertAsync"/>), or one per 200 records of it.</item>
    /// <item>Then the deletes, in one call (<see cref="ISalesforceRestClient.DeleteAsync"/>), after every upsert: a child
    /// an upsert moved to another parent must not be taken down by the delete of its old parent.</item>
    /// </list>
    /// An entity that failed in one call is not sent in the next ones: its outcome is already decided, and what it still has
    /// to send would only fail for the same reason. Records it saved stay saved, and the retry reads and diffs them.
    /// </summary>
    internal async Task<List<SyncResult>> WriteAsync(IReadOnlyList<PlannedUnit> units, int maxRecordsPerWrite, CancellationToken ct)
    {
        var all = units.SelectMany(u => u.Changes).ToList();
        var worst = units.ToDictionary(u => u.Key, _ => SyncOutcome.Saved);       // an entity's outcome: its worst record's
        var failures = units.ToDictionary(u => u.Key, _ => new List<(SyncOutcome Outcome, SalesforceError Error)>());

        async Task SendAsync(IEnumerable<Change> changes, Func<Change[], Task<IReadOnlyList<RecordResult>>> call)
        {
            foreach (var chunk in changes.Where(c => worst[c.UnitKey] == SyncOutcome.Saved).ToList().Chunk(maxRecordsPerWrite))
            {
                var answers = await call(chunk);
                for (var i = 0; i < chunk.Length; i++)
                {
                    // An answer is expected for every record, in order; a missing one is a failure that says nothing about the data
                    var (outcome, recordErrors) = interpreter.Interpret(chunk[i], answers.Count == chunk.Length ? answers[i] : null);
                    failures[chunk[i].UnitKey].AddRange(recordErrors.Select(e => (outcome, e)));
                    worst[chunk[i].UnitKey] = (SyncOutcome)Math.Max((int)worst[chunk[i].UnitKey], (int)outcome);
                }
            }
        }

        foreach (var sobject in ParentsFirst(all.Where(c => !c.IsDelete)))
        {
            await SendAsync(all.Where(c => !c.IsDelete && c.SObject == sobject),
                chunk => salesforce.UpsertAsync(sobject, chunk[0].ExternalIdField, [.. chunk.Select(c => c.Body())], ct));
        }

        await SendAsync(all.Where(c => c.IsDelete), chunk => salesforce.DeleteAsync([.. chunk.Select(c => c.Id!)], ct));

        return [.. units.Select(u => new SyncResult(u.Key, worst[u.Key], worst[u.Key] == SyncOutcome.Saved ? []
            : [.. failures[u.Key].OrderByDescending(f => f.Outcome).Select(f => f.Error)]))];
    }

    /// <summary>
    /// The objects with changes, each after the objects its records point at (a foreign key). An object whose parent has
    /// no changes of its own goes first: that parent is in Salesforce already.
    /// </summary>
    private static List<string> ParentsFirst(IEnumerable<Change> upserts)
    {
        var parentsOf = upserts.GroupBy(c => c.SObject, StringComparer.OrdinalIgnoreCase).ToDictionary(g => g.Key,
            g => g.SelectMany(c => c.Record.Record.Parents).Select(k => k.SObject)
                .Where(p => !p.Equals(g.Key, StringComparison.OrdinalIgnoreCase)).ToHashSet(StringComparer.OrdinalIgnoreCase),
            StringComparer.OrdinalIgnoreCase);

        var ordered = new List<string>();
        while (ordered.Count < parentsOf.Count)
        {
            ordered.Add(parentsOf.Keys.FirstOrDefault(o => !ordered.Contains(o)
                                                           && parentsOf[o].All(p => ordered.Contains(p) || !parentsOf.ContainsKey(p)))
                        ?? throw new InvalidOperationException(
                            $"Objects point at each other: {string.Join(", ", parentsOf.Keys.Except(ordered))}"));
        }
        return ordered;
    }
}
