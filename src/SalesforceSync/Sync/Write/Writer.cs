using SalesforceSync.Infrastructure.Salesforce;

namespace SalesforceSync.Sync.Write;

/// <summary>
/// Step 5 of a batch: sends the planned changes, one call per object, and reads the answers into one outcome per entity.
/// <list type="number">
/// <item>Upserts, object by object, parents first: an object comes after every object its records point at, because
/// the org rejects a child whose parent doesn't exist yet. One call per object (<see cref="ISalesforceRestClient.UpsertAsync"/>), or one per
/// 200 records of it.</item>
/// <item>Then the deletes, in one call (<see cref="ISalesforceRestClient.DeleteAsync"/>), after every upsert: a child an upsert moved to another
/// parent must not be taken down by the delete of its old parent.</item>
/// </list>
/// An entity that failed in one call is not sent in the next ones: its outcome is already decided, and what it still has
/// to send would only fail for the same reason. Records it saved stay saved, and the retry reads and diffs them.
/// </summary>
internal sealed class Writer(ISalesforceRestClient salesforce, WriteResultInterpreter interpreter)
{
    public async Task<List<SyncResult>> WriteAsync(IReadOnlyList<PlannedUnit> units, int maxRecordsPerWrite, CancellationToken ct)
    {
        var all = units.SelectMany(u => u.Changes).ToList();
        var progress = units.ToDictionary(u => u.Key, _ => new UnitProgress());

        foreach (var sobject in ParentsFirst(all.Where(c => !c.IsDelete)))
        {
            var upserts = all.Where(c => !c.IsDelete && c.SObject == sobject && !progress[c.UnitKey].Failed).ToList();
            foreach (var chunk in upserts.Chunk(maxRecordsPerWrite))
            {
                var results = await salesforce.UpsertAsync(sobject, chunk[0].ExternalIdField, [.. chunk.Select(c => c.Body())], ct);
                Record(chunk, results, progress);
            }
        }

        var deletes = all.Where(c => c.IsDelete && !progress[c.UnitKey].Failed).ToList();
        foreach (var chunk in deletes.Chunk(maxRecordsPerWrite))
        {
            var results = await salesforce.DeleteAsync([.. chunk.Select(c => c.Id!)], ct);
            Record(chunk, results, progress);
        }

        return [.. units.Select(u => progress[u.Key].Result(u.Key))];
    }

    /// <summary>What each record's answer means for its entity.</summary>
    private void Record(Change[] sent, IReadOnlyList<RecordResult> results, Dictionary<string, UnitProgress> progress)
    {
        for (var i = 0; i < sent.Length; i++)
        {
            // An answer is expected for every record, in order; a missing one is a failure that says nothing about the data
            var answer = results.Count == sent.Length ? results[i] : null;
            var (status, errors) = interpreter.Interpret(sent[i], answer);
            progress[sent[i].UnitKey].Add(status, errors);
        }
    }

    /// <summary>
    /// The objects with changes, each after the objects its records point at (a foreign key). An object whose parent has no
    /// changes of its own goes first: that parent is in Salesforce already.
    /// </summary>
    private static List<string> ParentsFirst(IEnumerable<Change> upserts)
    {
        var parentsOf = upserts.GroupBy(c => c.SObject).ToDictionary(g => g.Key,
            g => g.SelectMany(c => c.Record.Record.ForeignKeys ?? []).Select(k => k.SObject)
                .Where(p => !p.Equals(g.Key, StringComparison.OrdinalIgnoreCase)).ToHashSet(StringComparer.OrdinalIgnoreCase));

        var ordered = new List<string>();
        while (ordered.Count < parentsOf.Count)
        {
            var next = parentsOf.Keys.Where(o => !ordered.Contains(o)
                                                 && parentsOf[o].All(p => ordered.Contains(p) || !parentsOf.ContainsKey(p)));
            ordered.Add(next.FirstOrDefault() ?? throw new InvalidOperationException(
                $"Objects point at each other: {string.Join(", ", parentsOf.Keys.Except(ordered))}"));
        }
        return ordered;
    }

    /// <summary>How one entity's records fared, as the calls answer.</summary>
    private sealed class UnitProgress
    {
        private readonly List<(WriteStatus Status, SalesforceError Error)> _errors = [];
        private WriteStatus _status = WriteStatus.Saved;

        public bool Failed => _status != WriteStatus.Saved;

        public void Add(WriteStatus status, List<SalesforceError> errors)
        {
            _errors.AddRange(errors.Select(e => (status, e)));
            _status = (WriteStatus)Math.Max((int)_status, (int)status);
        }

        /// <summary>One outcome from the worst record's; the errors that decided it come first.</summary>
        public SyncResult Result(string key)
        {
            var errors = _errors.OrderByDescending(e => e.Status).Select(e => e.Error).ToList();
            return _status switch
            {
                WriteStatus.Saved => new(key, SyncOutcome.Saved, []),
                WriteStatus.Permanent => new(key, SyncOutcome.PermanentFailure, errors),
                _ => new(key, SyncOutcome.TransientFailure, errors)
            };
        }
    }
}
