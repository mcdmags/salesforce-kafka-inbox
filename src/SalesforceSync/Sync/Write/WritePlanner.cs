using SalesforceSync.Infrastructure.Salesforce;

namespace SalesforceSync.Sync.Write;

/// <summary>What to do with a batch after the read.</summary>
/// <param name="Requests">The write requests, each one API call (see <see cref="WriteRequest"/>).</param>
/// <param name="NotSent">The units that won't be sent, with their outcome: unchanged, rejected, too large, or waiting for a parent.</param>
internal sealed record WritePlan(IReadOnlyList<WriteRequest> Requests, IReadOnlyList<SyncResult> NotSent);

/// <summary>
/// Step 4 of a batch: decides what each record needs by comparing it with what Salesforce has (what the read found).
/// No I/O. Per record:
/// <list type="bullet">
/// <item>not in Salesforce: create it, with all its non-empty fields;</item>
/// <item>in Salesforce, some field differs: update it, with only the fields that differ (<see cref="Changes"/>);</item>
/// <item>in Salesforce, nothing differs: send nothing;</item>
/// <item>to delete and in Salesforce: delete it by the Id the read found; to delete and not there: send nothing.</item>
/// </list>
/// Per unit (one entity's records), the result is one of: rejected (a field can't be written: permanent), waiting (a
/// parent is in neither Salesforce nor the unit: transient), unchanged, or planned (its writes go to
/// <see cref="WriteRequest.Pack"/>).
/// </summary>
internal static class WritePlanner
{
    /// <param name="units">The batch's units, normalized against Describe.</param>
    /// <param name="current">What the read found.</param>
    /// <param name="maxRecordsPerWrite">The row type's <see cref="Data.Inbox.IInboxRow.MaxRecordsPerWrite"/>: records per collection.</param>
    public static WritePlan Plan(IReadOnlyList<NormalizedUnit> units, CurrentRecords current, int maxRecordsPerWrite)
    {
        var planned = new List<PlannedUnit>();
        var decided = new List<SyncResult>();

        foreach (var unit in units)
        {
            var problems = new List<SalesforceError>(); // what Salesforce would reject: permanent
            var missing = new List<SalesforceError>(); // a parent that isn't there yet: transient
            var writes = PlanUnit(unit, current, problems, missing);

            if (problems.Count > 0)
            {
                decided.Add(new(unit.Key, SyncOutcome.PermanentFailure, problems));
            }
            else if (missing.Count > 0)
            {
                decided.Add(new(unit.Key, SyncOutcome.TransientFailure, missing));
            }
            else if (writes.Count == 0)
            {
                decided.Add(new(unit.Key, SyncOutcome.Unchanged, []));
            }
            else
            {
                planned.Add(new PlannedUnit(unit.Key, writes));
            }
        }

        var (requests, tooLarge) = WriteRequest.Pack(planned, maxRecordsPerWrite);
        decided.AddRange(tooLarge);
        return new WritePlan(requests, decided);
    }

    /// <summary>What an earlier record of a unit is to the records after it, which may point at it.</summary>
    /// <param name="Level">The record's level in the unit (see <see cref="CollectionKey.Level"/>).</param>
    /// <param name="Create">Its write when it is new (a child's outcome follows it), else null.</param>
    private readonly record struct EarlierRecord(int Level, PlannedWrite? Create);

    /// <summary>
    /// The unit's writes, in record order. A record whose foreign key points at another record of the unit goes one
    /// level below it, so it is sent after it; a parent that is in Salesforce already puts no constraint on it.
    /// </summary>
    /// <param name="unit">The unit, normalized.</param>
    /// <param name="current">What the read found in Salesforce.</param>
    /// <param name="problems">Receives what Salesforce would reject (a field that can't be written).</param>
    /// <param name="missing">Receives the foreign keys whose parent is neither in Salesforce nor in the unit.</param>
    private static List<PlannedWrite> PlanUnit(NormalizedUnit unit, CurrentRecords current, List<SalesforceError> problems,
        List<SalesforceError> missing)
    {
        var writes = new List<PlannedWrite>();
        var earlier = new Dictionary<(string SObject, string ExternalId), EarlierRecord>();

        foreach (var record in unit.Records)
        {
            var existing = current.Find(record.Record);

            if (record.Record.Delete)
            {
                // By the Id the read found. A record the read didn't find is deleted already (or never was): nothing to send.
                if (existing is not null)
                {
                    writes.Add(new PlannedWrite(record, existing.Id, new Dictionary<string, object?>(), [], level: 0));
                }

                continue;
            }

            var changes = Changes(record, existing, problems);

            var level = 0;
            var newParents = new List<PlannedWrite>();
            foreach (var key in record.Record.ForeignKeys ?? [])
            {
                if (earlier.TryGetValue((key.SObject.ToLowerInvariant(), key.ExternalId), out var sibling))
                {
                    level = Math.Max(level, sibling.Level + 1);
                    if (sibling.Create is not null)
                    {
                        newParents.Add(sibling.Create);
                    }
                }
                else if (current.Find(key) is null)
                {
                    missing.Add(new(record.ReferenceId, SalesforceErrors.DependencyNotSynced,
                        $"{record.Schema.Name}.{key.Field}: {key.SObject} {key.ExternalIdField}={key.ExternalId} is not in Salesforce yet"));
                }
            }

            // A new record is always written; an existing one only when a field differs
            var write = existing is not null && changes.Count == 0
                ? null
                : new PlannedWrite(record, existing?.Id, changes, newParents, level);
            if (write is not null)
            {
                writes.Add(write);
            }

            earlier[(record.Record.SObject.ToLowerInvariant(), record.Record.ExternalId)] =
                new EarlierRecord(level, existing is null ? write : null);
        }
        return writes;
    }

    /// <summary>
    /// The diff. The fields to send: every non-empty one for a new record, otherwise only those whose normalized value
    /// differs from Salesforce's. A field Salesforce won't let us set (not createable / updateable) is a problem only
    /// if it must actually be written.
    /// </summary>
    /// <param name="existing">The record in Salesforce, or null if it is new.</param>
    /// <param name="record">The desired record, normalized.</param>
    /// <param name="problems">Receives the fields that must be written but can't be.</param>
    public static Dictionary<string, object?> Changes(NormalizedRecord record, CurrentRecord? existing, List<SalesforceError> problems)
    {
        var changes = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (var (field, value) in record.Fields)
        {
            // A new record has no value to clear, and an explicit null would override the field's default
            if (existing is null && value is null)
            {
                continue;
            }

            if (existing is not null && existing.TryGetValue(field.Name, out var stored) && FieldValues.AreEqual(field, value, stored))
            {
                continue;
            }

            if (NotWritable(record, field, existing) is { } problem)
            {
                problems.Add(problem);
                continue;
            }

            changes[field.Name] = FieldValues.ToWire(value);
        }
        return changes;
    }

    /// <summary>The problem with writing <paramref name="field"/>, if Salesforce doesn't allow it on a create / an update.</summary>
    private static SalesforceError? NotWritable(NormalizedRecord record, SalesforceField field, CurrentRecord? existing)
    {
        var creating = existing is null;
        return (creating ? field.Createable : field.Updateable)
            ? null
            : new SalesforceError(record.ReferenceId, "INVALID_FIELD_FOR_INSERT_UPDATE",
                $"{record.Schema.Name}.{field.Name} is not {(creating ? "createable" : "updateable")}");
    }
}
