using SalesforceSync.Infrastructure.Salesforce;
using static SalesforceSync.Sync.Normalizer;
using static SalesforceSync.Sync.Reader;

namespace SalesforceSync.Sync;

/// <summary>
/// Step 4. Decides what each record needs by comparing it with what Salesforce has: a create, an update of the fields that
/// differ, a delete or nothing. No I/O.
/// </summary>
internal static class Differ
{
    /// <summary>One record to send, as decided by the diff: a create, an update (only the fields that differ) or a delete.</summary>
    /// <param name="UnitKey">The entity the record belongs to.</param>
    /// <param name="Record">The record, normalized against Describe.</param>
    /// <param name="Id">Its Salesforce Id when it exists there (a delete needs it), null when it is new.</param>
    /// <param name="Fields">What to send, by API name: all of a new record's non-empty fields, only the changed ones of an existing one.</param>
    internal sealed record Change(string UnitKey, NormalizedRecord Record, string? Id, IReadOnlyDictionary<string, object?> Fields)
    {
        public bool IsDelete => Record.Record.Delete;
        public bool IsCreate => !IsDelete && Id is null;
        public string SObject => Record.Schema.Name;
        public string ExternalIdField => Record.Schema.Find(Record.Record.ExternalIdField)!.Name;
        public string ReferenceId => Record.ReferenceId;

        /// <summary>The record as an upsert takes it: its type, its external id (which decides create or update) and the fields to send.</summary>
        public SObjectBody Body()
        {
            var body = new SObjectBody(SObject) { [ExternalIdField] = Record.Record.ExternalId };
            foreach (var (field, value) in Fields)
            {
                body[field] = value;
            }
            return body;
        }
    }

    /// <summary>An entity that has something to send.</summary>
    internal sealed record PlannedUnit(string Key, IReadOnlyList<Change> Changes);

    /// <summary>
    /// What each record needs, by comparing it with what Salesforce has (what the read found):
    /// <list type="bullet">
    /// <item>not in Salesforce: a create, with all its non-empty fields;</item>
    /// <item>in Salesforce, some field differs: an update, with only the fields that differ (<see cref="Diff"/>);</item>
    /// <item>in Salesforce, nothing differs: nothing to send;</item>
    /// <item>to delete and in Salesforce: a delete by the Id the read found; to delete and not there: nothing to send.</item>
    /// </list>
    /// Then each entity (unit) is one of: rejected (a field can't be written: permanent), waiting (a record it points at
    /// is neither in Salesforce nor in the unit: transient), unchanged (nothing to send) or planned. No I/O.
    /// </summary>
    /// <returns>The entities with something to send, and the results of those that need no call.</returns>
    internal static (List<PlannedUnit> Planned, List<SyncResult> Decided) Plan(IReadOnlyList<NormalizedUnit> units, CurrentRecords current)
    {
        var planned = new List<PlannedUnit>();
        var decided = new List<SyncResult>();

        foreach (var unit in units)
        {
            var problems = new List<SalesforceError>(); // what Salesforce would reject: permanent
            var missing = new List<SalesforceError>();  // a parent that isn't there yet: transient
            var changes = ChangesOf(unit, current, problems, missing);

            if (problems.Count == 0 && missing.Count == 0 && changes.Count > 0)
            {
                planned.Add(new PlannedUnit(unit.Key, changes));
            }
            else
            {
                decided.Add(problems.Count > 0 ? new(unit.Key, SyncOutcome.PermanentFailure, problems)
                    : missing.Count > 0 ? new(unit.Key, SyncOutcome.TransientFailure, missing)
                    : new(unit.Key, SyncOutcome.Unchanged, []));
            }
        }

        return (planned, decided);
    }

    private static List<Change> ChangesOf(NormalizedUnit unit, CurrentRecords current, List<SalesforceError> problems,
        List<SalesforceError> missing)
    {
        var changes = new List<Change>();
        var inUnit = unit.Records.Where(r => !r.Record.Delete).Select(r => (r.Record.SObject.ToLowerInvariant(), r.Record.ExternalId)).ToHashSet();

        foreach (var record in unit.Records)
        {
            var existing = current.Find(record.Record.SObject, record.Record.ExternalIdField, record.Record.ExternalId);

            if (record.Record.Delete)
            {
                // By the Id the read found. A record the read didn't find is deleted already (or never was): nothing to send.
                if (existing is not null)
                {
                    changes.Add(new Change(unit.Key, record, existing.Id, new Dictionary<string, object?>()));
                }
                continue;
            }

            // A record that points at a parent needs it to exist: in Salesforce already, or created by this same unit
            foreach (var parent in record.Record.Parents)
            {
                if (!inUnit.Contains((parent.SObject.ToLowerInvariant(), parent.ExternalId)) && current.Find(parent.SObject, parent.ExternalIdField, parent.ExternalId) is null)
                {
                    missing.Add(new(record.ReferenceId, SalesforceErrors.DependencyNotSynced,
                        $"{record.Schema.Name}.{parent.Field}: {parent.SObject} {parent.ExternalIdField}={parent.ExternalId} is not in Salesforce yet"));
                }
            }

            var fields = Diff(record, existing, problems);

            // A new record is always written; an existing one only when a field differs
            if (existing is null || fields.Count > 0)
            {
                changes.Add(new Change(unit.Key, record, existing?.Id, fields));
            }
        }
        return changes;
    }

    /// <summary>
    /// The fields to send: every non-empty one for a new record, otherwise only those whose normalized value differs from
    /// Salesforce's. A field Salesforce won't let us set (not createable / updateable) is a problem only if it must
    /// actually be written.
    /// </summary>
    /// <param name="record">The desired record, normalized.</param>
    /// <param name="existing">The record in Salesforce, or null if it is new.</param>
    /// <param name="problems">Receives the fields that must be written but can't be.</param>
    internal static Dictionary<string, object?> Diff(NormalizedRecord record, CurrentRecord? existing, List<SalesforceError> problems)
    {
        var fields = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
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

            var creating = existing is null;
            if (!(creating ? field.Createable : field.Updateable))
            {
                problems.Add(new SalesforceError(record.ReferenceId, "INVALID_FIELD_FOR_INSERT_UPDATE",
                    $"{record.Schema.Name}.{field.Name} is not {(creating ? "createable" : "updateable")}"));
                continue;
            }

            fields[field.Name] = FieldValues.ToWire(value);
        }
        return fields;
    }
}
