using System.Text.Json;
using SalesforceSync.Infrastructure.Salesforce;

namespace SalesforceSync.Sync.Write;

/// <summary>What to do with a batch after the read.</summary>
/// <param name="Requests">
/// The write requests, each with its units and their writes by collection (one upsert each, and one delete), each one
/// API call: one Collections upsert or delete, or one Composite request of several (<see cref="CompositeWrite"/>).
/// </param>
/// <param name="NotSent">The units that won't be sent, with their outcome: unchanged, rejected, too large, or waiting for a lookup target.</param>
internal sealed record WritePlan(IReadOnlyList<CompositeWrite> Requests, IReadOnlyList<SyncResult> NotSent);

/// <summary>
/// Turns units and Salesforce's current records into the collection requests to send. Whether a record is created or
/// updated is decided by what the read found, and so is whether a record to delete is sent (by its Id) at all. A lookup becomes a literal Id, or a reference to a new record's upsert in
/// the same request (<c>@{Account[3].id}</c>); a foreign key stays the text it is, and only puts its record after its
/// parent. Compares a record's normalized values with what Salesforce has (<see cref="Changes"/>). No I/O.
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
            var problems = new List<SalesforceError>();
            var missing = new List<SalesforceError>();
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

        var (requests, tooLarge) = CompositeWrite.Pack(planned, maxRecordsPerWrite);
        decided.AddRange(tooLarge);
        return new WritePlan(requests, decided);
    }

    /// <summary>
    /// The unit's writes: a create for each record Salesforce doesn't have, an update for each one that differs, a delete
    /// for each record to delete that Salesforce has, nothing for the rest. A lookup becomes the target's Id when the target exists, and a link to the target's create when
    /// the target is an earlier record of this unit that is new. Lookups and foreign keys both put a record one level
    /// below the records of the unit they point at (so it is sent after them), and make a new one of those its parent.
    /// </summary>
    /// <param name="problems">Receives what Salesforce would reject (a field that can't be written).</param>
    /// <param name="missing">Receives the lookups and foreign keys whose target is neither in Salesforce nor in the unit.</param>
    /// <param name="unit">The unit, normalized.</param>
    /// <param name="current">What the read found in Salesforce.</param>
    private static List<PlannedWrite> PlanUnit(NormalizedUnit unit, CurrentRecords current, List<SalesforceError> problems,
        List<SalesforceError> missing)
    {
        var writes = new List<PlannedWrite>();
        // The unit's records so far, by (sobject, external id), for the later records that look them up
        var earlier = new Dictionary<(string SObject, string ExternalId), LookupTarget>();

        foreach (var record in unit.Records)
        {
            var existing = current.Find(record.Record);
            if (record.Record.Delete)
            {
                // By the Id the read found; a record the read didn't find is deleted already (or never was), so
                // nothing is sent for it
                if (existing is not null)
                {
                    writes.Add(new PlannedWrite(record, existing.Id, new Dictionary<string, object?>(), [], [],
                        new HashSet<string>(), depth: 0));
                }

                continue;
            }

            var changes = Changes(record, existing, problems);
            var links = new List<ParentReference>();
            var parents = new List<PlannedWrite>();
            var depth = 0;

            // The record the lookup or foreign key points at, which it follows; null (and missing) if there is none
            LookupTarget? Parent(SObjectLookup pointer)
            {
                if (FindTarget(pointer, current, earlier) is not { } target)
                {
                    missing.Add(new(record.ReferenceId, SalesforceErrors.DependencyNotSynced,
                        $"{record.Schema.Name}.{pointer.Field}: {pointer.SObject} {pointer.ExternalIdField}={pointer.ExternalId} " +
                        "is not in Salesforce yet"));
                    return null;
                }

                depth = Math.Max(depth, target.Depth + 1);
                if (target.Create is not null)
                {
                    parents.Add(target.Create);
                }

                return target;
            }

            // A foreign key is written like any field (in changes already): it only orders the record after its parent
            foreach (var key in record.Record.ForeignKeys ?? [])
            {
                Parent(key);
            }

            foreach (var lookup in record.Lookups)
            {
                if (Parent(lookup.Target) is not { } target)
                {
                    continue;
                }

                if (target.Create is null && LinkedId(existing, lookup.Field) == target.Id)
                {
                    continue; // already points at it
                }

                if (NotWritable(record, lookup.Field, existing) is { } problem)
                {
                    problems.Add(problem);
                    continue;
                }

                if (target.Create is null)
                {
                    changes[lookup.Field.Name] = target.Id;
                }
                else
                {
                    links.Add(new ParentReference(lookup.Field, target.Create));
                }
            }

            var write = Write(record, existing, changes, links, parents, depth);
            if (write is not null)
            {
                writes.Add(write);
            }

            earlier[(record.Record.SObject.ToLowerInvariant(), record.Record.ExternalId)] = existing is null
                ? new LookupTarget(null, write, depth)
                : new LookupTarget(existing.Id18, null, depth);
        }
        return writes;
    }

    /// <summary>
    /// What a lookup or foreign key points at: a record with an Id, or the create of an earlier record of the same
    /// unit. <paramref name="Depth"/> is that record's level in the unit (-1 for a record outside it), which puts the
    /// record pointing at it one level below.
    /// </summary>
    private readonly record struct LookupTarget(string? Id, PlannedWrite? Create, int Depth);

    private static LookupTarget? FindTarget(SObjectLookup lookup, CurrentRecords current,
        Dictionary<(string SObject, string ExternalId), LookupTarget> earlier)
    {
        return earlier.TryGetValue((lookup.SObject.ToLowerInvariant(), lookup.ExternalId), out var sibling)
            ? sibling
            : current.Find(lookup) is { } inSalesforce ? new LookupTarget(inSalesforce.Id18, null, -1) : (LookupTarget?)null;
    }

    /// <summary>The Id the lookup field holds in Salesforce now, in its 18-character form.</summary>
    private static string? LinkedId(CurrentRecord? existing, SalesforceField field)
    {
        return existing is not null && existing.TryGetValue(field.Name, out var linked) && linked.ValueKind == JsonValueKind.String
                ? FieldValues.To18CharId(linked.GetString()!)
                : null;
    }

    /// <summary>A create for a new record, an update if anything differs, else null.</summary>
    private static PlannedWrite? Write(NormalizedRecord record, CurrentRecord? existing, Dictionary<string, object?> changes,
        List<ParentReference> links, List<PlannedWrite> parents, int depth)
    {
        if (existing is not null && changes.Count == 0 && links.Count == 0)
        {
            return null;
        }

        var lookupFields = record.Lookups.Where(l => changes.ContainsKey(l.Field.Name)).Select(l => l.Field.Name)
            .Concat(links.Select(l => l.Field.Name))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        return new PlannedWrite(record, existing?.Id, changes, links, parents, lookupFields, depth);
    }

    /// <summary>
    /// Fields to send: every non-empty one for a new record, otherwise only those whose normalized value differs from
    /// Salesforce. A field Salesforce won't let us set (not createable / updateable) is a problem only if it must
    /// actually be written.
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