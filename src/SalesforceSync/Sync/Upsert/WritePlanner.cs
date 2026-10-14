using System.Text.Json;
using SalesforceSync.Salesforce;

namespace SalesforceSync.Sync.Upsert;

/// <summary>What to do with a batch after the read.</summary>
/// <param name="Requests">The Composite requests to send, each with its units and their writes by collection.</param>
/// <param name="NotSent">The units that won't be sent, with their outcome: unchanged, rejected, too large, or waiting for a lookup target.</param>
internal sealed record WritePlan(IReadOnlyList<CompositeWrite> Requests, IReadOnlyList<UpsertResult> NotSent);

/// <summary>
/// Turns units and Salesforce's current records into the collection requests to send. Whether a record is created or
/// updated is decided by what the read found. A lookup becomes a literal Id, or a reference to a create in the same
/// request (<c>@{Account_create[3].id}</c>). No I/O.
/// </summary>
internal static class WritePlanner
{
    public static WritePlan Plan(IReadOnlyList<NormalizedUnit> units, CurrentRecords current)
    {
        var planned = new List<PlannedUnit>();
        var decided = new List<UpsertResult>();

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

        var (requests, tooLarge) = CompositeWrite.Pack(planned);
        decided.AddRange(tooLarge);
        return new WritePlan(requests, decided);
    }

    /// <summary>
    /// The unit's writes: a create for each record Salesforce doesn't have, an update for each one that differs, nothing
    /// for the rest. A lookup becomes the target's Id when the target exists, and a link to the target's create when
    /// the target is an earlier record of this unit that is new.
    /// </summary>
    /// <param name="problems">Receives what Salesforce would reject (a field that can't be written).</param>
    /// <param name="missing">Receives the lookups whose target is neither in Salesforce nor in the unit.</param>
    private static List<PlannedWrite> PlanUnit(NormalizedUnit unit, CurrentRecords current, List<SalesforceError> problems,
        List<SalesforceError> missing)
    {
        var writes = new List<PlannedWrite>();
        // The unit's records so far, by (sobject, external id), for the later records that look them up
        var earlier = new Dictionary<(string SObject, string ExternalId), LookupTarget>();

        foreach (var record in unit.Records)
        {
            var existing = current.Find(record.Record);
            var changes = RecordDiffer.Changes(record, existing, problems);
            var links = new List<NewParentLink>();
            var depth = 0;

            foreach (var lookup in record.Lookups)
            {
                if (FindTarget(lookup.Target, current, earlier) is not { } target)
                {
                    missing.Add(new(record.ReferenceId, SalesforceErrors.DependencyNotSynced,
                        $"{record.Schema.Name}.{lookup.Field.Name}: {lookup.Target.SObject} {lookup.Target.ExternalIdField}={lookup.Target.ExternalId} is not in Salesforce yet"));
                    continue;
                }
                depth = Math.Max(depth, target.Depth + 1);

                if (target.Create is null && LinkedId(existing, lookup.Field) == target.Id)
                {
                    continue; // already points at it
                }

                if (RecordDiffer.NotWritable(record, lookup.Field, existing) is { } problem)
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
                    links.Add(new NewParentLink(lookup.Field, target.Create));
                }
            }

            var write = Write(record, existing, changes, links, depth);
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
    /// What a lookup points at: a record with an Id, or the create of an earlier record of the same unit.
    /// <paramref name="Depth"/> is that record's level in the unit (-1 for a record outside it), which puts the
    /// record looking it up one level below.
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

    /// <summary>A create for a new record (with its external id), an update if anything differs, else null.</summary>
    private static PlannedWrite? Write(NormalizedRecord record, CurrentRecord? existing, Dictionary<string, object?> changes,
        List<NewParentLink> links, int depth)
    {
        if (existing is not null && changes.Count == 0 && links.Count == 0)
        {
            return null;
        }

        var lookupFields = record.Lookups.Where(l => changes.ContainsKey(l.Field.Name)).Select(l => l.Field.Name)
            .Concat(links.Select(l => l.Field.Name))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        if (existing is null)
        {
            changes[record.Schema.Find(record.Record.ExternalIdField)!.Name] = record.Record.ExternalId;
        }

        return new PlannedWrite(record, existing?.Id, changes, links, lookupFields, depth);
    }
}