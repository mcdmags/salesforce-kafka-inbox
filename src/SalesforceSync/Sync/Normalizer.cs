using SalesforceSync.Infrastructure.Salesforce;

namespace SalesforceSync.Sync;

/// <summary>
/// Step 1. Puts each unit's values in a canonical form (<see cref="FieldValues.Normalize"/>) and its record types as
/// the Ids the org knows them by, before any call that writes. A unit whose record type the org doesn't have is
/// rejected on its own. No other check is made here: the org's metadata is not read, so a field it doesn't have or won't
/// let us set is found by Salesforce itself (the read, or the write's answer for the record).
/// </summary>
internal sealed class Normalizer(RecordTypeCache recordTypes)
{
    /// <summary>A unit whose values are in canonical form, ready to be compared and sent.</summary>
    internal sealed record NormalizedUnit(string Key, IReadOnlyList<NormalizedRecord> Records);

    /// <summary>A record with its values normalized, by API name (and its RecordTypeId among them).</summary>
    internal sealed record NormalizedRecord(SObjectRecord Record, IReadOnlyDictionary<string, object?> Fields)
    {
        public string ReferenceId => Record.ReferenceId;
    }

    /// <summary>
    /// The units with their values normalized. A record type the org doesn't have (or not active) is a problem of that
    /// unit alone: the unit is rejected, transient (an admin may yet add or activate it: the object's record types are
    /// read again next time), and the rest of the batch carries on. A record's record type becomes its RecordTypeId
    /// here, a field like any other.
    /// </summary>
    internal async Task<(List<NormalizedUnit> Normalized, List<SyncResult> Rejected)> NormalizeAsync(
        IReadOnlyList<(string Key, SyncUnit Unit)> units, CancellationToken ct)
    {
        var normalized = new List<NormalizedUnit>(units.Count);
        var rejected = new List<SyncResult>();
        foreach (var (key, unit) in units)
        {
            var problems = new List<SalesforceError>();
            var records = new List<NormalizedRecord>(unit.Records.Count);
            foreach (var record in unit.Records)
            {
                records.Add(await NormalizeAsync(record, problems, ct));
            }

            if (problems.Count == 0)
            {
                normalized.Add(new NormalizedUnit(key, records));
            }
            else
            {
                rejected.Add(new(key, SyncOutcome.TransientFailure, problems));
            }
        }
        return (normalized, rejected);
    }

    /// <summary>
    /// A record type the org rejected or didn't have may just have been created, activated or assigned: the objects
    /// are read again next time, in every pod.
    /// </summary>
    internal async Task InvalidateRecordTypesAsync(IReadOnlyList<(string Key, SyncUnit Unit)> units, IEnumerable<SyncResult> results,
        CancellationToken ct)
    {
        var failed = results
            .SelectMany(r => r.Errors.Where(e => e.Code == SalesforceErrors.InvalidRecordType).Select(e => (r.Key, e.ReferenceId)))
            .ToHashSet();
        foreach (var sobject in units
                     .SelectMany(u => u.Unit.Records.Where(r => failed.Contains((u.Key, r.ReferenceId))))
                     .Select(r => r.SObject)
                     .Distinct(StringComparer.OrdinalIgnoreCase))
        {
            await recordTypes.InvalidateAsync(sobject, ct);
        }
    }

    private async Task<NormalizedRecord> NormalizeAsync(SObjectRecord record, List<SalesforceError> problems, CancellationToken ct)
    {
        var fields = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (var (name, value) in record.Fields)
        {
            fields[name] = FieldValues.Normalize(value);
        }

        if (record.RecordType is { } developerName)
        {
            if (await recordTypes.IdOfAsync(record.SObject, developerName, ct) is { } id)
            {
                fields[RecordTypeCache.RecordTypeIdField] = id;
            }
            else
            {
                problems.Add(new(record.ReferenceId, SalesforceErrors.InvalidRecordType,
                    $"Record type '{developerName}' of {record.SObject} doesn't exist or is not active", [RecordTypeCache.RecordTypeIdField]));
            }
        }

        return new NormalizedRecord(record, fields);
    }
}
