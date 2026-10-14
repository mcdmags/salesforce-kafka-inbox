using SalesforceSync.Salesforce;

namespace SalesforceSync.Sync.Upsert;

/// <summary>
/// Checks a unit against the Describe metadata and normalizes its values (<see cref="FieldValues"/>), before any call:
/// an unknown field, a value Salesforce would reject, or a lookup that isn't one is a problem of that unit alone.
/// </summary>
internal static class RecordNormalizer
{
    /// <summary>The units that can be written, and a permanent failure for each of the others.</summary>
    public static (List<NormalizedUnit> Normalized, List<UpsertResult> Rejected) Normalize(
        IReadOnlyList<(string Key, UpsertUnit Unit)> units, IReadOnlyDictionary<string, SObjectSchema> schemas)
    {
        var normalized = new List<NormalizedUnit>(units.Count);
        var rejected = new List<UpsertResult>();
        foreach (var (key, unit) in units)
        {
            var candidate = Normalize(key, unit, schemas, out var problems);
            if (problems.Count > 0)
            {
                rejected.Add(new(key, SyncOutcome.PermanentFailure, problems));
            }
            else
            {
                normalized.Add(candidate);
            }
        }
        return (normalized, rejected);
    }

    /// <param name="problems">Why Salesforce would reject the unit; empty when it can be written.</param>
    public static NormalizedUnit Normalize(string key, UpsertUnit unit, IReadOnlyDictionary<string, SObjectSchema> schemas,
        out List<SalesforceError> problems)
    {
        var found = problems = [];
        var records = unit.Records.Select(r => Normalize(r, schemas, found)).ToList();
        return new NormalizedUnit(key, records);
    }

    private static NormalizedRecord Normalize(SObjectRecord record, IReadOnlyDictionary<string, SObjectSchema> schemas,
        List<SalesforceError> problems)
    {
        var schema = schemas[record.SObject];
        var fields = NormalizeFields(record, schema, problems);
        var lookups = CheckLookups(record, schema, schemas, problems);

        if (schema.Find(record.ExternalIdField) is null)
        {
            problems.Add(NoSuchColumn(record, record.ExternalIdField, schema));
        }

        return new NormalizedRecord(record, schema, fields, lookups);
    }

    private static Dictionary<SalesforceField, object?> NormalizeFields(SObjectRecord record, SObjectSchema schema,
        List<SalesforceError> problems)
    {
        var fields = new Dictionary<SalesforceField, object?>();
        foreach (var (name, value) in record.Fields)
        {
            if (schema.Find(name) is not { } field)
            {
                problems.Add(NoSuchColumn(record, name, schema));
            }
            else if (FieldValues.TryNormalize(field, value, out var normalized, out var error))
            {
                fields[field] = normalized;
            }
            else
            {
                problems.Add(new(record.ReferenceId, error.Code, $"{field.Name}: {error.Message}"));
            }
        }
        return fields;
    }

    /// <summary>A lookup needs a reference field on the record's object and the external id field on the target's.</summary>
    private static List<NormalizedLookup> CheckLookups(SObjectRecord record, SObjectSchema schema,
        IReadOnlyDictionary<string, SObjectSchema> schemas, List<SalesforceError> problems)
    {
        var lookups = new List<NormalizedLookup>();
        foreach (var lookup in record.Lookups ?? [])
        {
            if (schema.Find(lookup.Field) is { Type: "reference" } field && schemas[lookup.SObject].Find(lookup.ExternalIdField) is not null)
            {
                lookups.Add(new NormalizedLookup(field, lookup));
            }
            else
            {
                problems.Add(new(record.ReferenceId, "INVALID_FIELD",
                    $"{schema.Name}.{lookup.Field} is not a lookup, or {lookup.SObject}.{lookup.ExternalIdField} doesn't exist"));
            }
        }
        return lookups;
    }

    private static SalesforceError NoSuchColumn(SObjectRecord record, string column, SObjectSchema schema)
    {
        return new(record.ReferenceId, "INVALID_FIELD", $"No such column '{column}' on {schema.Name}");
    }
}