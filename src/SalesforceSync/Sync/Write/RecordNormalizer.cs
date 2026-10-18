using SalesforceSync.Infrastructure.Salesforce;

namespace SalesforceSync.Sync.Write;

/// <summary>
/// Step 2 of a batch: checks a unit against the Describe metadata and normalizes its values
/// (<see cref="FieldValues"/>), before any call. An unknown field, a value Salesforce would reject, a foreign key that
/// isn't a text field, or a record type the org doesn't offer is a problem of that unit alone: the unit is rejected and
/// the rest of the batch carries on. A record's record type becomes its RecordTypeId here, a field like any other.
/// </summary>
internal static class RecordNormalizer
{
    /// <summary>
    /// The units that can be written, and a failure for each of the others: permanent if any of its problems is
    /// (<see cref="SalesforceErrors.IsPermanent"/>), else transient (a record type the org doesn't offer yet).
    /// </summary>
    public static (List<NormalizedUnit> Normalized, List<SyncResult> Rejected) Normalize(
        IReadOnlyList<(string Key, SyncUnit Unit)> units, IReadOnlyDictionary<string, SObjectSchema> schemas,
        SalesforceErrors errors)
    {
        var normalized = new List<NormalizedUnit>(units.Count);
        var rejected = new List<SyncResult>();
        foreach (var (key, unit) in units)
        {
            var candidate = Normalize(key, unit, schemas, out var problems);
            if (problems.Count > 0)
            {
                var permanent = problems.Any(p => errors.IsPermanent(p.Code));
                rejected.Add(new(key, permanent ? SyncOutcome.PermanentFailure : SyncOutcome.TransientFailure,
                    [.. problems.OrderByDescending(p => errors.IsPermanent(p.Code))]));
            }
            else
            {
                normalized.Add(candidate);
            }
        }
        return (normalized, rejected);
    }

    /// <param name="problems">Why Salesforce would reject the unit; empty when it can be written.</param>
    /// <param name="key">The entity's key, for the errors.</param>
    /// <param name="unit">The records the strategy mapped.</param>
    /// <param name="schemas">Describe of every object in the unit, by API name.</param>
    public static NormalizedUnit Normalize(string key, SyncUnit unit, IReadOnlyDictionary<string, SObjectSchema> schemas,
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
        var values = RecordTypeId(record, schema, problems) is { } recordTypeId
            ? record.Fields.Append(new(SObjectSchema.RecordTypeIdField, recordTypeId))
            : record.Fields;
        var fields = NormalizeFields(record, values, schema, problems);
        CheckForeignKeys(record, schema, schemas, problems);

        if (schema.Find(record.ExternalIdField) is null)
        {
            problems.Add(NoSuchColumn(record, record.ExternalIdField, schema));
        }

        return new NormalizedRecord(record, schema, fields);
    }

    /// <summary>
    /// The Id of the record's record type, from Describe's record types: null if the record has none, or if the org
    /// has no such record type for the running user (a problem; the caller describes the object again next time).
    /// Describe rather than a query on RecordType: no extra call, cached and refreshed with the fields, and it already
    /// says whether the record type is active and available to the running user.
    /// </summary>
    private static string? RecordTypeId(SObjectRecord record, SObjectSchema schema, List<SalesforceError> problems)
    {
        if (record.RecordType is not { } developerName)
        {
            return null;
        }

        var recordType = schema.RecordTypes.GetValueOrDefault(developerName);
        var why = recordType switch
        {
            null => "doesn't exist",
            { Active: false } => "is not active",
            { Available: false } => "is not available to the running user",
            _ => null
        };
        if (why is null)
        {
            return recordType!.RecordTypeId;
        }

        problems.Add(new(record.ReferenceId, SalesforceErrors.InvalidRecordType,
            $"Record type '{developerName}' of {schema.Name} {why}", [SObjectSchema.RecordTypeIdField]));
        return null;
    }

    private static Dictionary<SalesforceField, object?> NormalizeFields(SObjectRecord record,
        IEnumerable<KeyValuePair<string, object?>> values, SObjectSchema schema, List<SalesforceError> problems)
    {
        var fields = new Dictionary<SalesforceField, object?>();
        foreach (var (name, value) in values)
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

    /// <summary>
    /// A foreign key needs the external id field on its parent's object, and a field on the record that holds text
    /// (not a reference, which holds an Id; the field itself is checked with the values).
    /// </summary>
    private static void CheckForeignKeys(SObjectRecord record, SObjectSchema schema,
        IReadOnlyDictionary<string, SObjectSchema> schemas, List<SalesforceError> problems)
    {
        foreach (var key in record.ForeignKeys ?? [])
        {
            if (schema.Find(key.Field) is { Type: "reference" } || schemas[key.SObject].Find(key.ExternalIdField) is null)
            {
                problems.Add(new(record.ReferenceId, "INVALID_FIELD",
                    $"{schema.Name}.{key.Field} is a lookup, not a foreign key, or {key.SObject}.{key.ExternalIdField} doesn't exist"));
            }
        }
    }

    private static SalesforceError NoSuchColumn(SObjectRecord record, string column, SObjectSchema schema)
    {
        return new(record.ReferenceId, "INVALID_FIELD", $"No such column '{column}' on {schema.Name}");
    }
}

/// <summary>A unit whose values are in the form Salesforce stores them, ready to be compared and sent.</summary>
internal sealed record NormalizedUnit(string Key, IReadOnlyList<NormalizedRecord> Records);

/// <summary>A record with its object's Describe metadata and its values normalized per field.</summary>
internal sealed record NormalizedRecord(
    SObjectRecord Record,
    SObjectSchema Schema,
    IReadOnlyDictionary<SalesforceField, object?> Fields)
{
    public string ReferenceId => Record.ReferenceId;
}

