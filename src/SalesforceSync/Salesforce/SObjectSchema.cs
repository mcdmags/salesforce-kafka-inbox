namespace SalesforceSync.Salesforce;

/// <summary>Salesforce field metadata from sObject Describe (only what the diff and validation need).</summary>
/// <param name="Precision">Numbers with decimals: total digits; digits left of the point = Precision - Scale.</param>
/// <param name="Digits">int and long fields: the maximum number of digits (they have no precision).</param>
public sealed record SalesforceField(
    string Name,
    string Type,
    int Length,
    int Precision,
    int Scale,
    bool Createable,
    bool Updateable,
    bool Nillable,
    int Digits = 0);

/// <summary>A record type from sObject Describe's <c>recordTypeInfos</c> (names as in the API).</summary>
/// <param name="Available">Assigned to the running user (profile or permission set), so records can be given it.</param>
public sealed record SalesforceRecordType(string DeveloperName, string RecordTypeId, bool Active, bool Available);

/// <summary>
/// Fields and record types of one sObject, keyed case-insensitively like Salesforce API names. Both come from the same
/// Describe, so the record type Ids are cached with the fields (<see cref="SalesforceSchemaCache"/>).
/// </summary>
public sealed class SObjectSchema(string name, IEnumerable<SalesforceField> fields, IEnumerable<SalesforceRecordType>? recordTypes = null)
{
    /// <summary>The field that holds a record's record type.</summary>
    public const string RecordTypeIdField = "RecordTypeId";

    public string Name { get; } = name;

    public IReadOnlyDictionary<string, SalesforceField> Fields { get; } =
        fields.ToDictionary(f => f.Name, StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Record types by developer name (an object without record types has only Master, or none). Developer names are
    /// unique per namespace only: of two with the same name, the first Describe lists is kept.
    /// </summary>
    public IReadOnlyDictionary<string, SalesforceRecordType> RecordTypes { get; } = (recordTypes ?? [])
        .DistinctBy(r => r.DeveloperName, StringComparer.OrdinalIgnoreCase)
        .ToDictionary(r => r.DeveloperName, StringComparer.OrdinalIgnoreCase);

    public SalesforceField? Find(string field)
    {
        return Fields.GetValueOrDefault(field);
    }
}

/// <summary>Result of a (conditional) describe call: the schema, or null when Salesforce answered 304 Not Modified.</summary>
public sealed record DescribeResult(SObjectSchema? Schema);