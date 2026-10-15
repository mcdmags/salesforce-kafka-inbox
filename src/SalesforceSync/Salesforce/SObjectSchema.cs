using System.Text.Json.Serialization;

namespace SalesforceSync.Salesforce;

/// <summary>Salesforce field metadata from sObject Describe (only what the diff and validation need).</summary>
/// <param name="Precision">Numbers with decimals: total digits; digits left of the point = Precision - Scale.</param>
/// <param name="Digits">int and long fields: the maximum number of digits (they have no precision).</param>
/// <param name="RelationshipName">Reference fields: the name a related record is set by in a write, e.g.
/// <c>Account</c> for <c>AccountId</c>, <c>Account__r</c> for <c>Account__c</c>.</param>
public sealed record SalesforceField(
    string Name,
    string Type,
    int Length,
    int Precision,
    int Scale,
    bool Createable,
    bool Updateable,
    bool Nillable,
    int Digits = 0,
    string? RelationshipName = null)
{
    /// <summary>
    /// <see cref="RelationshipName"/>, or Salesforce's naming convention when Describe didn't say (a schema cached
    /// before the name was kept): a custom field's <c>__c</c> becomes <c>__r</c>, a standard field loses its <c>Id</c>.
    /// </summary>
    [JsonIgnore]
    public string? Relationship => RelationshipName
        ?? (Name.EndsWith("__c", StringComparison.OrdinalIgnoreCase) ? $"{Name[..^3]}__r"
            : Name.EndsWith("Id", StringComparison.Ordinal) && Name.Length > 2 ? Name[..^2]
            : null);
}

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