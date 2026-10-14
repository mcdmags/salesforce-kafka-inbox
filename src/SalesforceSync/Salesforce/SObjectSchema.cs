namespace SalesforceSync.Salesforce;

/// <summary>Salesforce field metadata from sObject Describe (only what the diff and validation need).</summary>
/// <param name="RelationshipName">For a lookup: the name it is set by through the target's external id, e.g. Account__r.</param>
public sealed record SalesforceField(
    string Name,
    string Type,
    int Length,
    int Precision,
    int Scale,
    bool Createable,
    bool Updateable,
    bool Nillable,
    string? RelationshipName = null);

/// <summary>Fields of one sObject, keyed case-insensitively like Salesforce API names.</summary>
public sealed class SObjectSchema(string name, IEnumerable<SalesforceField> fields)
{
    public string Name { get; } = name;

    public IReadOnlyDictionary<string, SalesforceField> Fields { get; } =
        fields.ToDictionary(f => f.Name, StringComparer.OrdinalIgnoreCase);

    public SalesforceField? Find(string field) => Fields.GetValueOrDefault(field);
}

/// <summary>Result of a (conditional) describe call: the schema, or null when Salesforce answered 304 Not Modified.</summary>
public sealed record DescribeResult(SObjectSchema? Schema);
