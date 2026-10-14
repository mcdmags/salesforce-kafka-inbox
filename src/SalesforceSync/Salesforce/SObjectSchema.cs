namespace SalesforceSync.Salesforce;

/// <summary>Salesforce field metadata from sObject Describe (only what the diff and validation need).</summary>
/// <param name="Precision">Numbers with decimals: total digits; digits left of the point = Precision - Scale.</param>
/// <param name="RelationshipName">For a lookup: the name it is set by through the target's external id, e.g. Account__r.</param>
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
    string? RelationshipName = null,
    int Digits = 0);

/// <summary>Fields of one sObject, keyed case-insensitively like Salesforce API names.</summary>
public sealed class SObjectSchema(string name, IEnumerable<SalesforceField> fields)
{
    public string Name { get; } = name;

    public IReadOnlyDictionary<string, SalesforceField> Fields { get; } =
        fields.ToDictionary(f => f.Name, StringComparer.OrdinalIgnoreCase);

    public SalesforceField? Find(string field)
    {
        return Fields.GetValueOrDefault(field);
    }
}

/// <summary>Result of a (conditional) describe call: the schema, or null when Salesforce answered 304 Not Modified.</summary>
public sealed record DescribeResult(SObjectSchema? Schema);