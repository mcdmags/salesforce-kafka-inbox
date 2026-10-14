namespace SalesforceSync.Salesforce.SObjects;

/// <summary>
/// Marks a class as a Salesforce object: its API name and the unique external id field records are matched by.
/// Every property of the class names its field with <c>[JsonPropertyName]</c>, so Salesforce API names (also
/// namespaced ones such as <c>LLC_BI__Active__c</c>) appear only on these classes.
/// <code>[SObject("Account", ExternalIdField = "External_Id__c")]</code>
/// </summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false)]
public sealed class SObjectAttribute(string name) : Attribute
{
    /// <summary>The object's API name, e.g. <c>Account</c> or <c>Profile__c</c>.</summary>
    public string Name { get; } = name;

    /// <summary>API name of the external id field; the class must have a string property with that name.</summary>
    public required string ExternalIdField { get; init; }
}

/// <summary>
/// On a string property: the field is a lookup, and the property holds the external id of the record it points at
/// (not a Salesforce Id; the engine resolves it). The target is named by its sObject class, which already knows its
/// own API name and external id field.
/// <code>[JsonPropertyName("Account__c"), SObjectLookup(typeof(AccountSObject))]
/// public string? AccountExternalId { get; set; }</code>
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
public sealed class SObjectLookupAttribute(Type target) : Attribute
{
    /// <summary>The sObject class (marked <see cref="SObjectAttribute"/>) of the record the lookup points at.</summary>
    public Type Target { get; } = target;
}
