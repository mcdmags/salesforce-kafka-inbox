namespace SalesforceSync.Salesforce.SObjects;

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
