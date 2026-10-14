namespace SalesforceSync.Salesforce.SObjects;

/// <summary>
/// Marks a class as a Salesforce object: its API name, the unique external id field records are matched by, and the
/// record type every record is written with. Every property of the class names its field with
/// <c>[JsonPropertyName]</c>, so Salesforce API names (also namespaced ones such as <c>LLC_BI__Active__c</c>) appear
/// only on these classes.
/// <code>[SObject("Account", ExternalIdField = "External_Id__c", RecordType = "Business")]</code>
/// </summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false)]
public sealed class SObjectAttribute(string name) : Attribute
{
    /// <summary>The object's API name, e.g. <c>Account</c> or <c>Profile__c</c>.</summary>
    public string Name { get; } = name;

    /// <summary>API name of the external id field; the class must have a string property with that name.</summary>
    public required string ExternalIdField { get; init; }

    /// <summary>
    /// Developer name of the record type every record of the class has (the engine sets <c>RecordTypeId</c> to its Id,
    /// from Describe). It must exist in the org, be active and be assigned to the user the sync runs as.
    /// </summary>
    public required string RecordType { get; init; }
}