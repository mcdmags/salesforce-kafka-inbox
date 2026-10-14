namespace SalesforceSync.Salesforce.SObjects;

/// <summary>A lookup field of an sObject class and the object it points at.</summary>
public sealed record SObjectLookupField(string Field, string TargetSObject, string TargetExternalIdField);
