using SalesforceSync.Salesforce.SObjects;

namespace SalesforceSync.Sync;

/// <summary>A Salesforce record to create or update, identified by an external id.</summary>
/// <param name="ReferenceId">Unique within its unit; other records of the unit can point at it before it exists.</param>
/// <param name="Fields">Desired values by API name. Fields not listed are left alone.</param>
/// <param name="Lookups">Reference fields to set to another record's Id.</param>
/// <param name="RecordType">
/// Developer name of the record's record type: RecordTypeId is set to its Id (from Describe) like any other field.
/// Records of sObject classes always have one; null leaves RecordTypeId alone.
/// </param>
public sealed record SObjectRecord(
    string ReferenceId,
    string SObject,
    string ExternalIdField,
    string ExternalId,
    IReadOnlyDictionary<string, object?> Fields,
    IReadOnlyList<SObjectLookup>? Lookups = null,
    string? RecordType = null)
{
    /// <summary>
    /// The record for an sObject instance: the object, external id field, record type, field names and lookups come
    /// from the class's attributes (<see cref="SObjectMetadata{TSObject}"/>, cached per class). Every field is
    /// included, nulls too: the instance is the complete desired state, so a null field is cleared if Salesforce has a
    /// value.
    /// </summary>
    /// <exception cref="ArgumentException">The external id or a lookup's target id is missing.</exception>
    public static SObjectRecord From<TSObject>(string referenceId, TSObject sobject) where TSObject : class
    {
        var meta = SObjectMetadata<TSObject>.Instance;
        var externalId = meta.ExternalIdOf(sobject);
        if (string.IsNullOrWhiteSpace(externalId))
        {
            throw new ArgumentException($"{meta.SObject}.{meta.ExternalIdField} is required");
        }

        List<SObjectLookup>? lookups = null;
        foreach (var (lookup, targetId) in meta.LookupsOf(sobject))
        {
            if (string.IsNullOrWhiteSpace(targetId))
            {
                throw new ArgumentException($"{meta.SObject}.{lookup.Field} needs the external id of the {lookup.TargetSObject} it points at");
            }

            (lookups ??= []).Add(new SObjectLookup(lookup.Field, lookup.TargetSObject, lookup.TargetExternalIdField, targetId));
        }
        return new SObjectRecord(referenceId, meta.SObject, meta.ExternalIdField, externalId, meta.FieldsOf(sobject), lookups,
            meta.RecordType);
    }
}