namespace SalesforceSync.ToSalesforce;

/// <summary>
/// One Salesforce record an event asks for, identified by its object and external id: to create or update (upsert) with
/// these field values, or to delete. A type's <c>Map</c> returns the records of one entity in the order they must be
/// written: for an upsert, parents first; for a delete, children first.
/// </summary>
/// <param name="SObject">The object's API name, e.g. Account.</param>
/// <param name="ExternalId">The record's external id: its identity in this sync (the field is <see cref="RecordWriter.ExternalIdField"/>).</param>
/// <param name="Fields">The values to send, by API name; a null clears the field. Empty for a delete.</param>
/// <param name="RecordType">The developer name of the record type; its Id is sent as RecordTypeId. Null leaves it alone.</param>
/// <param name="Parents">Records this one points at by a Text field: they must exist in Salesforce before it is sent.</param>
/// <param name="Delete">Delete the record instead of upserting it.</param>
public sealed record Rec(
    string SObject,
    string ExternalId,
    IReadOnlyDictionary<string, object?> Fields,
    string? RecordType = null,
    IReadOnlyList<Parent>? Parents = null,
    bool Delete = false)
{
    public static Rec Deleting(string sobject, string externalId)
    {
        return new Rec(sobject, externalId, new Dictionary<string, object?>(), Delete: true);
    }
}

/// <summary>A record another points at: the Text <paramref name="Field"/> holds the external id of an <paramref name="SObject"/>.</summary>
public sealed record Parent(string Field, string SObject, string ExternalId);
