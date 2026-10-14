using SalesforceSync.Salesforce;

namespace SalesforceSync.Sync.Upsert;

/// <summary>The writes of one unit; the unit is saved only if all of them are.</summary>
internal sealed record PlannedUnit(string Key, IReadOnlyList<PlannedWrite> Writes);

/// <summary>The collection a write goes into: one per level, object and verb.</summary>
/// <param name="Depth">0 for a record that looks up nothing in its unit, 1 for its children, ...</param>
/// <param name="ByReference">It points at its parent's create by reference (<c>@{Account_create[3].id}</c>).</param>
internal sealed record CollectionKey(int Depth, string SObject, bool Create, bool ByReference);

/// <summary>A lookup to set, by reference, to a record that is created in the same request.</summary>
internal sealed record NewParentLink(SalesforceField Field, PlannedWrite Parent);

/// <summary>What came of a write, least to most significant: a unit's outcome follows its worst record.</summary>
internal enum WriteStatus
{
    Saved,
    /// <summary>Not saved because of a record of another unit (its subrequest went down on a broken reference).</summary>
    NotAttempted,
    /// <summary>Not saved because its parent in the same unit wasn't; the parent's result says what to do.</summary>
    ParentNotSaved,
    Transient,
    Permanent
}

/// <summary>One record to create (<paramref name="id"/> null) or update, and what came of it.</summary>
/// <param name="changes">The fields to send, by API name.</param>
/// <param name="lookupFields">The lookups this write sets, by field name.</param>
internal sealed class PlannedWrite(NormalizedRecord record, string? id, IReadOnlyDictionary<string, object?> changes,
    IReadOnlyList<NewParentLink> links, IReadOnlySet<string> lookupFields, int depth)
{
    public bool Create => id is null;
    public string ReferenceId => record.ReferenceId;
    public IReadOnlyList<NewParentLink> Links => links;
    public IReadOnlySet<string> LookupFields => lookupFields;

    public CollectionKey Collection { get; } = new(depth, record.Schema.Name, id is null, links.Count > 0);

    /// <summary>Where the record is in the request: its collection's reference id and its position in it.</summary>
    public string? CollectionReferenceId { get; set; }
    public int IndexInCollection { get; set; }

    public WriteStatus Status { get; set; } = WriteStatus.NotAttempted;
    public List<SalesforceError> Errors { get; set; } = [];

    /// <summary>The record as sObject Collections takes it: its type, its id when updating, and the fields to set.</summary>
    public IReadOnlyDictionary<string, object?> Body()
    {
        var body = new Dictionary<string, object?> { ["attributes"] = new Dictionary<string, object?> { ["type"] = record.Schema.Name } };
        if (id is not null)
        {
            body["id"] = id;
        }

        foreach (var (field, value) in changes)
        {
            body[field] = value;
        }

        foreach (var link in links)
        {
            // The parent's row in its collection's response. Composite documents @{referenceId.field} with []
            // indexing; indexing into an sObject Collections response is not confirmed: verify in a sandbox.
            body[link.Field.Name] = $"@{{{link.Parent.CollectionReferenceId}[{link.Parent.IndexInCollection}].id}}";
        }
        return body;
    }
}