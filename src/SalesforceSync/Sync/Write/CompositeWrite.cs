using System.Text.RegularExpressions;
using SalesforceSync.Salesforce;

namespace SalesforceSync.Sync.Write;

/// <summary>
/// One write request, one API call: its units and their writes by collection, each collection an sObject Collections
/// upsert by external id, or the Collections delete of the request's records to delete, by Id. A request of one
/// collection (one object, e.g. only Accounts changed, or only deletes) is that collection on its own; one of several
/// (Accounts and their Profiles, a new child linked to its new parent by a lookup, upserts and deletes) is one
/// Composite request with a subrequest per collection, parents first and the delete last. The collections' order, their reference
/// ids and every write's position are fixed when the request is assembled (<see cref="Pack"/>), so building the
/// request and reading its results never depend on each other.
/// </summary>
internal sealed partial class CompositeWrite
{
    private readonly Dictionary<CollectionKey, List<PlannedWrite>> _byKey = [];

    private CompositeWrite()
    {
    }

    public List<PlannedUnit> Units { get; } = [];

    /// <summary>The collections in sending order, with their reference ids.</summary>
    public IReadOnlyList<(string ReferenceId, CollectionKey Key, List<PlannedWrite> Writes)> Collections { get; private set; } = [];

    /// <summary>
    /// Distributes the units' writes over requests. Records are grouped into one collection per level and object;
    /// children that point at their parent by reference get a collection of their own, because one reference without
    /// a value (its parent failed) fails that whole subrequest, and children with a literal Id should not go down with
    /// it. A request takes 5 collections of <paramref name="maxRecordsPerWrite"/> records; a unit that fits none of the
    /// requests so far starts another, so an entity's records (and the references between them) share a request, and its
    /// outcome is read from that request's answers alone.
    /// </summary>
    /// <param name="units">The units with writes, in order.</param>
    /// <param name="maxRecordsPerWrite">The row type's <see cref="Inbox.IInboxRow.MaxRecordsPerWrite"/>: records per collection.</param>
    /// <returns>The requests, and a permanent failure for each unit that can't fit any request.</returns>
    public static (List<CompositeWrite> Requests, List<SyncResult> TooLarge) Pack(IReadOnlyList<PlannedUnit> units,
        int maxRecordsPerWrite)
    {
        var requests = new List<CompositeWrite>();
        var tooLarge = new List<SyncResult>();

        foreach (var unit in units)
        {
            var needed = unit.Writes.GroupBy(w => w.Collection).ToDictionary(g => g.Key, g => g.Count());
            if (needed.Count > SalesforceRestClient.MaxCompositeCollections || needed.Values.Any(n => n > maxRecordsPerWrite))
            {
                tooLarge.Add(new(unit.Key, SyncOutcome.PermanentFailure, [new SalesforceError(null, "LIMIT_EXCEEDED",
                    $"The unit needs {needed.Count} collections; a Composite request takes {SalesforceRestClient.MaxCompositeCollections} " +
                    $"of {maxRecordsPerWrite} records")]));
                continue;
            }

            // First request with room, so units needing the same collections end up together
            var request = requests.FirstOrDefault(r => r.Fits(needed, maxRecordsPerWrite));
            if (request is null)
            {
                requests.Add(request = new CompositeWrite());
            }

            request.Add(unit);
        }

        foreach (var request in requests)
        {
            request.Arrange();
        }

        return (requests, tooLarge);
    }

    /// <summary>
    /// The upserts and the delete, in the order and with the reference ids fixed by <see cref="Pack"/>: parents first, so
    /// a new parent exists before its child is sent. Each upserted record carries its external id and exactly the
    /// fields planned for it (all of a new record's, only the changed ones of an existing record; a lookup as the Id the
    /// read found, or as a reference to its new parent's result); each deleted one is its Id.
    /// </summary>
    public List<CollectionRequest> BuildRequests()
    {
        return [.. Collections.Select(c => c.Key.Delete
            ? new CollectionDelete(c.ReferenceId, [.. c.Writes.Select(w => w.Id!)])
            : (CollectionRequest)new CollectionUpsert(c.ReferenceId, c.Key.SObject, c.Key.ExternalIdField,
                [.. c.Writes.Select(w => w.Body())]))];
    }

    /// <summary>Whether a unit needing these collections (key -> records) still fits within the limits.</summary>
    private bool Fits(IReadOnlyDictionary<CollectionKey, int> needed, int maxRecordsPerWrite)
    {
        return _byKey.Keys.Union(needed.Keys).Count() <= SalesforceRestClient.MaxCompositeCollections
               && needed.All(n => (_byKey.GetValueOrDefault(n.Key)?.Count ?? 0) + n.Value <= maxRecordsPerWrite);
    }

    private void Add(PlannedUnit unit)
    {
        Units.Add(unit);
        foreach (var write in unit.Writes)
        {
            if (!_byKey.TryGetValue(write.Collection, out var writes))
            {
                _byKey[write.Collection] = writes = [];
            }

            writes.Add(write);
        }
    }

    /// <summary>
    /// Parents before children (a reference can only point back). Each record learns its collection and position
    /// there, which is how its children refer to it.
    /// </summary>
    private void Arrange()
    {
        var collections = new List<(string, CollectionKey, List<PlannedWrite>)>();
        foreach (var (key, writes) in _byKey.OrderBy(c => c.Key.Depth).ThenBy(c => c.Key.SObject, StringComparer.Ordinal)
                     .ThenBy(c => c.Key.ByReference))
        {
            var referenceId = ReferenceIdFor(key);
            if (collections.Any(c => c.Item1 == referenceId))
            {
                referenceId += $"_{collections.Count}";
            }

            for (var i = 0; i < writes.Count; i++)
            {
                (writes[i].CollectionReferenceId, writes[i].IndexInCollection) = (referenceId, i);
            }

            collections.Add((referenceId, key, writes));
        }
        Collections = collections;
    }

    /// <summary>E.g. Account, Contact_ref, delete. Reference ids take letters, digits and underscores only.</summary>
    private static string ReferenceIdFor(CollectionKey key)
    {
        return key.Delete ? "delete" : NotReferenceIdCharacter().Replace($"{key.SObject}{(key.ByReference ? "_ref" : "")}", "_");
    }

    [GeneratedRegex("[^A-Za-z0-9_]")]
    private static partial Regex NotReferenceIdCharacter();
}

/// <summary>The writes of one unit; the unit is saved only if all of them are.</summary>
internal sealed record PlannedUnit(string Key, IReadOnlyList<PlannedWrite> Writes);

/// <summary>The collection (one upsert) a write goes into: one per level and object; or <see cref="Deletes"/>.</summary>
/// <param name="Depth">0 for a record that points at nothing in its unit (by a lookup or a foreign key), 1 for its children, ...</param>
/// <param name="ExternalIdField">The external id field as the org names it, which the upsert matches records by.</param>
/// <param name="ByReference">It points at its new parent by reference (<c>@{Account[3].id}</c>).</param>
/// <param name="SObject">The object's API name.</param>
/// <param name="Delete">The collection deletes its records, by Id.</param>
internal sealed record CollectionKey(int Depth, string SObject, string ExternalIdField, bool ByReference, bool Delete = false)
{
    /// <summary>
    /// The one delete of a request: a Collections delete takes the Ids of records of any objects. It goes after every
    /// upsert of the request, so a child that the upserts move to another parent is not taken down by a delete of its
    /// old parent in the same request (a master-detail delete cascades to the children).
    /// </summary>
    public static readonly CollectionKey Deletes = new(int.MaxValue, "", "Id", false, Delete: true);
}

/// <summary>A lookup to set, by reference, to a record that is created in the same request.</summary>
internal sealed record ParentReference(SalesforceField Field, PlannedWrite Parent);

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

/// <summary>One record to create (<paramref name="id"/> null), update or delete, and what came of it.</summary>
/// <param name="changes">The fields to send, by API name.</param>
/// <param name="parents">The new records of its unit it points at (by a lookup or a foreign key): sent before it.</param>
/// <param name="lookupFields">The lookups this write sets, by field name.</param>
/// <param name="record">The record as normalized against Describe.</param>
/// <param name="id">Its Id in Salesforce, null when it is new; a delete is by it.</param>
/// <param name="links">The lookups set by reference to a new parent's result.</param>
/// <param name="depth">Its level in the unit: after every record it points at.</param>
internal sealed class PlannedWrite(NormalizedRecord record, string? id, IReadOnlyDictionary<string, object?> changes,
    IReadOnlyList<ParentReference> links, IReadOnlyList<PlannedWrite> parents, IReadOnlySet<string> lookupFields, int depth)
{
    public bool Create => id is null;
    public bool Delete => record.Record.Delete;
    public string? Id => id;
    public string ReferenceId => record.ReferenceId;
    public IReadOnlyList<ParentReference> Links => links;
    public IReadOnlyList<PlannedWrite> Parents => parents;
    public IReadOnlySet<string> LookupFields => lookupFields;

    public CollectionKey Collection { get; } = record.Record.Delete
        ? CollectionKey.Deletes
        : new(depth, record.Schema.Name, record.Schema.Find(record.Record.ExternalIdField)!.Name, links.Count > 0);

    /// <summary>Where the record is in the request: its collection's reference id and its position in it.</summary>
    public string? CollectionReferenceId { get; set; }
    public int IndexInCollection { get; set; }

    public WriteStatus Status { get; set; } = WriteStatus.NotAttempted;
    public List<SalesforceError> Errors { get; set; } = [];

    /// <summary>
    /// The record as a Collections upsert takes it: its type, its external id (which decides create or update) and the
    /// planned fields. No Id: Salesforce finds the record by the external id.
    /// </summary>
    public IReadOnlyDictionary<string, object?> Body()
    {
        var body = new Dictionary<string, object?>
        {
            ["attributes"] = new Dictionary<string, object?> { ["type"] = Collection.SObject },
            [Collection.ExternalIdField] = record.Record.ExternalId
        };
        foreach (var (field, value) in changes)
        {
            body[field] = value;
        }

        foreach (var link in links)
        {
            // The parent's row in its upsert's response. Composite documents @{referenceId.field} with []
            // indexing; indexing into an sObject Collections response is not confirmed: verify in a sandbox.
            body[link.Field.Name] = $"@{{{link.Parent.CollectionReferenceId}[{link.Parent.IndexInCollection}].id}}";
        }
        return body;
    }
}