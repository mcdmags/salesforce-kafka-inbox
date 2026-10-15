using System.Text.RegularExpressions;
using SalesforceSync.Salesforce;

namespace SalesforceSync.Sync.Upsert;

/// <summary>
/// One Composite write request: its units and their writes by collection. The collections' order, their reference
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
    public IReadOnlyList<(string ReferenceId, bool Create, List<PlannedWrite> Writes)> Collections { get; private set; } = [];

    /// <summary>
    /// Distributes the units' writes over Composite requests. Records are grouped into one collection per level,
    /// object and verb; children that point at their parent by reference get a collection of their own, because one
    /// reference without a value (its parent failed) fails that whole subrequest, and children with a literal Id
    /// should not go down with it. A request takes 5 collections of 200 records; a unit that fits none of the requests
    /// so far starts another, so an entity's records (and the references between them) share a request.
    /// </summary>
    /// <returns>The requests, and a permanent failure for each unit that can't fit any request.</returns>
    public static (List<CompositeWrite> Requests, List<UpsertResult> TooLarge) Pack(IReadOnlyList<PlannedUnit> units)
    {
        var requests = new List<CompositeWrite>();
        var tooLarge = new List<UpsertResult>();

        foreach (var unit in units)
        {
            var needed = unit.Writes.GroupBy(w => w.Collection).ToDictionary(g => g.Key, g => g.Count());
            if (needed.Count > SalesforceRestClient.MaxCompositeCollections || needed.Values.Any(n => n > SalesforceRestClient.MaxCollectionRecords))
            {
                tooLarge.Add(new(unit.Key, SyncOutcome.PermanentFailure, [new SalesforceError(null, "LIMIT_EXCEEDED",
                    $"The unit needs {needed.Count} collections; a Composite request takes {SalesforceRestClient.MaxCompositeCollections} " +
                    $"of {SalesforceRestClient.MaxCollectionRecords} records")]));
                continue;
            }

            // First request with room, so units needing the same collections end up together
            var request = requests.FirstOrDefault(r => r.Fits(needed));
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
    /// The request as one sObject Collections upsert by external id, when it can be one: none of its writes points at
    /// a record created in the same request (an upsert has no <c>@{reference}</c>), all are of one object, and they fit
    /// one call (200 records). Same single API call as the Composite request, with a plainer body: each record carries
    /// its external id and exactly the fields planned for it (all of a new record's, only the changed ones of an
    /// existing record). Null when the request stays Composite: one write needing a reference (a new parent and its
    /// new child) keeps the whole request Composite, writes that wouldn't need it included, and writes of several
    /// objects stay one Composite request rather than one upsert per object. Marks the writes
    /// <see cref="PlannedWrite.ByUpsert"/>, which is how their results are read.
    /// </summary>
    public UpsertCall? AsUpsert()
    {
        var writes = Collections.SelectMany(c => c.Writes).ToList();
        if (writes.Count is 0 or > SalesforceRestClient.MaxCollectionRecords
            || writes.Any(w => w.Links.Count > 0)
            || writes.Select(w => (w.SObject, w.ExternalIdField)).Distinct().Count() != 1)
        {
            return null;
        }

        foreach (var write in writes)
        {
            write.ByUpsert = true;
        }

        return new UpsertCall($"{ReferenceIdFor(writes[0].SObject)}_upsert", writes[0].SObject, writes[0].ExternalIdField, writes);
    }

    /// <summary>The subrequests, in the order and with the reference ids fixed by <see cref="Pack"/>.</summary>
    public List<CollectionRequest> BuildRequests()
    {
        return [.. Collections.Select(c => new CollectionRequest(c.ReferenceId, c.Create, [.. c.Writes.Select(w => w.Body())]))];
    }

    /// <summary>Whether a unit needing these collections (key -> records) still fits within Salesforce's limits.</summary>
    private bool Fits(IReadOnlyDictionary<CollectionKey, int> needed)
    {
        return _byKey.Keys.Union(needed.Keys).Count() <= SalesforceRestClient.MaxCompositeCollections
               && needed.All(n => (_byKey.GetValueOrDefault(n.Key)?.Count ?? 0) + n.Value <= SalesforceRestClient.MaxCollectionRecords);
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
    /// Parents before children (a reference can only point back), creates before updates. Each record learns its
    /// collection and position there, which is how its children refer to it.
    /// </summary>
    private void Arrange()
    {
        var collections = new List<(string, bool, List<PlannedWrite>)>();
        foreach (var (key, writes) in _byKey.OrderBy(c => c.Key.Depth).ThenByDescending(c => c.Key.Create)
                     .ThenBy(c => c.Key.SObject, StringComparer.Ordinal).ThenBy(c => c.Key.ByReference))
        {
            var referenceId = ReferenceIdFor(key);
            if (collections.Any(c => c.Item1 == referenceId))
            {
                referenceId += $"_{key.Depth}";
            }

            for (var i = 0; i < writes.Count; i++)
            {
                (writes[i].CollectionReferenceId, writes[i].IndexInCollection) = (referenceId, i);
            }

            collections.Add((referenceId, key.Create, writes));
        }
        Collections = collections;
    }

    /// <summary>E.g. Account_create, Profile__c_create_ref. Reference ids take letters, digits and underscores only.</summary>
    private static string ReferenceIdFor(CollectionKey key)
    {
        return ReferenceIdFor($"{key.SObject}_{(key.Create ? "create" : "update")}{(key.ByReference ? "_ref" : "")}");
    }

    private static string ReferenceIdFor(string name)
    {
        return NotReferenceIdCharacter().Replace(name, "_");
    }

    [GeneratedRegex("[^A-Za-z0-9_]")]
    private static partial Regex NotReferenceIdCharacter();
}

/// <summary>The writes of one unit; the unit is saved only if all of them are.</summary>
internal sealed record PlannedUnit(string Key, IReadOnlyList<PlannedWrite> Writes);

/// <summary>The collection a write goes into: one per level, object and verb.</summary>
/// <param name="Depth">0 for a record that looks up nothing in its unit, 1 for its children, ...</param>
/// <param name="ByReference">It points at its parent's create by reference (<c>@{Account_create[3].id}</c>).</param>
internal sealed record CollectionKey(int Depth, string SObject, bool Create, bool ByReference);

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

/// <summary>One record to create (<paramref name="id"/> null) or update, and what came of it.</summary>
/// <param name="changes">The fields to send, by API name.</param>
/// <param name="lookupFields">The lookups this write sets, by field name.</param>
internal sealed class PlannedWrite(NormalizedRecord record, string? id, IReadOnlyDictionary<string, object?> changes,
    IReadOnlyList<ParentReference> links, IReadOnlySet<string> lookupFields, int depth)
{
    public bool Create => id is null;
    public string ReferenceId => record.ReferenceId;
    public string SObject => record.Schema.Name;

    /// <summary>The external id field as the org names it.</summary>
    public string ExternalIdField => record.Schema.Find(record.Record.ExternalIdField)!.Name;

    /// <summary>Sent in an sObject Collections upsert (matched by external id) rather than a create or an update.</summary>
    public bool ByUpsert { get; set; }
    public IReadOnlyList<ParentReference> Links => links;
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

    /// <summary>
    /// The record as a Collections upsert takes it: its type, its external id (which decides create or update) and the
    /// planned fields. No Id: Salesforce finds the record by the external id.
    /// </summary>
    public IReadOnlyDictionary<string, object?> UpsertBody()
    {
        var body = new Dictionary<string, object?>
        {
            ["attributes"] = new Dictionary<string, object?> { ["type"] = SObject },
            [ExternalIdField] = record.Record.ExternalId
        };
        foreach (var (field, value) in changes)
        {
            body[field] = value;
        }

        return body;
    }
}