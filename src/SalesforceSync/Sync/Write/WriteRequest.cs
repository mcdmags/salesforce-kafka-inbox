using System.Text.RegularExpressions;
using SalesforceSync.Infrastructure.Salesforce;

namespace SalesforceSync.Sync.Write;

/// <summary>
/// Step 5 of a batch: one API call's worth of writes. Salesforce takes at most 5 collections per call and 200 records
/// per collection, so the planned units are packed into as few requests as those limits allow: that is where the
/// batch saves its API calls. A request is either one collection on its own, or several collections as one Composite
/// request; a collection is all the upserts of one object at one level, or the request's single delete.
/// <para>
/// All the records of one entity (unit) go into the same request, so that its outcome is read from that request's
/// answers alone.
/// </para>
/// </summary>
internal sealed partial class WriteRequest
{
    private readonly Dictionary<CollectionKey, List<PlannedWrite>> _byKey = [];

    private WriteRequest()
    {
    }

    public List<PlannedUnit> Units { get; } = [];

    /// <summary>The collections in sending order, with their reference ids (how the answer names them).</summary>
    public IReadOnlyList<(string ReferenceId, CollectionKey Key, List<PlannedWrite> Writes)> Collections { get; private set; } = [];

    /// <summary>
    /// Distributes the units over requests: each unit goes into the first request it fits (so units that need the same
    /// collections end up together), else into a new one. A unit that fits no request at all (more than 5 collections,
    /// or more than <paramref name="maxRecordsPerWrite"/> records in one) can never be sent: a permanent failure.
    /// </summary>
    /// <param name="units">The units with writes, in order.</param>
    /// <param name="maxRecordsPerWrite">The row type's <see cref="Data.Inbox.IInboxRow.MaxRecordsPerWrite"/>: records per collection.</param>
    public static (List<WriteRequest> Requests, List<SyncResult> TooLarge) Pack(IReadOnlyList<PlannedUnit> units,
        int maxRecordsPerWrite)
    {
        var requests = new List<WriteRequest>();
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

            var request = requests.FirstOrDefault(r => r.Fits(needed, maxRecordsPerWrite));
            if (request is null)
            {
                requests.Add(request = new WriteRequest());
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
    /// What to send: an upsert per collection (each record with its external id and exactly the fields planned for it:
    /// all of a new record's, only the changed ones of an existing record) and the one delete (the records' Ids), in
    /// the order <see cref="Arrange"/> fixed.
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
    /// Puts the collections in sending order and names them. Parents come before children (the org rejects a child
    /// whose parent doesn't exist yet), and the delete comes last.
    /// </summary>
    private void Arrange()
    {
        var collections = new List<(string, CollectionKey, List<PlannedWrite>)>();
        foreach (var (key, writes) in _byKey.OrderBy(c => c.Key.Level).ThenBy(c => c.Key.SObject, StringComparer.Ordinal))
        {
            var referenceId = ReferenceIdFor(key);
            if (collections.Any(c => c.Item1 == referenceId))
            {
                referenceId += $"_{collections.Count}"; // the same object at a second level
            }

            collections.Add((referenceId, key, writes));
        }
        Collections = collections;
    }

    /// <summary>E.g. Account, Profile__c, delete. Reference ids take letters, digits and underscores only.</summary>
    private static string ReferenceIdFor(CollectionKey key)
    {
        return key.Delete ? "delete" : NotReferenceIdCharacter().Replace(key.SObject, "_");
    }

    [GeneratedRegex("[^A-Za-z0-9_]")]
    private static partial Regex NotReferenceIdCharacter();
}

/// <summary>The writes of one unit (one entity); the unit is saved only if all of them are.</summary>
internal sealed record PlannedUnit(string Key, IReadOnlyList<PlannedWrite> Writes);

/// <summary>The collection (one upsert) a write goes into: one per level and object; or <see cref="Deletes"/>.</summary>
/// <param name="Level">0 for a record whose parents are all in Salesforce already, one more than its deepest parent in its unit otherwise.</param>
/// <param name="SObject">The object's API name.</param>
/// <param name="ExternalIdField">The external id field as the org names it, which the upsert matches records by.</param>
/// <param name="Delete">The collection deletes its records, by Id.</param>
internal sealed record CollectionKey(int Level, string SObject, string ExternalIdField, bool Delete = false)
{
    /// <summary>
    /// The one delete of a request: a Collections delete takes the Ids of records of any objects. It goes after every
    /// upsert of the request, so a child that the upserts move to another parent is not taken down by a delete of its
    /// old parent in the same request (a master-detail delete cascades to the children).
    /// </summary>
    public static readonly CollectionKey Deletes = new(int.MaxValue, "", "Id", Delete: true);
}

/// <summary>What came of a write, least to most significant: a unit's outcome follows its worst record.</summary>
internal enum WriteStatus
{
    Saved,
    /// <summary>Not saved because of a record of another unit; retry now, no attempt spent.</summary>
    NotAttempted,
    /// <summary>Not saved because its parent in the same unit wasn't; the parent's result says what to do.</summary>
    ParentNotSaved,
    Transient,
    Permanent
}

/// <summary>One record to create (<c>id</c> null), update or delete, and what came of it.</summary>
/// <param name="record">The record as normalized against Describe.</param>
/// <param name="id">Its Id in Salesforce, null when it is new; a delete is by it.</param>
/// <param name="changes">The fields to send, by API name.</param>
/// <param name="newParents">The new records of its unit that it points at: sent before it, and its outcome follows theirs.</param>
/// <param name="level">After every record it points at in its unit (see <see cref="CollectionKey.Level"/>).</param>
internal sealed class PlannedWrite(NormalizedRecord record, string? id, IReadOnlyDictionary<string, object?> changes,
    IReadOnlyList<PlannedWrite> newParents, int level)
{
    public bool Create => id is null;
    public bool Delete => record.Record.Delete;
    public string? Id => id;
    public string ReferenceId => record.ReferenceId;

    /// <summary>The records of its unit that this one points at and that are new: if one wasn't saved, this one can't be.</summary>
    public IReadOnlyList<PlannedWrite> Parents => newParents;

    public CollectionKey Collection { get; } = record.Record.Delete
        ? CollectionKey.Deletes
        : new(level, record.Schema.Name, record.Schema.Find(record.Record.ExternalIdField)!.Name);

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
        return body;
    }
}
