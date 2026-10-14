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
        return NotReferenceIdCharacter().Replace(
                $"{key.SObject}_{(key.Create ? "create" : "update")}{(key.ByReference ? "_ref" : "")}", "_");
    }

    [GeneratedRegex("[^A-Za-z0-9_]")]
    private static partial Regex NotReferenceIdCharacter();
}