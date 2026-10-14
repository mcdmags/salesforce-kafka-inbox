using System.Text.RegularExpressions;
using SalesforceSync.Salesforce;

namespace SalesforceSync.Sync.Upsert;

/// <summary>One Composite write request: its units and their writes by collection.</summary>
internal sealed partial class CompositeWrite
{
    private readonly Dictionary<CollectionKey, List<PlannedWrite>> _collections = [];

    public List<PlannedUnit> Units { get; } = [];

    /// <summary>The collections in sending order, set by <see cref="BuildRequests"/>.</summary>
    public List<(string ReferenceId, List<PlannedWrite> Writes)> Collections { get; } = [];

    /// <summary>Whether a unit needing these collections (key -> records) still fits within Salesforce's limits.</summary>
    public bool Fits(IReadOnlyDictionary<CollectionKey, int> needed)
        => _collections.Keys.Union(needed.Keys).Count() <= SalesforceRestClient.MaxCompositeCollections
           && needed.All(n => (_collections.GetValueOrDefault(n.Key)?.Count ?? 0) + n.Value <= SalesforceRestClient.MaxCollectionRecords);

    public void Add(PlannedUnit unit)
    {
        Units.Add(unit);
        foreach (var write in unit.Writes)
        {
            if (!_collections.TryGetValue(write.Collection, out var writes)) _collections[write.Collection] = writes = [];
            writes.Add(write);
        }
    }

    /// <summary>
    /// Parents before children (a reference can only point back), creates before updates. Each record learns its
    /// collection and position there, which is how its children refer to it.
    /// </summary>
    public List<CollectionRequest> BuildRequests()
    {
        var requests = new List<CollectionRequest>();
        foreach (var (key, writes) in _collections.OrderBy(c => c.Key.Depth).ThenByDescending(c => c.Key.Create)
                     .ThenBy(c => c.Key.SObject, StringComparer.Ordinal).ThenBy(c => c.Key.ByReference))
        {
            var referenceId = ReferenceIdFor(key);
            if (Collections.Any(c => c.ReferenceId == referenceId)) referenceId += $"_{key.Depth}";

            for (var i = 0; i < writes.Count; i++) (writes[i].CollectionReferenceId, writes[i].IndexInCollection) = (referenceId, i);
            Collections.Add((referenceId, writes));
            requests.Add(new CollectionRequest(referenceId, key.Create, writes.Select(w => w.Body()).ToList()));
        }
        return requests;
    }

    /// <summary>E.g. Account_create, Profile__c_create_ref. Reference ids take letters, digits and underscores only.</summary>
    private static string ReferenceIdFor(CollectionKey key)
        => NotReferenceIdCharacter().Replace(
            $"{key.SObject}_{(key.Create ? "create" : "update")}{(key.ByReference ? "_ref" : "")}", "_");

    [GeneratedRegex("[^A-Za-z0-9_]")]
    private static partial Regex NotReferenceIdCharacter();
}
