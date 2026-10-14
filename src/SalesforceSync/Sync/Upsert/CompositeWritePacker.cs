using SalesforceSync.Salesforce;

namespace SalesforceSync.Sync.Upsert;

/// <summary>The planned writes as Composite requests, and the units that can't be sent at all.</summary>
internal sealed record PackedWrites(IReadOnlyList<CompositeWrite> Requests, IReadOnlyList<UpsertResult> TooLarge);

internal static class CompositeWritePacker
{
    /// <summary>
    /// Distributes the units' writes over Composite requests. Records are grouped into one collection per level,
    /// object and verb; children that point at their parent by reference get a collection of their own, because one
    /// reference without a value (its parent failed) fails that whole subrequest, and children with a literal Id
    /// should not go down with it. A request takes 5 collections of 200 records; a unit that fits none of the requests
    /// so far starts another, so an entity's records (and the references between them) share a request.
    /// </summary>
    public static PackedWrites Pack(IReadOnlyList<PlannedUnit> units)
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
            if (request is null) requests.Add(request = new CompositeWrite());
            request.Add(unit);
        }
        return new PackedWrites(requests, tooLarge);
    }
}
