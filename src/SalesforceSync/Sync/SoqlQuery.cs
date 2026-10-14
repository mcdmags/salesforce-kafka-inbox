using SalesforceSync.Salesforce.SObjects;

namespace SalesforceSync.Sync;

/// <summary>Records of one sObject to read from Salesforce, by a field (normally a unique external id).</summary>
public sealed record SoqlQuery(string SObject, string WhereField, IReadOnlyCollection<string> Values)
{
    /// <summary>Reads the units' records of <typeparamref name="TSObject"/> by external id (to diff them).</summary>
    public static SoqlQuery RecordsOf<TSObject>(IEnumerable<UpsertUnit> units) where TSObject : class
    {
        var meta = SObjectMetadata<TSObject>.Instance;
        return new SoqlQuery(meta.SObject, meta.ExternalIdField, ExternalIdsOf(units, meta.SObject));
    }

    /// <summary>Reads the <typeparamref name="TSObject"/> records the units' lookups point at (for their Ids).</summary>
    public static SoqlQuery LookupTargetsOf<TSObject>(IEnumerable<UpsertUnit> units) where TSObject : class
    {
        var meta = SObjectMetadata<TSObject>.Instance;
        return new SoqlQuery(meta.SObject, meta.ExternalIdField, LookupIdsOf(units, meta.SObject));
    }

    /// <summary>The external ids of every record of <paramref name="sobject"/> in the units.</summary>
    public static IReadOnlyCollection<string> ExternalIdsOf(IEnumerable<UpsertUnit> units, string sobject)
    {
        return [.. units.SelectMany(u => u.Records)
                .Where(r => r.SObject.Equals(sobject, StringComparison.OrdinalIgnoreCase))
                .Select(r => r.ExternalId)
                .Distinct(StringComparer.Ordinal)];
    }

    /// <summary>The external ids every lookup of the units points at in <paramref name="sobject"/>.</summary>
    public static IReadOnlyCollection<string> LookupIdsOf(IEnumerable<UpsertUnit> units, string sobject)
    {
        return [.. units.SelectMany(u => u.Records)
                .SelectMany(r => r.Lookups ?? [])
                .Where(l => l.SObject.Equals(sobject, StringComparison.OrdinalIgnoreCase))
                .Select(l => l.ExternalId)
                .Distinct(StringComparer.Ordinal)];
    }
}