using SalesforceSync.Infrastructure.Salesforce;

namespace SalesforceSync.Sync;

/// <summary>
/// Step 1. The field metadata of every object a batch involves (cached per process, revalidated with
/// If-Modified-Since), and dropping it when it is out of date.
/// </summary>
internal sealed class Describer(ISalesforceRestClient salesforce, SalesforceSchemaCache schemaCache, TimeSpan refreshInterval)
{
    /// <summary>Field metadata of every object involved (cached per process, revalidated with If-Modified-Since).</summary>
    internal async Task<Dictionary<string, SObjectSchema>> DescribeAsync(IReadOnlyList<(string Key, SyncUnit Unit)> units,
        IReadOnlyList<SoqlQuery> queries, CancellationToken ct)
    {
        var sobjects = units.SelectMany(u => u.Unit.Records)
            .SelectMany(r => r.Parents.Select(p => p.SObject).Append(r.SObject))
            .Concat(queries.Select(q => q.SObject))
            .Distinct(StringComparer.OrdinalIgnoreCase);

        var schemas = new Dictionary<string, SObjectSchema>(StringComparer.OrdinalIgnoreCase);
        foreach (var sobject in sobjects)
        {
            schemas[sobject] = await schemaCache.GetAsync(sobject, refreshInterval,
                (since, token) => salesforce.DescribeAsync(sobject, since, token), ct);
        }

        return schemas;
    }

    /// <summary>
    /// A record type the cached describe doesn't have (or not as active and available), or whose Id Salesforce rejected,
    /// may just have been created, activated or assigned: its object is described again next time.
    /// </summary>
    internal async Task InvalidateRecordTypesAsync(IReadOnlyList<(string Key, SyncUnit Unit)> units, IEnumerable<SyncResult> results,
        CancellationToken ct)
    {
        var failed = results
            .SelectMany(r => r.Errors.Where(e => e.Code == SalesforceErrors.InvalidRecordType).Select(e => (r.Key, e.ReferenceId)))
            .ToHashSet();
        var sobjects = units
            .SelectMany(u => u.Unit.Records.Where(r => failed.Contains((u.Key, r.ReferenceId))))
            .Select(r => r.SObject)
            .Distinct(StringComparer.OrdinalIgnoreCase);
        foreach (var sobject in sobjects)
        {
            await schemaCache.InvalidateAsync(sobject, ct);
        }
    }

    /// <summary>The objects are described again the next time they are needed.</summary>
    internal async Task InvalidateAsync(IEnumerable<string> sobjects, CancellationToken ct)
    {
        foreach (var sobject in sobjects)
        {
            await schemaCache.InvalidateAsync(sobject, ct);
        }
    }
}
