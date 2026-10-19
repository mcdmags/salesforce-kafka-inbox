using Microsoft.Extensions.Caching.Hybrid;
using Microsoft.Extensions.Options;

namespace SalesforceSync.Infrastructure.Salesforce;

/// <summary>
/// Ids of the active record types of each object, by developer name: Salesforce takes a record type as its Id, which
/// differs between orgs. One SOQL query on RecordType per object, cached in a <see cref="HybridCache"/> for
/// <see cref="SalesforceOptions.RecordTypeRefreshInterval"/>: in process, and shared by all pods through the registered
/// <see cref="Microsoft.Extensions.Caching.Distributed.IDistributedCache"/> (Redis, or memory when none is configured),
/// so the org is asked once per interval in total and a new pod starts warm. Concurrent callers share one query per
/// object, and a failing cache backend is logged and treated as empty. Nothing else of the org's metadata is read.
/// </summary>
public sealed class RecordTypeCache(HybridCache cache, ISalesforceRestClient salesforce, IOptions<SalesforceOptions> options)
{
    /// <summary>The field that holds a record's record type.</summary>
    public const string RecordTypeIdField = "RecordTypeId";

    /// <summary>A row of the RecordType query: a record type of an object, as the org has it.</summary>
    internal sealed record RecordTypeRow
    {
        public required string Id { get; init; }
        public required string DeveloperName { get; init; }
        public required bool IsActive { get; init; }
    }

    private readonly HybridCacheEntryOptions _entry = new()
    {
        Expiration = options.Value.RecordTypeRefreshInterval,
        LocalCacheExpiration = options.Value.RecordTypeRefreshInterval
    };

    /// <summary>
    /// <c>salesforce-sync:recordtypes:{org host}:{API version}:{object}</c>: different orgs or API versions never share an entry.
    /// </summary>
    private readonly string _keyPrefix = KeyPrefix(options.Value.InstanceUrl, options.Value.ApiVersion);

    /// <summary>The Id of the object's active record type with this developer name, or null if the org has none.</summary>
    public async Task<string?> IdOfAsync(string sobject, string developerName, CancellationToken ct)
    {
        var recordTypes = await cache.GetOrCreateAsync(Key(sobject), token => ReadAsync(sobject, token), _entry, cancellationToken: ct);
        return recordTypes.FirstOrDefault(r => r.DeveloperName.Equals(developerName, StringComparison.OrdinalIgnoreCase))?.Id;
    }

    /// <summary>The object's record types are read from the org again next time (one was missing or refused), by every pod.</summary>
    public async Task InvalidateAsync(string sobject, CancellationToken ct)
    {
        await cache.RemoveAsync(Key(sobject), ct);
    }

    private async ValueTask<RecordTypeRow[]> ReadAsync(string sobject, CancellationToken ct)
    {
        var rows = await salesforce.SelectAsync<RecordTypeRow>(
            SalesforceRestClient.Soql("RecordType", ["Id", "DeveloperName", "IsActive"], "SobjectType", [sobject]), ct);
        // Developer names are unique per namespace only: of two with the same name, the first is kept
        return [.. rows.Where(r => r.IsActive).DistinctBy(r => r.DeveloperName, StringComparer.OrdinalIgnoreCase)];
    }

    private string Key(string sobject)
    {
        return _keyPrefix + sobject.ToLowerInvariant();
    }

    private static string KeyPrefix(string instanceUrl, string apiVersion)
    {
        var host = Uri.TryCreate(instanceUrl, UriKind.Absolute, out var url) ? url.Host : instanceUrl;
        return $"salesforce-sync:recordtypes:{host.ToLowerInvariant()}:{apiVersion}:";
    }
}
