using System.Text.Json;

namespace SalesforceSync.Infrastructure.Salesforce;

/// <summary>
/// What the sync needs from Salesforce, one method per API call. The engine depends on this rather than on
/// <see cref="SalesforceRestClient"/> so it can be exercised without HTTP, and another transport could replace the REST one.
/// It is deliberately bulk-only: no single-record get, update, delete or upsert, because each of those costs one API call
/// per record against the daily budget (<c>SalesforceLimits:DailyApiAllocation</c>). Add a member only for an operation
/// that takes many records per call. The limits and URL helpers (<see cref="SalesforceRestClient.MaxUrlBytes"/>,
/// <see cref="SalesforceRestClient.Soql"/>) are static on the REST client: they are Salesforce's rules, not behaviour a
/// substitute would change.
/// </summary>
public interface ISalesforceRestClient
{
    /// <summary>The path every call starts with (<see cref="SalesforceOptions.DataPath"/>).</summary>
    string DataPath { get; }

    /// <summary>sObject Describe; with <paramref name="ifModifiedSince"/>, null when the org answers 304 Not Modified.</summary>
    Task<SObjectSchema?> DescribeAsync(string sobject, DateTimeOffset? ifModifiedSince, CancellationToken ct);

    /// <summary>Up to 5 queries in one Composite request (one API call), the rows keyed by reference id.</summary>
    Task<Dictionary<string, List<JsonElement>>> QueryAsync(IReadOnlyList<CompositeQuery> queries, CancellationToken ct);

    /// <summary>Up to 5 sObject Collections upserts and deletes of up to 200 records each, in one API call.</summary>
    Task<IReadOnlyList<CollectionResult>> WriteAsync(IReadOnlyList<CollectionRequest> collections, CancellationToken ct);
}
