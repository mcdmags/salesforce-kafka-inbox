using System.Text.Json;

namespace SalesforceSync.Infrastructure.Salesforce;

/// <summary>
/// What the sync needs from Salesforce, one method per API call, each taking many records. The engine depends on this
/// rather than on <see cref="SalesforceRestClient"/> so it can be exercised without HTTP, and another transport could
/// replace the REST one.
/// <para>
/// There is no call for one record: it would cost one API call per record against the daily budget
/// (<c>SalesforceLimits:DailyApiAllocation</c>), where a read takes up to 2,000 external ids and an upsert or a delete up
/// to 200 records. The limits and URL helpers (<see cref="SalesforceRestClient.MaxUrlBytes"/>,
/// <see cref="SalesforceRestClient.Soql"/>) are static on the REST client: they are Salesforce's rules, not behaviour a
/// substitute would change.
/// </para>
/// </summary>
public interface ISalesforceRestClient
{
    /// <summary>The path every call starts with (<see cref="SalesforceOptions.DataPath"/>).</summary>
    string DataPath { get; }

    /// <summary>sObject Describe; with <paramref name="ifModifiedSince"/>, null when the org answers 304 Not Modified.</summary>
    Task<SObjectSchema?> DescribeAsync(string sobject, DateTimeOffset? ifModifiedSince, CancellationToken ct);

    /// <summary>Runs one SOQL query and returns all its rows (one call; more only if a page isn't enough).</summary>
    Task<List<JsonElement>> SelectAsync(string soql, CancellationToken ct);

    /// <summary>
    /// Creates or updates up to 200 records of one object, each matched by its value of <paramref name="externalIdField"/>:
    /// one result per record, in order. Every record succeeds or fails on its own.
    /// </summary>
    Task<IReadOnlyList<RecordResult>> UpsertAsync(string sobject, string externalIdField, IReadOnlyList<SObjectBody> records,
        CancellationToken ct);

    /// <summary>Deletes up to 200 records, of any objects, by their Salesforce Ids: one result per record, in order.</summary>
    Task<IReadOnlyList<RecordResult>> DeleteAsync(IReadOnlyList<string> ids, CancellationToken ct);
}
