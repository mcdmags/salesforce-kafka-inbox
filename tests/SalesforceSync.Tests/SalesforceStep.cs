using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using SalesforceSync.Inbox;
using SalesforceSync.Salesforce;
using SalesforceSync.Sync;

namespace SalesforceSync.Tests;

/// <summary>
/// The engine wired for its Salesforce step alone (<see cref="SalesforceBatchUpsertService.WriteToSalesforceAsync"/>:
/// describe, read, decide, send, interpret): a real client on the given HTTP stack, its own caches, and no database.
/// </summary>
internal static class SalesforceStep
{
    private const string OrgUrl = "https://fake.my.salesforce.com";

    /// <param name="http">The HTTP stack to the org (see <see cref="Http"/>).</param>
    /// <param name="options">The Salesforce options of the engine and its error classification.</param>
    public static SalesforceBatchUpsertService Service(HttpClient http, IOptions<SalesforceOptions> options)
    {
        return new(new SalesforceRestClient(http), SchemaCache(),
                new SalesforceErrors(options), options, Options.Create(new SyncOptions()),
                NullLogger<SalesforceBatchUpsertService>.Instance);
    }

    /// <summary>A Describe cache of its own; with <paramref name="shared"/>, a pod sharing that cache with others.</summary>
    public static SalesforceSchemaCache SchemaCache(IDistributedCache? shared = null, TimeProvider? clock = null)
    {
        return new(clock ?? TimeProvider.System, shared ?? new MemoryDistributedCache(Options.Create(new MemoryDistributedCacheOptions())),
            Options.Create(new SalesforceOptions { InstanceUrl = OrgUrl }),
            NullLogger<SalesforceSchemaCache>.Instance);
    }

    /// <summary>A client of the org through <paramref name="handler"/>, at the address the caches are keyed by.</summary>
    public static HttpClient Http(HttpMessageHandler handler)
    {
        return new(handler) { BaseAddress = new Uri(OrgUrl) };
    }
}