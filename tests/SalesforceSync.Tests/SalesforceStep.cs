using System.Net.Http.Headers;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using SalesforceSync.Data.Inbox;
using SalesforceSync.FakeSalesforce;
using SalesforceSync.Infrastructure.RateLimiting;
using SalesforceSync.Infrastructure.Salesforce;
using SalesforceSync.Sync;

namespace SalesforceSync.Tests;

/// <summary>
/// The engine wired for its Salesforce step alone (<see cref="SalesforceBatchSyncService.WriteToSalesforceAsync"/>:
/// describe, read, decide, send, interpret): a real client on the given HTTP stack, its own caches, and no database.
/// </summary>
internal static class SalesforceStep
{
    private const string OrgUrl = "https://fake.my.salesforce.com";

    // What appsettings.json supplies in the running worker; the tests spell out the URLs these make
    private static readonly SalesforceOptions Org = new() { BaseApiPath = "/services/data", ApiVersion = "v62.0" };

    /// <summary>A client on <paramref name="http"/> with the default API path and version.</summary>
    public static SalesforceRestClient Client(HttpClient http)
    {
        return new SalesforceRestClient(http, Options.Create(Org));
    }

    /// <summary>The path of the REST calls with the default options.</summary>
    public static readonly string DataPath = Org.DataPath;

    /// <param name="http">The HTTP stack to the org (see <see cref="Http"/>).</param>
    /// <param name="options">The Salesforce options of the engine and its error classification.</param>
    /// <param name="gate">The concurrency gate, for tests that run whole batches; the Salesforce step needs none.</param>
    public static SalesforceBatchSyncService Service(HttpClient http, IOptions<SalesforceOptions> options,
        ISalesforceBatchGate? gate = null)
    {
        return new(gate ?? new NoDatabase(), Client(http), SchemaCache(),
                new SalesforceErrors(options), options, Options.Create(new SyncOptions()),
                NullLogger<SalesforceBatchSyncService>.Instance);
    }

    /// <summary>A Describe cache of its own; with <paramref name="shared"/>, a pod sharing that cache with others.</summary>
    public static SalesforceSchemaCache SchemaCache(IDistributedCache? shared = null, TimeProvider? clock = null)
    {
        return new(clock ?? TimeProvider.System, shared ?? new MemoryDistributedCache(Options.Create(new MemoryDistributedCacheOptions())),
            Options.Create(new SalesforceOptions { InstanceUrl = OrgUrl, BaseApiPath = Org.BaseApiPath, ApiVersion = Org.ApiVersion }),
            NullLogger<SalesforceSchemaCache>.Instance);
    }

    /// <summary>
    /// A client of the org through <paramref name="handler"/>, at the address the caches are keyed by, with the mock
    /// org's first access token (as after OAuth: the Salesforce step has no auth handler).
    /// </summary>
    public static HttpClient Http(HttpMessageHandler handler)
    {
        var http = new HttpClient(handler) { BaseAddress = new Uri(OrgUrl) };
        http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", FakeSalesforceStore.FirstAccessToken);
        return http;
    }

    /// <summary>The Salesforce step never takes a slot.</summary>
    private sealed class NoDatabase : ISalesforceBatchGate
    {
        public Task<IAsyncDisposable?> TryEnterAsync(CancellationToken ct)
        {
            throw new NotSupportedException("The Salesforce step does not use the inbox");
        }
    }
}