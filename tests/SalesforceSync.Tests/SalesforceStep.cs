using System.Net.Http.Headers;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Caching.Hybrid;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using SalesforceSync.Data.Inbox;
using SalesforceSync.FakeSalesforce;
using SalesforceSync.Infrastructure.RateLimiting;
using SalesforceSync.Infrastructure.Salesforce;
using SalesforceSync.ToSalesforce;

namespace SalesforceSync.Tests;

/// <summary>
/// The parts of the Salesforce half wired for tests: a real client on the given HTTP stack, its own record type cache,
/// no database.
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

    /// <summary>The Salesforce calls on <paramref name="http"/>: no database, their own record type cache.</summary>
    public static SalesforceCalls Calls(HttpClient http, IOptions<SalesforceOptions> options)
    {
        return new(Client(http), RecordTypes(http), new SalesforceErrors(options));
    }

    /// <summary>The flow of a batch, with a rate limiter that is never paused unless told to be.</summary>
    public static SalesforceBatchSyncService Flow(ISalesforceRateLimiter? limiter = null, SyncOptions? options = null)
    {
        return new(limiter ?? new NotPaused(), Options.Create(options ?? new SyncOptions()), NullLogger<SalesforceBatchSyncService>.Instance);
    }

    /// <summary>A record type cache reading through <paramref name="http"/>; with <paramref name="shared"/>, a pod sharing that cache with others.</summary>
    public static RecordTypeCache RecordTypes(HttpClient http, IDistributedCache? shared = null)
    {
        var services = new ServiceCollection();
        services.AddSingleton(shared ?? new MemoryDistributedCache(Options.Create(new MemoryDistributedCacheOptions())));
        services.AddHybridCache();
        return new(services.BuildServiceProvider().GetRequiredService<HybridCache>(), Client(http),
            Options.Create(new SalesforceOptions { InstanceUrl = OrgUrl, BaseApiPath = Org.BaseApiPath, ApiVersion = Org.ApiVersion }));
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

    /// <summary>A limiter that is never paused and never asked for a token: the Salesforce step calls it through the client only.</summary>
    public sealed class NotPaused : ISalesforceRateLimiter
    {
        public bool IsPaused { get; set; }

        public Task AcquireAsync(CancellationToken ct)
        {
            return Task.CompletedTask;
        }

        public void Pause(TimeSpan duration)
        {
        }

        public void ReportLimits(SalesforceLimitInfo info)
        {
        }
    }
}
