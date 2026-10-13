using Microsoft.Extensions.Options;
using SalesforceSync.Data;
using SalesforceSync.Salesforce;
using SalesforceSync.Sync;

namespace SalesforceSync;

/// <summary>
/// The sync's service registrations, shared by the worker (Program.cs) and the benchmarks so both run the same
/// pipeline. The host adds what differs per environment: options binding, the Postgres data source, Kafka, the
/// hosted services and the primary HTTP handler (real Salesforce with auth, or the mock org).
/// </summary>
public static class SyncServiceCollectionExtensions
{
    /// <summary>
    /// Salesforce REST client and what it needs (Describe cache, diff, error classification).
    /// Handler order: gzip -> (auth and primary handler, added by the caller).
    /// </summary>
    public static IHttpClientBuilder AddSalesforceClient(this IServiceCollection services)
    {
        services.AddSingleton(TimeProvider.System);
        services.AddSingleton<SalesforceSchemaCache>(); // Describe results, shared by all workers in the process
        services.AddSingleton<WriteEchoTracker>();
        services.AddSingleton<SalesforceErrors>();
        services.AddTransient<GzipRequestHandler>();
        services.AddTransient<SalesforceRecordUpserter>();

        return services.AddHttpClient<SalesforceRestClient>((sp, c) =>
            {
                var o = sp.GetRequiredService<IOptions<SalesforceOptions>>().Value;
                c.BaseAddress = new Uri(o.InstanceUrl);
                // Identifies the pod/process in Salesforce API logs (and in the mock org's per-client counters)
                c.DefaultRequestHeaders.UserAgent.ParseAdd($"SalesforceSync/1.0 ({Environment.MachineName}; pid {Environment.ProcessId})");
            })
            .AddHttpMessageHandler<GzipRequestHandler>();
    }

    /// <summary>
    /// Inbox -> Salesforce: the Postgres execution strategy, one engine, and one strategy per synced object.
    /// Register a new object's <see cref="ISalesforceEntitySync"/> here and the worker loops pick it up.
    /// </summary>
    public static IServiceCollection AddInboxSync(this IServiceCollection services)
    {
        services.AddSingleton<PostgresExecutionStrategy>(); // every unit of work retries transient Postgres errors
        services.AddTransient<SalesforceBatchUpsertService>();
        services.AddTransient<ISalesforceEntitySync, AccountSyncService>();
        services.AddTransient<ISalesforceEntitySync, AccountRelationshipSyncService>();
        return services;
    }
}
