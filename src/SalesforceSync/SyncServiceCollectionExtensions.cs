using System.Net;
using Microsoft.Extensions.Options;
using SalesforceSync.Data;
using SalesforceSync.FakeSalesforce;
using SalesforceSync.Inbox;
using SalesforceSync.Salesforce;
using SalesforceSync.Sync;

namespace SalesforceSync;

/// <summary>
/// The sync's service registrations. <see cref="AddSalesforceSyncWorker"/> is the whole worker (Program.cs and the
/// integration tests' in-process pods use it, so both run the identical wiring); <see cref="AddSalesforceClient"/>
/// and <see cref="AddInboxSync"/> are the pipeline itself, which the benchmarks also use with their own options,
/// data source and primary HTTP handler.
/// </summary>
public static class SyncServiceCollectionExtensions
{
    /// <summary>
    /// The worker: options (validated at startup), the Postgres data source (<c>ConnectionStrings:inboxdb</c>), the
    /// inbox schema, the Salesforce client (real OAuth endpoint, or the in-memory org with
    /// <c>Salesforce:UseFake</c>), the Kafka consumer (when <c>ConnectionStrings:kafka</c> is set) and the worker loops.
    /// </summary>
    public static IHostApplicationBuilder AddSalesforceSyncWorker(this IHostApplicationBuilder builder)
    {
        var services = builder.Services;
        var config = builder.Configuration;

        services.AddOptions<SalesforceOptions>()
            .Bind(config.GetSection(SalesforceOptions.Section))
            .Validate(o => Uri.IsWellFormedUriString(o.InstanceUrl, UriKind.Absolute),
                $"{SalesforceOptions.Section}:InstanceUrl must be an absolute URL")
            .ValidateOnStart();
        services.AddOptions<SyncOptions>()
            .Bind(config.GetSection(SyncOptions.Section))
            .ValidateOnStart();
        services.AddSingleton<IValidateOptions<SyncOptions>, SyncOptionsValidator>();
        services.Configure<FakeSalesforceOptions>(config.GetSection(FakeSalesforceOptions.Section));
        services.Configure<PostgresRetryOptions>(config.GetSection(PostgresRetryOptions.Section));

        // Postgres (ConnectionStrings:inboxdb, injected by Aspire, appsettings or the tests)
        builder.AddNpgsqlDataSource("inboxdb");
        services.AddHostedService<InboxSchemaInitializer>(); // must be registered before the workers

        // Salesforce client. Handler order: gzip -> auth.
        var salesforce = services.AddSalesforceClient();

        if (config.GetValue<bool>("Salesforce:UseFake"))
        {
            // In-memory org: the real client and inbox are exercised; only the HTTP endpoint is fake.
            services.AddSingleton(sp => new FakeSalesforceStore(sp.GetRequiredService<IOptions<FakeSalesforceOptions>>().Value));
            salesforce.ConfigurePrimaryHttpMessageHandler(sp => new FakeSalesforceHandler(sp.GetRequiredService<FakeSalesforceStore>()));
            services.AddHostedService<FakeStatsReporter>();
        }
        else
        {
            services.AddHttpClient(SalesforceTokenProvider.HttpClientName);
            services.AddSingleton<SalesforceTokenProvider>();
            services.AddTransient<SalesforceAuthHandler>();
            salesforce.AddHttpMessageHandler<SalesforceAuthHandler>()
                // Long-lived pooled connections (recycled so DNS changes are picked up) instead of handler rotation;
                // responses are compressed by Salesforce when we accept it
                .ConfigurePrimaryHttpMessageHandler(() => new SocketsHttpHandler
                {
                    AutomaticDecompression = DecompressionMethods.All,
                    PooledConnectionLifetime = TimeSpan.FromMinutes(5)
                })
                .SetHandlerLifetime(Timeout.InfiniteTimeSpan);
        }

        // Kafka -> inbox (only when a Kafka connection string is configured)
        if (!string.IsNullOrWhiteSpace(config.GetConnectionString("kafka")))
        {
            builder.AddKafkaConsumer<string, string>("kafka", settings =>
            {
                // At-least-once without a broker round trip per message: KafkaInboxConsumer stores an offset only after
                // its row is in the inbox, and the client commits stored offsets in the background (and on close).
                settings.Config.EnableAutoCommit = true;
                settings.Config.EnableAutoOffsetStore = false;
            });
            services.AddHostedService<KafkaInboxConsumer>();
        }

        // Inbox -> Salesforce: one engine, one strategy per synced object
        services.AddInboxSync();
        services.AddHostedService<InboxWorkerService>();

        return builder;
    }

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
                c.DefaultRequestHeaders.UserAgent.ParseAdd(
                    $"SalesforceSync/1.0 ({o.UserAgentInstance ?? $"{Environment.MachineName}; pid {Environment.ProcessId}"})");
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
