using System.Net;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Npgsql;
using SalesforceSync.Data;
using SalesforceSync.Inbox;
using SalesforceSync.RateLimiting;
using SalesforceSync.Salesforce;
using SalesforceSync.Sync;

namespace SalesforceSync;

/// <summary>
/// The sync's service registrations. <see cref="AddSalesforceSyncWorker"/> is the whole worker (Program.cs and the
/// integration tests' in-process pods use it, so both run the identical wiring); <see cref="AddSalesforceClient"/>,
/// <see cref="AddSalesforceRateLimiting"/> and <see cref="AddInboxSync"/> are the pipeline itself, which the benchmarks
/// also use with their own options, data source and primary HTTP handler.
/// </summary>
public static class SyncServiceCollectionExtensions
{
    /// <summary>
    /// The worker: options (validated at startup), the Postgres data source (<c>ConnectionStrings:inboxdb</c>), the
    /// database schema (migrated at startup), the rate-limited Salesforce client (OAuth and REST over HTTP to <c>Salesforce:InstanceUrl</c>, a real org or
    /// the mock one) with its Describe cache shared through Redis (when <c>ConnectionStrings:redis</c> is
    /// set), the Kafka consumer (when <c>ConnectionStrings:kafka</c> is set) and the worker loops.
    /// </summary>
    public static IHostApplicationBuilder AddSalesforceSyncWorker(this IHostApplicationBuilder builder)
    {
        var services = builder.Services;
        var config = builder.Configuration;

        AddWorkerOptions(services, config);

        // Postgres (ConnectionStrings:inboxdb, injected by Aspire, appsettings or the tests)
        builder.AddNpgsqlDataSource("inboxdb");
        services.AddHostedService<SyncSchemaInitializer>(); // migrates the database; must be registered before the workers

        // Describe results shared by all pods (ConnectionStrings:redis, injected by Aspire); without it each process
        // keeps its own (AddSalesforceClient falls back to an in-memory IDistributedCache)
        if (!string.IsNullOrWhiteSpace(config.GetConnectionString("redis")))
        {
            builder.AddRedisDistributedCache("redis");
        }

        // Salesforce client, rate limited through Postgres (all pods share one bucket, budget and pause).
        // Handler order: rate limiter (outermost, so every attempt costs a token) -> gzip -> auth.
        var salesforce = services.AddSalesforceClient()
            .AddSalesforceRateLimiting(); // its initializer runs after the schema's, before the workers
        UseSalesforceOrg(salesforce);

        // Kafka -> inbox (only when a Kafka connection string is configured)
        if (!string.IsNullOrWhiteSpace(config.GetConnectionString("kafka")))
        {
            AddKafkaIngest(builder);
        }

        // Inbox -> Salesforce: one engine, one strategy per synced object
        services.AddInboxSync();
        services.AddHostedService<InboxWorkerService>();

        return builder;
    }

    private static void AddWorkerOptions(IServiceCollection services, IConfiguration config)
    {
        services.AddOptions<SalesforceOptions>()
            .Bind(config.GetSection(SalesforceOptions.Section))
            .PostConfigure(o =>
            {
                if (string.IsNullOrWhiteSpace(o.TokenUrl))
                {
                    o.TokenUrl = $"{o.InstanceUrl.TrimEnd('/')}/services/oauth2/token";
                }
            })
            .Validate(o => Uri.IsWellFormedUriString(o.InstanceUrl, UriKind.Absolute),
                $"{SalesforceOptions.Section}:InstanceUrl must be an absolute URL")
            .Validate(o => Uri.IsWellFormedUriString(o.TokenUrl, UriKind.Absolute),
                $"{SalesforceOptions.Section}:TokenUrl must be an absolute URL (or empty)")
            .ValidateOnStart();
        AddValidatedOptions<SyncOptions, SyncOptionsValidator>(services, config.GetSection(SyncOptions.Section));
        AddValidatedOptions<SalesforceLimitsOptions, SalesforceLimitsValidator>(services, config.GetSection(SalesforceLimitsOptions.Section));
        services.Configure<RateLimitOptions>(config.GetSection(RateLimitOptions.Section));
        services.Configure<PostgresRetryOptions>(config.GetSection(PostgresRetryOptions.Section));
    }

    /// <summary>Options bound to a section and checked by <typeparamref name="TValidator"/> at startup.</summary>
    private static void AddValidatedOptions<TOptions, TValidator>(IServiceCollection services, IConfigurationSection section)
        where TOptions : class
        where TValidator : class, IValidateOptions<TOptions>
    {
        services.AddOptions<TOptions>().Bind(section).ValidateOnStart();
        services.AddSingleton<IValidateOptions<TOptions>, TValidator>();
    }

    /// <summary>The org at <c>Salesforce:InstanceUrl</c>: OAuth, and one long-lived connection pool.</summary>
    private static void UseSalesforceOrg(IHttpClientBuilder salesforce)
    {
        var services = salesforce.Services;
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

    private static void AddKafkaIngest(IHostApplicationBuilder builder)
    {
        builder.AddKafkaConsumer<string, string>("kafka", settings =>
        {
            // At-least-once without a broker round trip per message: KafkaInboxConsumer stores an offset only after
            // its row is in the inbox, and the client commits stored offsets in the background (and on close).
            settings.Config.EnableAutoCommit = true;
            settings.Config.EnableAutoOffsetStore = false;
        });
        builder.Services.AddHostedService<KafkaInboxConsumer>();
    }

    /// <summary>
    /// Salesforce REST client and what the engine's Salesforce step needs (Describe cache, error classification). The
    /// Describe cache's shared layer is the registered <see cref="Microsoft.Extensions.Caching.Distributed.IDistributedCache"/>,
    /// or an in-memory one (this process only) when none is.
    /// Handler order: gzip -> (auth and primary handler, added by the caller); <see cref="AddSalesforceRateLimiting"/>
    /// puts the rate limiter in front.
    /// </summary>
    public static IHttpClientBuilder AddSalesforceClient(this IServiceCollection services)
    {
        services.AddSingleton(TimeProvider.System);
        services.AddDistributedMemoryCache(); // only if no IDistributedCache (Redis) is registered
        services.AddSingleton<SalesforceSchemaCache>(); // Describe results, shared by all workers in the process (and pods)
        services.AddSingleton<SalesforceErrors>();
        services.AddTransient<GzipRequestHandler>();

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
    /// Rate limiting shared through Postgres, for the client from <see cref="AddSalesforceClient"/>: the token bucket,
    /// daily budget and pause (<see cref="SalesforceRateLimiter"/>, configured at startup by
    /// <see cref="RateLimitInitializer"/>), the global concurrency slots (advisory locks, at most
    /// <see cref="SyncOptions.MaxConcurrentRequests"/> batches across all pods) and <see cref="SalesforceRateLimitHandler"/>,
    /// inserted as the outermost handler whatever else is added, so every attempt costs a token:
    /// rate limiter -> gzip -> auth. Register it after <see cref="SyncSchemaInitializer"/> and before the worker loops
    /// (hosted services start in registration order). <see cref="SalesforceBatchUpsertService"/> needs it.
    /// </summary>
    public static IHttpClientBuilder AddSalesforceRateLimiting(this IHttpClientBuilder salesforce)
    {
        var services = salesforce.Services;
        services.AddSingleton<SalesforceRateLimiter>();
        services.AddHostedService<RateLimitInitializer>();
        services.AddSingleton(sp => new SalesforceConcurrencySlots(
            sp.GetRequiredService<IDbContextFactory<SyncDbContext>>(),
            sp.GetRequiredService<IOptions<SyncOptions>>().Value.MaxConcurrentRequests,
            sp.GetRequiredService<ILogger<SalesforceConcurrencySlots>>()));
        services.AddSingleton<ISalesforceBatchGate, SalesforceBatchGate>();
        services.AddTransient<SalesforceRateLimitHandler>();

        return salesforce
            // A call may first wait for a token (RateLimit:MaxWait), so the timeout covers both
            .ConfigureHttpClient((sp, c) => c.Timeout = sp.GetRequiredService<IOptions<SalesforceLimitsOptions>>().Value.RequestTimeout)
            .ConfigureAdditionalHttpMessageHandlers((handlers, sp) =>
                handlers.Insert(0, sp.GetRequiredService<SalesforceRateLimitHandler>()));
    }

    /// <summary>
    /// Inbox -> Salesforce: the database context, one engine, and per synced object its sync (its strategy, topic and
    /// inbox). Register a new object's sync here (its <see cref="InboxRow"/> type is mapped in <see cref="SyncDbContext"/>
    /// and a migration creates its table): the consumer subscribes to its topic and the worker loops pick it up.
    /// </summary>
    public static IServiceCollection AddInboxSync(this IServiceCollection services)
    {
        // The database: EF contexts pooled over the app's NpgsqlDataSource; every unit of work retries transient errors
        services.AddPooledDbContextFactory<SyncDbContext>((sp, o) => SyncDbContext.Configure(o,
            sp.GetRequiredService<NpgsqlDataSource>(), sp.GetRequiredService<IOptions<PostgresRetryOptions>>().Value));
        services.AddSingleton(typeof(IRepository<>), typeof(InboxRepository<>)); // one per inbox row type
        services.AddTransient<SalesforceBatchUpsertService>();

        // The synced objects
        AddEntitySync<AccountEvent, AccountInboxRow>(services, AccountSync.Strategy, AccountSync.DefaultTopic);
        AddEntitySync<AccountRelationshipEvent, AccountRelationshipInboxRow>(services,
            AccountRelationshipSync.Strategy, AccountRelationshipSync.DefaultTopic);
        AddEntitySync<ContactEvent, ContactInboxRow>(services, ContactSync.Strategy, ContactSync.DefaultTopic);
        return services;
    }

    private static void AddEntitySync<TEvent, TRow>(IServiceCollection services, UpsertStrategy<TEvent> strategy, string defaultTopic)
        where TRow : InboxRow, IInboxRow, new()
    {
        ValidateLimits<TRow>();
        services.AddTransient<ISalesforceEntitySync>(sp => new EntitySyncService<TEvent, TRow>(
            sp.GetRequiredService<SalesforceBatchUpsertService>(), sp.GetRequiredService<IRepository<TRow>>(), strategy, defaultTopic));
    }

    /// <summary>A row type's query and write limits are within Salesforce's (<see cref="IInboxRow"/>).</summary>
    /// <exception cref="InvalidOperationException">One is not; the message names the row type and the member.</exception>
    internal static void ValidateLimits<TRow>() where TRow : IInboxRow
    {
        if (TRow.MaxValuesPerQuery is < 1 or > SalesforceRestClient.MaxQueryValues)
        {
            throw new InvalidOperationException($"{typeof(TRow).Name}.{nameof(IInboxRow.MaxValuesPerQuery)} ({TRow.MaxValuesPerQuery}) " +
                                                $"must be 1 to {SalesforceRestClient.MaxQueryValues} (a query's result page).");
        }

        if (TRow.MaxRecordsPerWrite is < 1 or > SalesforceRestClient.MaxCollectionRecords)
        {
            throw new InvalidOperationException($"{typeof(TRow).Name}.{nameof(IInboxRow.MaxRecordsPerWrite)} ({TRow.MaxRecordsPerWrite}) " +
                                                $"must be 1 to {SalesforceRestClient.MaxCollectionRecords} (records per sObject Collections upsert).");
        }
    }
}