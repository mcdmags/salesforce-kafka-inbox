using System.Net;
using System.Text.RegularExpressions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Npgsql;
using SalesforceSync.Data;
using SalesforceSync.Data.Inbox;
using SalesforceSync.Hosting;
using SalesforceSync.Inbox;
using SalesforceSync.Infrastructure.RateLimiting;
using SalesforceSync.Infrastructure.Salesforce;
using SalesforceSync.RateLimiting;
using SalesforceSync.Persistence;
using SalesforceSync.Sync;
using SalesforceSync.ToSalesforce;

namespace SalesforceSync;

/// <summary>
/// The sync's service registrations. <see cref="AddSalesforceSyncWorker"/> is the whole worker (Program.cs and the
/// integration tests' in-process pods use it, so both run identical wiring). <see cref="AddSalesforceClient"/>,
/// <see cref="AddSalesforceRateLimiting"/> and <see cref="AddInboxSync"/> are the pipeline itself, which the benchmarks
/// also use with their own options, data source and primary HTTP handler.
/// </summary>
public static class SyncServiceCollectionExtensions
{
    /// <summary>
    /// The worker: options (validated at startup), the Postgres data source (<c>ConnectionStrings:inboxdb</c>), the
    /// rate-limited Salesforce client (OAuth and REST over HTTP to
    /// <c>Salesforce:InstanceUrl</c>, a real org or the mock one) with its record type cache shared through Redis (when
    /// <c>ConnectionStrings:redis</c> is set), the Kafka consumer (when <c>ConnectionStrings:kafka</c> is set) and the
    /// worker loops.
    /// </summary>
    public static IHostApplicationBuilder AddSalesforceSyncWorker(this IHostApplicationBuilder builder)
    {
        var services = builder.Services;
        var config = builder.Configuration;

        AddWorkerOptions(services, config);

        // Postgres (ConnectionStrings:inboxdb, injected by Aspire, appsettings or the tests)
        builder.AddNpgsqlDataSource("inboxdb");

        // Record type Ids shared by all pods (ConnectionStrings:redis, injected by Aspire). Without it each process
        // keeps its own (AddSalesforceClient falls back to an in-memory IDistributedCache)
        if (!string.IsNullOrWhiteSpace(config.GetConnectionString("redis")))
        {
            builder.AddRedisDistributedCache("redis");
        }

        // Salesforce client, rate limited in memory (each process has its own pause).
        // Handler order: rate limiter (outermost, so every attempt waits for a pause) -> gzip -> auth.
        var salesforce = services.AddSalesforceClient()
            .AddSalesforceRateLimiting();
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

    /// <summary>
    /// The deploy's migrate step (<c>SalesforceSync --migrate</c>): applies the migrations once before the pods roll
    /// out. Needs only Postgres.
    /// </summary>
    public static async Task MigrateAsync(this IHostApplicationBuilder builder)
    {
        var services = builder.Services;
        var config = builder.Configuration;
        services.Configure<PostgresRetryOptions>(config.GetSection(PostgresRetryOptions.Section));
        AddValidatedOptions<SyncOptions, SyncOptionsValidator>(services, config.GetSection(SyncOptions.Section));
        builder.AddNpgsqlDataSource("inboxdb");
        services.AddInboxSync();
        using var host = ((HostApplicationBuilder)builder).Build();
        await SyncSchema.MigrateAsync(host.Services.GetRequiredService<IDbContextFactory<SyncDbContext>>());
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
            .Validate(o => o.BaseApiPath.StartsWith('/'),
                $"{SalesforceOptions.Section}:BaseApiPath must be set, starting with '/' (e.g. /services/data)")
            .Validate(o => Regex.IsMatch(o.ApiVersion, @"^v\d+\.\d$"),
                $"{SalesforceOptions.Section}:ApiVersion must be set, like v62.0")
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
            // Long-lived pooled connections (recycled so DNS changes are picked up) instead of handler rotation.
            // Salesforce compresses responses when we accept it
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
    /// Salesforce REST client and what the engine's Salesforce step needs (record type cache, error classification). The
    /// record type cache's shared layer is the registered <see cref="Microsoft.Extensions.Caching.Distributed.IDistributedCache"/>,
    /// or an in-memory one (this process only) when none is.
    /// Handler order: gzip -> (auth and primary handler, added by the caller); <see cref="AddSalesforceRateLimiting"/>
    /// puts the rate limiter in front.
    /// </summary>
    public static IHttpClientBuilder AddSalesforceClient(this IServiceCollection services)
    {
        services.AddSingleton(TimeProvider.System);
        services.AddDistributedMemoryCache(); // only if no IDistributedCache (Redis) is registered
        services.AddHybridCache(); // in process, and in the IDistributedCache (Redis) shared by the pods
        services.AddSingleton<RecordTypeCache>(); // record type Ids
        services.AddSingleton<SalesforceErrors>();
        services.AddTransient<GzipRequestHandler>();

        return services.AddHttpClient<ISalesforceRestClient, SalesforceRestClient>((sp, c) =>
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
    /// Rate limiting for the client from <see cref="AddSalesforceClient"/>, in memory: the process's pause, from
    /// Sforce-Limit-Info and limit errors (<see cref="SalesforceRateLimiter"/>), and <see cref="SalesforceRateLimitHandler"/>,
    /// inserted as the outermost handler whatever else is added, so every attempt waits for a pause:
    /// rate limiter -> gzip -> auth. Register it before the worker loops
    /// (hosted services start in registration order). <see cref="SalesforceBatchSyncService"/> needs it.
    /// </summary>
    public static IHttpClientBuilder AddSalesforceRateLimiting(this IHttpClientBuilder salesforce)
    {
        var services = salesforce.Services;
        services.AddSingleton<SalesforceRateLimiter>();
        services.AddSingleton<ISalesforceRateLimiter>(sp => sp.GetRequiredService<SalesforceRateLimiter>());
        services.AddTransient<SalesforceRateLimitHandler>();

        return salesforce
            // A call may first wait for a token (RateLimit:MaxWait), so the timeout covers both
            .ConfigureHttpClient((sp, c) => c.Timeout = sp.GetRequiredService<IOptions<SalesforceLimitsOptions>>().Value.RequestTimeout)
            .ConfigureAdditionalHttpMessageHandlers((handlers, sp) =>
                handlers.Insert(0, sp.GetRequiredService<SalesforceRateLimitHandler>()));
    }

    /// <summary>
    /// Inbox -> Salesforce: the database context, one engine, and per synced object its sync (its strategy and inbox).
    /// Register a new object's sync here (its <see cref="InboxRow"/> type is mapped in <see cref="SyncDbContext"/> and a
    /// migration creates its table): the consumer stores the events of its type that its ingest takes from the
    /// aggregates, and the worker loops pick it up.
    /// </summary>
    public static IServiceCollection AddInboxSync(this IServiceCollection services)
    {
        // The database: EF contexts pooled over the app's NpgsqlDataSource. Every unit of work retries transient errors
        services.AddPooledDbContextFactory<SyncDbContext>((sp, o) => SyncDbContext.Configure(o,
            sp.GetRequiredService<NpgsqlDataSource>(), sp.GetRequiredService<IOptions<PostgresRetryOptions>>().Value));
        services.AddSingleton(typeof(IRepository<>), typeof(InboxRepository<>)); // one per inbox row type
        services.AddTransient<SalesforceBatchSyncService>();

        // The synced objects, parents first. Each has two halves: its ingest, which stores what it takes from an
        // aggregate in the inbox, and its sync, which the engine sends to Salesforce
        AddEntitySync<AccountIngest, AccountSync, AccountInboxRow>(services);
        AddEntitySync<AccountRelationshipIngest, AccountRelationshipSync, AccountRelationshipInboxRow>(services);
        AddEntitySync<ContactIngest, ContactSync, ContactInboxRow>(services);
        services.AddSingleton<AccountAggregate>();
        return services;
    }

    private static void AddEntitySync<TIngest, TSync, TRow>(IServiceCollection services)
        where TIngest : class, IAggregateIngest
        where TSync : class, IEntitySync
        where TRow : InboxRow, IInboxRow, new()
    {
        ValidateLimits<TRow>();
        services.AddSingleton<IAggregateIngest, TIngest>();
        services.AddTransient<IEntitySync, TSync>();
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
            throw new InvalidOperationException(
                $"{typeof(TRow).Name}.{nameof(IInboxRow.MaxRecordsPerWrite)} ({TRow.MaxRecordsPerWrite}) " +
                $"must be 1 to {SalesforceRestClient.MaxCollectionRecords} (records per sObject Collections upsert or delete).");
        }
    }
}