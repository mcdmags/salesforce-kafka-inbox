using System.Net;
using FluentValidation;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Npgsql;
using SalesforceSync.Data;
using SalesforceSync.Data.Inbox;
using SalesforceSync.Hosting;
using SalesforceSync.Inbox;
using SalesforceSync.Infrastructure.Salesforce;
using SalesforceSync.Persistence;
using SalesforceSync.Sync;
using SalesforceSync.ToSalesforce;

namespace SalesforceSync;

/// <summary>
/// Sync's service registrations. <see cref="AddSalesforceSyncWorker"/> = whole worker (Program.cs and integration
/// tests' in-process pods use it: identical wiring). <see cref="AddSalesforceClient"/>,
/// <see cref="AddInboxSync"/> = the pipeline itself; benchmarks reuse it with
/// own options, data source, primary HTTP handler.
/// </summary>
public static class SyncServiceCollectionExtensions
{
    /// <summary>
    /// The worker: options (validated at startup), Postgres data source (<c>ConnectionStrings:inboxdb</c>),
    /// Salesforce client (OAuth + REST over HTTP to <c>Salesforce:InstanceUrl</c>, real or mock org) with
    /// record type cache shared via Redis (if <c>ConnectionStrings:redis</c> set), Kafka consumer (if
    /// <c>ConnectionStrings:kafka</c> set), worker loops.
    /// </summary>
    public static IHostApplicationBuilder AddSalesforceSyncWorker(this IHostApplicationBuilder builder)
    {
        var services = builder.Services;
        var config = builder.Configuration;

        // Postgres (ConnectionStrings:inboxdb from Aspire, appsettings or tests)
        builder.AddNpgsqlDataSource("inboxdb");

        // Record type Ids shared by all pods (ConnectionStrings:redis from Aspire). Without: per-process
        // (AddSalesforceClient falls back to in-memory IDistributedCache)
        if (!string.IsNullOrWhiteSpace(config.GetConnectionString("redis")))
        {
            builder.AddRedisDistributedCache("redis");
        }

        builder.AddSalesforceOrgClient();

        // Kafka -> inbox (only if Kafka connection string set)
        if (!string.IsNullOrWhiteSpace(config.GetConnectionString("kafka")))
        {
            AddKafkaIngest(builder);
        }

        services.AddInboxSync();
        services.AddHostedService<InboxWorkerService>();

        return builder;
    }

    /// <summary>
    /// The settings (validated at startup) and the Salesforce client for the org at <c>Salesforce:InstanceUrl</c>:
    /// OAuth. The worker's, and the dev bulk tools' (SalesforceSync.BulkTools). Handler order: gzip -> auth.
    /// </summary>
    public static IHostApplicationBuilder AddSalesforceOrgClient(this IHostApplicationBuilder builder)
    {
        AddWorkerOptions(builder.Services, builder.Configuration);
        UseSalesforceOrg(builder.Services.AddSalesforceClient());
        return builder;
    }

    /// <summary>
    /// Deploy's migrate step (<c>SalesforceSync --migrate</c>): applies migrations once before pods roll out. Needs
    /// only Postgres.
    /// </summary>
    public static async Task MigrateAsync(this IHostApplicationBuilder builder)
    {
        var services = builder.Services;
        var config = builder.Configuration;
        AddValidatedOptions<PostgresRetryOptions, PostgresRetryOptionsValidator>(services, config.GetSection(PostgresRetryOptions.Section));
        AddValidatedOptions<SyncOptions, SyncOptionsValidator>(services, config.GetSection(SyncOptions.Section));
        builder.AddNpgsqlDataSource("inboxdb");
        services.AddInboxSync();
        using var host = ((HostApplicationBuilder)builder).Build();
        await SyncSchema.MigrateAsync(host.Services.GetRequiredService<IDbContextFactory<SyncDbContext>>());
    }

    private static void AddWorkerOptions(IServiceCollection services, IConfiguration config)
    {
        AddValidatedOptions<SalesforceOptions, SalesforceOptionsValidator>(services, config.GetSection(SalesforceOptions.Section))
            .PostConfigure(o =>
            {
                if (string.IsNullOrWhiteSpace(o.TokenUrl))
                {
                    o.TokenUrl = $"{o.InstanceUrl.TrimEnd('/')}/services/oauth2/token";
                }
            });
        AddValidatedOptions<SyncOptions, SyncOptionsValidator>(services, config.GetSection(SyncOptions.Section));
        AddValidatedOptions<SalesforceLimitsOptions, SalesforceLimitsValidator>(services, config.GetSection(SalesforceLimitsOptions.Section));
        AddValidatedOptions<PostgresRetryOptions, PostgresRetryOptionsValidator>(services, config.GetSection(PostgresRetryOptions.Section));
    }

    /// <summary>Options bound to a section, checked at startup by the FluentValidation validator <typeparamref name="TValidator"/>.</summary>
    private static OptionsBuilder<TOptions> AddValidatedOptions<TOptions, TValidator>(IServiceCollection services, IConfigurationSection section)
        where TOptions : class
        where TValidator : class, IValidator<TOptions>
    {
        services.AddSingleton<IValidator<TOptions>, TValidator>();
        services.AddSingleton<IValidateOptions<TOptions>, FluentOptionsValidator<TOptions>>();
        return services.AddOptions<TOptions>().Bind(section).ValidateOnStart();
    }

    /// <summary>Org at <c>Salesforce:InstanceUrl</c>: OAuth, one long-lived connection pool.</summary>
    private static void UseSalesforceOrg(IHttpClientBuilder salesforce)
    {
        var services = salesforce.Services;
        services.AddHttpClient(SalesforceTokenProvider.HttpClientName);
        services.AddSingleton<SalesforceTokenProvider>();
        services.AddTransient<SalesforceAuthHandler>();
        salesforce.AddHttpMessageHandler<SalesforceAuthHandler>()
            // Long-lived pooled connections (recycled to pick up DNS changes) instead of handler rotation.
            // Salesforce compresses responses if accepted
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
            // At-least-once, no broker round trip per message: KafkaInboxConsumer stores offset only after its row is
            // in inbox; client commits stored offsets in background (and on close).
            settings.Config.EnableAutoCommit = true;
            settings.Config.EnableAutoOffsetStore = false;
        });
        builder.Services.AddHostedService<KafkaInboxConsumer>();
    }

    /// <summary>
    /// Salesforce REST client + what the send steps need (record type cache, error classification). Cache's
    /// shared layer = registered <see cref="Microsoft.Extensions.Caching.Distributed.IDistributedCache"/>, else
    /// in-memory (this process only).
    /// Handler order: gzip -> (auth + primary handler, added by caller). Timeout per call:
    /// <c>SalesforceLimits:RequestTimeout</c>.
    /// </summary>
    public static IHttpClientBuilder AddSalesforceClient(this IServiceCollection services)
    {
        services.AddSingleton(TimeProvider.System);
        services.AddDistributedMemoryCache(); // only if no IDistributedCache (Redis) registered
        services.AddHybridCache(); // in process + in IDistributedCache (Redis) shared by pods
        services.AddSingleton<RecordTypeCache>();
        services.AddSingleton<SalesforceErrors>();
        services.AddTransient<GzipRequestHandler>();

        return services.AddHttpClient<ISalesforceRestClient, SalesforceRestClient>((sp, c) =>
            {
                var o = sp.GetRequiredService<IOptions<SalesforceOptions>>().Value;
                c.BaseAddress = new Uri(o.InstanceUrl);
                c.Timeout = sp.GetRequiredService<IOptions<SalesforceLimitsOptions>>().Value.RequestTimeout;
                // Identifies pod/process in Salesforce API logs (and mock org's per-client counters)
                c.DefaultRequestHeaders.UserAgent.ParseAdd(
                    $"SalesforceSync/1.0 ({o.UserAgentInstance ?? $"{Environment.MachineName}; pid {Environment.ProcessId}"})");
            })
            .AddHttpMessageHandler<GzipRequestHandler>();
    }

    /// <summary>
    /// Inbox -> Salesforce: DB context, the shared batch flow, per synced object its ingest + sync. Register a new
    /// object's sync here (its <see cref="InboxRow"/> type mapped in <see cref="SyncDbContext"/>, migration creates
    /// its table): consumer stores events its ingest takes from aggregates; worker loops pick it up.
    /// </summary>
    public static IServiceCollection AddInboxSync(this IServiceCollection services)
    {
        // DB: EF contexts pooled over app's NpgsqlDataSource. Every unit of work retries transient errors
        services.AddPooledDbContextFactory<SyncDbContext>((sp, o) => SyncDbContext.Configure(o,
            sp.GetRequiredService<NpgsqlDataSource>(), sp.GetRequiredService<IOptions<PostgresRetryOptions>>().Value));
        services.AddSingleton(typeof(IRepository<>), typeof(InboxRepository<>)); // one per inbox row type
        services.AddTransient<SalesforceCalls>();
        services.AddTransient<SalesforceBatchSyncService>();

        // Synced objects, parents first (a pass runs them in this order from its start type)
        AddEntitySync<AccountIngest, AccountSync>(services);
        AddEntitySync<AccountRelationshipIngest, AccountRelationshipSync>(services);
        AddEntitySync<ContactIngest, ContactSync>(services);
        services.AddSingleton<AccountAggregate>();
        return services;
    }

    private static void AddEntitySync<TIngest, TSync>(IServiceCollection services)
        where TIngest : class, IAggregateIngest
        where TSync : class, IEntitySync
    {
        services.AddSingleton<IAggregateIngest, TIngest>();
        services.AddTransient<IEntitySync, TSync>();
    }
}
