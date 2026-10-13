using System.Net;
using Microsoft.Extensions.Options;
using SalesforceSync.Data;
using SalesforceSync.FakeSalesforce;
using SalesforceSync.Inbox;
using SalesforceSync.Salesforce;
using SalesforceSync.Sync;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddOptions<SalesforceOptions>()
    .Bind(builder.Configuration.GetSection(SalesforceOptions.Section))
    .Validate(o => Uri.IsWellFormedUriString(o.InstanceUrl, UriKind.Absolute),
        $"{SalesforceOptions.Section}:InstanceUrl must be an absolute URL")
    .ValidateOnStart();
builder.Services.AddOptions<SyncOptions>()
    .Bind(builder.Configuration.GetSection(SyncOptions.Section))
    .ValidateOnStart();
builder.Services.AddSingleton<IValidateOptions<SyncOptions>, SyncOptionsValidator>();
builder.Services.Configure<FakeSalesforceOptions>(builder.Configuration.GetSection(FakeSalesforceOptions.Section));
builder.Services.Configure<PostgresRetryOptions>(builder.Configuration.GetSection(PostgresRetryOptions.Section));

// Postgres (ConnectionStrings:inboxdb, injected by Aspire or appsettings)
builder.AddNpgsqlDataSource("inboxdb");
builder.Services.AddSingleton<PostgresExecutionStrategy>(); // every unit of work retries transient Postgres errors
builder.Services.AddHostedService<InboxSchemaInitializer>(); // must be registered before the workers

// Salesforce client. Handler order: gzip -> auth.
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton<SalesforceSchemaCache>();   // Describe results, shared by all workers in the process
builder.Services.AddSingleton<WriteEchoTracker>();
builder.Services.AddSingleton<SalesforceErrors>();
builder.Services.AddTransient<GzipRequestHandler>();
builder.Services.AddTransient<SalesforceRecordUpserter>();

var salesforce = builder.Services.AddHttpClient<SalesforceRestClient>((sp, c) =>
    {
        var o = sp.GetRequiredService<IOptions<SalesforceOptions>>().Value;
        c.BaseAddress = new Uri(o.InstanceUrl);
        // Identifies the pod/process in Salesforce API logs (and in the mock org's per-client counters)
        c.DefaultRequestHeaders.UserAgent.ParseAdd($"SalesforceSync/1.0 ({Environment.MachineName}; pid {Environment.ProcessId})");
    })
    .AddHttpMessageHandler<GzipRequestHandler>();

if (builder.Configuration.GetValue<bool>("Salesforce:UseFake"))
{
    // In-memory org: the real client and inbox are exercised; only the HTTP endpoint is fake.
    builder.Services.AddSingleton(sp => new FakeSalesforceStore(sp.GetRequiredService<IOptions<FakeSalesforceOptions>>().Value));
    salesforce.ConfigurePrimaryHttpMessageHandler(sp => new FakeSalesforceHandler(sp.GetRequiredService<FakeSalesforceStore>()));
    builder.Services.AddHostedService<FakeStatsReporter>();
}
else
{
    builder.Services.AddHttpClient(SalesforceTokenProvider.HttpClientName);
    builder.Services.AddSingleton<SalesforceTokenProvider>();
    builder.Services.AddTransient<SalesforceAuthHandler>();
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
if (!string.IsNullOrWhiteSpace(builder.Configuration.GetConnectionString("kafka")))
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

// Inbox -> Salesforce: one engine, one strategy per synced object
builder.Services.AddTransient<SalesforceBatchUpsertService>();
builder.Services.AddTransient<ISalesforceEntitySync, AccountSyncService>();
builder.Services.AddTransient<ISalesforceEntitySync, AccountRelationshipSyncService>();
builder.Services.AddHostedService<InboxWorkerService>();

builder.Build().Run();
