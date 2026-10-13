using System.Net;
using Microsoft.Extensions.Options;
using SalesforceSync;
using SalesforceSync.Data;
using SalesforceSync.FakeSalesforce;
using SalesforceSync.Inbox;
using SalesforceSync.Salesforce;

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
builder.Services.AddHostedService<InboxSchemaInitializer>(); // must be registered before the workers

// Salesforce client (shared with the benchmarks). Handler order: gzip -> auth.
var salesforce = builder.Services.AddSalesforceClient();

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

// Inbox -> Salesforce: one engine, one strategy per synced object (shared with the benchmarks)
builder.Services.AddInboxSync();
builder.Services.AddHostedService<InboxWorkerService>();

builder.Build().Run();
