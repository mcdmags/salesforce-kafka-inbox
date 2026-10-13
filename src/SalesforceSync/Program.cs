using Microsoft.Extensions.Options;
using SalesforceSync.FakeSalesforce;
using SalesforceSync.Inbox;
using SalesforceSync.RateLimiting;
using SalesforceSync.Salesforce;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.Configure<SalesforceOptions>(builder.Configuration.GetSection(SalesforceOptions.Section));
builder.Services.Configure<SyncOptions>(builder.Configuration.GetSection(SyncOptions.Section));
builder.Services.Configure<RateLimitOptions>(builder.Configuration.GetSection(RateLimitOptions.Section));
builder.Services.AddOptions<SalesforceLimitsOptions>()
    .Bind(builder.Configuration.GetSection(SalesforceLimitsOptions.Section))
    .ValidateOnStart();
builder.Services.AddSingleton<IValidateOptions<SalesforceLimitsOptions>, SalesforceLimitsValidator>();
builder.Services.Configure<FakeSalesforceOptions>(builder.Configuration.GetSection(FakeSalesforceOptions.Section));

// Postgres (ConnectionStrings:inboxdb, injected by Aspire or appsettings)
builder.AddNpgsqlDataSource("inboxdb");
builder.Services.AddHostedService<InboxSchemaInitializer>(); // must be registered before the workers

// Rate limiting (shared through Postgres)
builder.Services.AddSingleton<SalesforceRateLimiter>();
builder.Services.AddHostedService<RateLimitInitializer>(); // after the schema, before the workers
builder.Services.AddSingleton(sp => new SalesforceConcurrencySlots(
    sp.GetRequiredService<Npgsql.NpgsqlDataSource>(),
    sp.GetRequiredService<IOptions<SyncOptions>>().Value.MaxConcurrentRequests));
builder.Services.AddTransient<SalesforceRateLimitHandler>();

// Salesforce client: rate limiter is outermost so every attempt costs a token
var salesforce = builder.Services.AddHttpClient<AccountProfileSync>((sp, c) =>
    {
        var o = sp.GetRequiredService<IOptions<SalesforceOptions>>().Value;
        c.BaseAddress = new Uri(o.InstanceUrl);
        c.Timeout = sp.GetRequiredService<IOptions<SalesforceLimitsOptions>>().Value.RequestTimeout; // includes MaxWait
        // Identifies the pod/process in Salesforce API logs (and in the mock org's per-client counters)
        c.DefaultRequestHeaders.UserAgent.ParseAdd($"SalesforceSync/1.0 ({Environment.MachineName}; pid {Environment.ProcessId})");
    })
    .AddHttpMessageHandler<SalesforceRateLimitHandler>();

if (builder.Configuration.GetValue<bool>("Salesforce:UseFake"))
{
    // In-memory org: the real client, rate limiter and inbox are exercised; only the HTTP endpoint is fake.
    builder.Services.AddSingleton(sp => new FakeSalesforceStore(sp.GetRequiredService<IOptions<FakeSalesforceOptions>>().Value));
    salesforce.ConfigurePrimaryHttpMessageHandler(sp => new FakeSalesforceHandler(sp.GetRequiredService<FakeSalesforceStore>()));
    builder.Services.AddHostedService<FakeStatsReporter>();
}
else
{
    builder.Services.AddHttpClient(SalesforceTokenProvider.HttpClientName);
    builder.Services.AddSingleton<SalesforceTokenProvider>();
    builder.Services.AddTransient<SalesforceAuthHandler>();
    salesforce.AddHttpMessageHandler<SalesforceAuthHandler>();
}

// Kafka -> inbox (only when a Kafka connection string is configured)
if (!string.IsNullOrWhiteSpace(builder.Configuration.GetConnectionString("kafka")))
{
    builder.AddKafkaConsumer<string, string>("kafka");
    builder.Services.AddHostedService<KafkaInboxConsumer>();
}

// Inbox -> Salesforce
builder.Services.AddTransient<InboxWorker>();
builder.Services.AddHostedService<InboxWorkerService>();

builder.Build().Run();
