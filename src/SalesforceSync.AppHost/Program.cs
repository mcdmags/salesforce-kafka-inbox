var builder = DistributedApplication.CreateBuilder(args);

// --AppHost:TestMode=true runs a minimal app: no persistent volume, admin UIs, load generator or benchmarks.
// (The integration tests don't use Aspire; they run on Testcontainers.)
var testMode = bool.TryParse(builder.Configuration["AppHost:TestMode"], out var t) && t;

// Infrastructure
var postgres = builder.AddPostgres("postgres");
if (!testMode)
{
    postgres.WithDataVolume().WithPgAdmin();
}

var inboxDb = postgres.AddDatabase("inboxdb");

var kafka = builder.AddKafka("kafka");
if (!testMode)
{
    kafka.WithKafkaUI();
}

// Describe results shared by every salesforce-sync replica (ConnectionStrings:redis)
var redis = builder.AddRedis("redis");
if (!testMode)
{
    redis.WithRedisInsight();
}

// Kafka -> Postgres inbox -> Salesforce, through the real OAuth + REST client
var sync = builder.AddProject<Projects.SalesforceSync>("salesforce-sync")
    .WithReference(inboxDb).WaitFor(inboxDb)
    .WithReference(kafka).WaitFor(kafka)
    .WithReference(redis).WaitFor(redis)
    .WithEnvironment("Salesforce__UseFake", "false")
    .WithEnvironment("Sync__Workers", "4");

// A real org (a sandbox) when the AppHost's user secrets hold Salesforce:InstanceUrl, ClientId and ClientSecret
// (README: Using a real Salesforce org); otherwise the mock org
var realOrg = builder.Configuration.GetSection("Salesforce");
var useRealOrg = !string.IsNullOrEmpty(realOrg["ClientSecret"]);
if (useRealOrg)
{
    var instanceUrl = realOrg["InstanceUrl"]!.TrimEnd('/');
    sync.WithEnvironment("Salesforce__InstanceUrl", instanceUrl)
        .WithEnvironment("Salesforce__TokenUrl", $"{instanceUrl}/services/oauth2/token")
        .WithEnvironment("Salesforce__ClientId", realOrg["ClientId"])
        .WithEnvironment("Salesforce__ClientSecret", builder.AddParameter("salesforce-client-secret", realOrg["ClientSecret"]!, secret: true))
        .WithEnvironment("SalesforceLimits__DailyApiAllocation", builder.Configuration["SalesforceLimits:DailyApiAllocation"] ?? "115000");
}
else
{
    // Mock Salesforce org (HTTP service; inspect it at /__fake/stats and /__fake/records/{sobject})
    var salesforce = builder.AddProject<Projects.SalesforceSync_FakeSalesforce_Server>("fake-salesforce")
        .WithEnvironment("FakeSalesforce__Latency", testMode ? "00:00:00" : "00:00:00.050")
        // Matches the worker's SalesforceLimits:DailyApiAllocation
        .WithEnvironment("FakeSalesforce__DailyApiLimit", "115000")
        .WithHttpHealthCheck("/health");
    var salesforceUrl = salesforce.GetEndpoint("http");
    sync.WaitFor(salesforce)
        .WithEnvironment("Salesforce__InstanceUrl", salesforceUrl)
        .WithEnvironment("Salesforce__TokenUrl", ReferenceExpression.Create($"{salesforceUrl}/services/oauth2/token"))
        .WithEnvironment("Salesforce__ClientId", "fake-client")
        .WithEnvironment("Salesforce__ClientSecret", "fake-secret");
}

if (!testMode)
{
    // Produces account change events (bursts included) once the sync worker is running
    var loadGenerator = builder.AddProject<Projects.SalesforceSync_LoadGenerator>("load-generator")
        .WithReference(kafka).WaitFor(kafka)
        .WaitFor(sync);
    if (useRealOrg)
    {
        // A real org's API calls count against its daily allocation: a small run
        loadGenerator.WithEnvironment("LoadGenerator__Accounts", "20")
            .WithEnvironment("LoadGenerator__TotalEvents", "200")
            .WithEnvironment("LoadGenerator__EventsPerSecond", "5");
    }

    // Benchmarks against the same Postgres (separate DB so they never touch the live inbox); start from the dashboard
    var benchDb = postgres.AddDatabase("benchdb");
    builder.AddProject<Projects.SalesforceSync_Benchmarks>("benchmarks")
        .WithReference(benchDb).WaitFor(benchDb)
        .WithArgs("--filter", "*InboxDrain*", "*StreamingIngest*")
        .WithExplicitStart();
}

builder.Build().Run();