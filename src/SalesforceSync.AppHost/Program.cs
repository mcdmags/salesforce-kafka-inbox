var builder = DistributedApplication.CreateBuilder(args);

// --AppHost:TestMode=true runs a minimal app: no persistent volume, admin UIs, load generator or benchmarks.
// (The integration tests don't use Aspire; they run on Testcontainers.)
var testMode = bool.TryParse(builder.Configuration["AppHost:TestMode"], out var t) && t;

// Infrastructure
var postgres = builder.AddPostgres("postgres");
if (!testMode) postgres.WithDataVolume().WithPgAdmin();
var inboxDb = postgres.AddDatabase("inboxdb");

var kafka = builder.AddKafka("kafka");
if (!testMode) kafka.WithKafkaUI();

// Mock Salesforce org (HTTP service; inspect it at /__fake/stats and /__fake/records/{sobject})
var salesforce = builder.AddProject<Projects.SalesforceSync_FakeSalesforce_Server>("fake-salesforce")
    .WithEnvironment("FakeSalesforce__Latency", testMode ? "00:00:00" : "00:00:00.050")
    // Matches the worker's SalesforceLimits (Enterprise, 15 Salesforce licenses: 100,000 + 15 x 1,000)
    .WithEnvironment("FakeSalesforce__DailyApiLimit", "115000")
    .WithHttpHealthCheck("/health");
var salesforceUrl = salesforce.GetEndpoint("http");

// Kafka -> Postgres inbox -> Salesforce (the real OAuth + REST client, pointed at the mock)
var sync = builder.AddProject<Projects.SalesforceSync>("salesforce-sync")
    .WithReference(inboxDb).WaitFor(inboxDb)
    .WithReference(kafka).WaitFor(kafka)
    .WaitFor(salesforce)
    .WithEnvironment("Salesforce__UseFake", "false")
    .WithEnvironment("Salesforce__InstanceUrl", salesforceUrl)
    .WithEnvironment("Salesforce__TokenUrl", ReferenceExpression.Create($"{salesforceUrl}/services/oauth2/token"))
    .WithEnvironment("Salesforce__ClientId", "fake-client")
    .WithEnvironment("Salesforce__ClientSecret", "fake-secret")
    .WithEnvironment("Sync__Workers", "4");

if (!testMode)
{
    // Produces account change events (bursts included) once the sync worker is running
    builder.AddProject<Projects.SalesforceSync_LoadGenerator>("load-generator")
        .WithReference(kafka).WaitFor(kafka)
        .WaitFor(sync);

    // Benchmarks against the same Postgres (separate DB so they never touch the live inbox); start from the dashboard
    var benchDb = postgres.AddDatabase("benchdb");
    builder.AddProject<Projects.SalesforceSync_Benchmarks>("benchmarks")
        .WithReference(benchDb).WaitFor(benchDb)
        .WithArgs("--filter", "*InboxDrain*", "*StreamingIngest*")
        .WithExplicitStart();
}

builder.Build().Run();
