// The dashboard prompt of the benchmarks' "Run benchmarks..." command (IInteractionService) is still experimental
#pragma warning disable ASPIREINTERACTION001

using Microsoft.Extensions.DependencyInjection;

var builder = DistributedApplication.CreateBuilder(args);

// --AppHost:TestMode=true runs a minimal app: no persistent volume, admin UIs, load generator or benchmarks.
// (The integration tests don't use Aspire; they run on Testcontainers.)
var testMode = bool.TryParse(builder.Configuration["AppHost:TestMode"], out var t) && t;

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

// Record type cache shared by every salesforce-sync replica (ConnectionStrings:redis)
var redis = builder.AddRedis("redis");
if (!testMode)
{
    redis.WithRedisInsight();
}

// The deploy pipeline's migrate step: runs once, then exits; the sync waits for it to complete
var migrate = builder.AddProject<Projects.SalesforceSync>("migrate-database")
    .WithReference(inboxDb).WaitFor(inboxDb)
    .WithArgs("--migrate");

// Kafka -> Postgres inbox -> Salesforce, through the real OAuth + REST client
var sync = builder.AddProject<Projects.SalesforceSync>("salesforce-sync")
    .WithReference(inboxDb).WaitFor(inboxDb).WaitForCompletion(migrate)
    .WithReference(kafka).WaitFor(kafka)
    .WithReference(redis).WaitFor(redis)
    .WithEnvironment("Sync__Workers", "4");

// Real org (sandbox) when the AppHost's user secrets hold Salesforce:InstanceUrl, ClientId and ClientSecret
// (README: Using a real Salesforce org); else the mock org, which accepts any credentials. The worker gets the same
// settings either way; only Salesforce:InstanceUrl differs.
var realOrg = builder.Configuration.GetSection("Salesforce");
// The daily API limit the mock org reports in Sforce-Limit-Info (a real org reports its own)
const string MockDailyApiLimit = "115000";
var useRealOrg = !string.IsNullOrEmpty(realOrg["ClientSecret"]);
sync.WithEnvironment("Salesforce__ClientId", realOrg["ClientId"] ?? "fake-client")
    .WithEnvironment("Salesforce__ClientSecret",
        builder.AddParameter("salesforce-client-secret", realOrg["ClientSecret"] ?? "fake-secret", secret: true));
if (useRealOrg)
{
    sync.WithEnvironment("Salesforce__InstanceUrl", realOrg["InstanceUrl"]);
}
else
{
    // Mock Salesforce org (HTTP service; inspect it at /__fake/stats and /__fake/records/{sobject})
    var salesforce = builder.AddProject<Projects.SalesforceSync_FakeSalesforce_Server>("fake-salesforce")
        .WithEnvironment("FakeSalesforce__Latency", testMode ? "00:00:00" : "00:00:00.050")
        .WithEnvironment("FakeSalesforce__DailyApiLimit", MockDailyApiLimit)
        .WithHttpHealthCheck("/health");
    sync.WaitFor(salesforce)
        .WithEnvironment("Salesforce__InstanceUrl", salesforce.GetEndpoint("http"));
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

    // Benchmarks on the same Postgres (separate DB, never touches the live inbox). Start from the dashboard with
    // "Run benchmarks...", which asks which run (quick, drain or full); ▶ repeats the last one.
    var benchDb = postgres.AddDatabase("benchdb");
    var benchmarkRun = "quick";
    builder.AddProject<Projects.SalesforceSync_Benchmarks>("benchmarks")
        .WithReference(benchDb).WaitFor(benchDb)
        .WithArgs(c =>
        {
            c.Args.Add("--run");
            c.Args.Add(benchmarkRun);
        })
        .WithExplicitStart()
        .WithCommand("run-benchmarks", "Run benchmarks...", async c =>
        {
            var choice = await c.Services.GetRequiredService<IInteractionService>().PromptInputAsync(
                "Run benchmarks", "Which run? Times are for an idle machine.",
                new InteractionInput
                {
                    Name = "run",
                    Label = "Run",
                    InputType = InputType.Choice,
                    Required = true,
                    Value = benchmarkRun,
                    Options = [
                        new("quick", "Quick: 5 drain cases, 3 iterations (about 2 minutes)"),
                        new("drain", "Drain: every drain and streaming case (about 20 minutes)"),
                        new("full", "Full: drain, streaming and micro benchmarks (about 30 minutes)")
                    ]
                },
                cancellationToken: c.CancellationToken);
            if (choice.Canceled)
            {
                return CommandResults.Canceled();
            }
            benchmarkRun = choice.Data.Value!;
            return await c.Services.GetRequiredService<ResourceCommandService>()
                .ExecuteCommandAsync(c.ResourceName, KnownResourceCommands.StartCommand, c.CancellationToken);
        }, new CommandOptions
        {
            IconName = "Play",
            IsHighlighted = true,
            // Not while a run is in progress: stop it first
            UpdateState = c => c.ResourceSnapshot.State?.Text == KnownResourceStates.Running
                ? ResourceCommandState.Disabled
                : ResourceCommandState.Enabled
        });
}

builder.Build().Run();