using SalesforceSync;

var builder = Host.CreateApplicationBuilder(args);

// --migrate: apply the database migrations and exit. The deploy pipeline (the AppHost, locally) runs this once before
// the pods start; the pods themselves never migrate.
if (args.Contains("--migrate"))
{
    await builder.MigrateAsync();
    return;
}

// Everything the worker runs (shared with the integration tests' in-process pods): options, Postgres, the rate-limited
// Salesforce client, Kafka -> inbox, inbox -> Salesforce.
builder.AddSalesforceSyncWorker();

builder.Build().Run();
