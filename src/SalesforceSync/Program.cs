using SalesforceSync;

var builder = Host.CreateApplicationBuilder(args);

// Everything the worker runs (shared with the integration tests' in-process pods): options, Postgres, the rate-limited
// Salesforce client, Kafka -> inbox, inbox -> Salesforce.
builder.AddSalesforceSyncWorker();

builder.Build().Run();
