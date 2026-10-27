using SalesforceSync;
using SalesforceSync.Hosting;

// bulk-delete <file.json> | <object> <ids file> [--yes]: hard-deletes those records with one Bulk API job and exits (BulkDeleteCommand).
// Its arguments are its own, not configuration.
if (args is [BulkDeleteCommand.Name, ..])
{
    Environment.ExitCode = await BulkDeleteCommand.RunAsync(args[1..]);
    return;
}

// bulk-delete-ids <folder>: writes the Ids of the records the sync made, one JSON file per object, for bulk-delete
// (ListDeleteIdsCommand)
if (args is [ListDeleteIdsCommand.Name, ..])
{
    Environment.ExitCode = await ListDeleteIdsCommand.RunAsync(args[1..]);
    return;
}

var builder = Host.CreateApplicationBuilder(args);

// --migrate: apply the database migrations and exit. The deploy pipeline (the AppHost, locally) runs this once
// before the pods start. Pods never migrate.
if (args.Contains("--migrate"))
{
    await builder.MigrateAsync();
    return;
}

// Shared with the integration tests' in-process pods.
builder.AddSalesforceSyncWorker();

builder.Build().Run();
