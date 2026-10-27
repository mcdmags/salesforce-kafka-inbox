using SalesforceSync.BulkTools;

// Dev-only bulk utilities; each command's arguments are its own, not configuration:
//   bulk-delete <file.json> | <object> <ids file> [--yes]   hard-deletes those records with one Bulk API job (BulkDeleteCommand)
//   bulk-delete-ids <folder> [--limit N]                    writes the Ids of the records the sync made, one JSON file per object (ListDeleteIdsCommand)
Environment.ExitCode = args switch
{
    [BulkDeleteCommand.Name, ..] => await BulkDeleteCommand.RunAsync(args[1..]),
    [ListDeleteIdsCommand.Name, ..] => await ListDeleteIdsCommand.RunAsync(args[1..]),
    _ => Usage()
};

static int Usage()
{
    Console.Error.WriteLine($"Usage: SalesforceSync.BulkTools {BulkDeleteCommand.Name} | {ListDeleteIdsCommand.Name} ...");
    return 2;
}
