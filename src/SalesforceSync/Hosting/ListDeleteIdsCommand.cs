using SalesforceSync.Infrastructure.Salesforce;
using SalesforceSync.ToSalesforce;

namespace SalesforceSync.Hosting;

/// <summary>
/// <c>SalesforceSync bulk-delete-ids &lt;folder&gt; [--limit N]</c>: for each object the sync writes, lists the records
/// that have an external id (the ones the sync made), or with <c>--limit</c> the first N by external id, and writes them
/// to <c>&lt;folder&gt;/&lt;object&gt;.json</c> (<see cref="DeleteIdsFile"/>), ready for <see cref="BulkDeleteCommand"/>.
/// Reads only; deletes nothing.
/// Exit code: 0 written, 1 a query failed, 2 bad arguments.
/// </summary>
public static class ListDeleteIdsCommand
{
    public const string Name = "bulk-delete-ids";

    /// <summary>The objects the sync writes; children first, so files are deleted in that order if taken top down.</summary>
    private static readonly SalesforceObject[] Objects =
    [
        SalesforceObject.Of<ContactRecord>(),
        SalesforceObject.Of<AccountRelationshipRecord>(),
        SalesforceObject.Of<ProfileRecord>(),
        SalesforceObject.Of<AccountRecord>()
    ];

    public static async Task<int> RunAsync(string[] args)
    {
        // <folder> [--limit N]: with a limit, the first N records per object by external id
        var (folder, limit) = args switch
        {
            [var f] => (f, (int?)null),
            [var f, "--limit", var n] when int.TryParse(n, out var max) && max > 0 => (f, max),
            _ => (null, null)
        };
        if (folder is null)
        {
            Console.Error.WriteLine($"Usage: SalesforceSync {Name} <folder to write the JSON files to> [--limit <records per object>]");
            return 2;
        }

        Directory.CreateDirectory(folder);
        using var host = OrgCommand.Host();
        using var cancel = OrgCommand.CancelKey();
        try
        {
            foreach (var sobject in Objects)
            {
                var query = $"SELECT Id, {sobject.ExternalIdField} FROM {sobject.Name} WHERE {sobject.ExternalIdField} != null"
                            + (limit is null ? "" : $" ORDER BY {sobject.ExternalIdField} LIMIT {limit}");
                var rows = await host.Client().SelectAsync(query, cancel.Token);
                var records = rows
                    .Select(r => new DeleteIdsFile.Row(r.GetProperty("Id").GetString()!, r.GetProperty(sobject.ExternalIdField).GetString()))
                    .OrderBy(r => r.ExternalId, StringComparer.Ordinal)
                    .ToList();
                var path = Path.GetFullPath(Path.Combine(folder, $"{sobject.Name}.json"));
                new DeleteIdsFile(sobject.Name, query, records).Write(path);
                Console.WriteLine($"{sobject.Name}: {records.Count:N0} records -> {path}");
            }

            return 0;
        }
        catch (SalesforceResponseException ex)
        {
            Console.Error.WriteLine(ex.Message);
            return 1;
        }
        catch (OperationCanceledException) when (cancel.IsCancellationRequested)
        {
            Console.Error.WriteLine("Stopped.");
            return 1;
        }
    }
}
