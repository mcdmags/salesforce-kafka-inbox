using System.Text.Json;
using System.Text.RegularExpressions;
using SalesforceSync.Infrastructure.Salesforce;

namespace SalesforceSync.BulkTools;

/// <summary>
/// <c>SalesforceSync.BulkTools bulk-delete &lt;file.json&gt; [--yes]</c> (a <see cref="DeleteIdsFile"/>, which names the object) or
/// <c>bulk-delete &lt;object&gt; &lt;ids file&gt; [--yes]</c> (one Id per line; an <c>Id</c> header, blank lines and
/// <c>#</c> comments are skipped): hard-deletes the records with those Ids with one Bulk API 2.0 job. They
/// skip the Recycle Bin, so their storage is freed at once, and can't be restored. Uses the worker's Salesforce
/// settings; asks before deleting unless <c>--yes</c>. Exit code: 0 all deleted, 1 some failed or cancelled, 2 bad input.
/// </summary>
public static partial class BulkDeleteCommand
{
    public const string Name = "bulk-delete";

    private const string Usage = "Usage: SalesforceSync.BulkTools bulk-delete <file.json> [--yes]  or  bulk-delete <object> <file of Ids, one per line> [--yes]";

    public static async Task<int> RunAsync(string[] args)
    {
        // <file.json> names its object; a text file needs it before
        var json = args is [var first, ..] && first.EndsWith(".json", StringComparison.OrdinalIgnoreCase);
        var (sobject, file, rest) = args switch
        {
            [var f, .. var r] when json => ("", f, r),
            [var o, var f, .. var r] when !json => (o, f, r),
            _ => ("", "", new[] { "?" })
        };
        if (rest.Any(a => a != "--yes"))
        {
            Console.Error.WriteLine(Usage);
            return 2;
        }

        if (!File.Exists(file))
        {
            Console.Error.WriteLine($"No such file: {file}");
            return 2;
        }

        IEnumerable<string> lines;
        if (json)
        {
            try
            {
                var idsFile = DeleteIdsFile.Read(file);
                (sobject, lines) = (idsFile.Object, idsFile.Records.Select(r => r.Id));
            }
            catch (JsonException ex)
            {
                Console.Error.WriteLine($"Not an Ids file ({{ \"object\": ..., \"records\": [{{ \"id\": ... }}] }}): {ex.Message}");
                return 2;
            }
        }
        else
        {
            lines = File.ReadLines(file);
        }

        var (ids, invalid) = ReadIds(lines);
        if (invalid.Count > 0)
        {
            Console.Error.WriteLine($"{invalid.Count} line(s) are not Salesforce Ids (15 or 18 letters and digits), e.g.: {string.Join(", ", invalid.Take(5))}");
            return 2;
        }

        if (ids.Count == 0)
        {
            Console.Error.WriteLine($"No Ids in {file}");
            return 2;
        }

        if (!rest.Contains("--yes") && !Confirmed(sobject, ids.Count))
        {
            Console.WriteLine("Cancelled: nothing deleted.");
            return 1;
        }

        using var host = OrgCommand.Host();
        using var cancel = OrgCommand.CancelKey();
        try
        {
            var result = await host.BulkClient().HardDeleteAsync(sobject, ids, cancel.Token);
            Console.WriteLine($"Job {result.JobId}: {result.Succeeded:N0} of {ids.Count:N0} {sobject} records hard-deleted, {result.Failures.Count:N0} failed.");
            foreach (var failure in result.Failures)
            {
                Console.WriteLine($"  {failure.Id}: {failure.Error}");
            }

            return result.Failures.Count == 0 ? 0 : 1;
        }
        catch (Exception ex) when (ex is SalesforceBulkJobException or SalesforceResponseException)
        {
            Console.Error.WriteLine(ex.Message);
            return 1;
        }
        catch (OperationCanceledException) when (cancel.IsCancellationRequested)
        {
            Console.Error.WriteLine("Stopped waiting. A job already uploaded keeps running in Salesforce: see Setup → Bulk Data Load Jobs.");
            return 1;
        }
    }

    /// <summary>
    /// The file's Ids, each once, in order, and the lines that aren't Ids. Skips blank lines, <c>#</c> comments and an
    /// <c>Id</c> header; quotes around an Id (a CSV export) are removed.
    /// </summary>
    public static (List<string> Ids, List<string> Invalid) ReadIds(IEnumerable<string> lines)
    {
        var ids = new List<string>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var invalid = new List<string>();
        foreach (var line in lines.Select(l => l.Trim().Trim('"').Trim()))
        {
            if (line.Length == 0 || line.StartsWith('#') || line.Equals("Id", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (!SalesforceId().IsMatch(line))
            {
                invalid.Add(line);
            }
            else if (seen.Add(line))
            {
                ids.Add(line);
            }
        }

        return (ids, invalid);
    }

    private static bool Confirmed(string sobject, int count)
    {
        if (Console.IsInputRedirected)
        {
            Console.Error.WriteLine("Not run: confirm with --yes when input is not a terminal.");
            return false;
        }

        Console.Write($"Hard-delete {count:N0} {sobject} records? They skip the Recycle Bin and can't be restored. Type yes to go on: ");
        return Console.ReadLine()?.Trim() == "yes";
    }

    [GeneratedRegex("^[A-Za-z0-9]{15}([A-Za-z0-9]{3})?$")]
    private static partial Regex SalesforceId();
}
