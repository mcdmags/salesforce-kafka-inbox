using System.Globalization;
using System.Net;
using BenchmarkDotNet.Reports;

namespace SalesforceSync.Benchmarks;

/// <summary>
/// index.html next to BenchmarkDotNet's HTML reports (results/): this run's reports first, then the earlier ones.
/// BenchmarkDotNet writes one report per benchmark class and overwrites it on the next run of that class, so the index
/// is the one page that says which reports came from the last run. The Aspire dashboard serves the folder and links
/// the index (AppHost, "benchmark-reports").
/// </summary>
public static class ReportIndex
{
    /// <summary>Environment variable with the index's URL, set by the AppHost; printed at the end of a run.</summary>
    public const string UrlVariable = "BENCHMARK_REPORT_URL";

    /// <returns>Where to find the index (the URL when served, else the file); null if no report was written.</returns>
    public static string? Write(IReadOnlyList<Summary> summaries, string run, DateTime startedUtc)
    {
        var folder = summaries.Select(s => s.ResultsDirectoryPath).FirstOrDefault(Directory.Exists);
        if (folder is null)
        {
            return null;
        }

        var reports = new DirectoryInfo(folder).GetFiles("*-report.html").OrderByDescending(f => f.LastWriteTimeUtc).ToList();
        // A second's slack: the file system's clock and ours may round differently
        var thisRun = reports.Where(f => f.LastWriteTimeUtc >= startedUtc.AddSeconds(-1)).ToList();
        var earlier = reports.Except(thisRun).ToList();

        var html = $"""
            <!doctype html>
            <html lang="en">
            <head>
              <meta charset="utf-8">
              <meta name="viewport" content="width=device-width, initial-scale=1">
              <title>Benchmark reports</title>
              <style>
                body {"{"} font-family: system-ui, sans-serif; margin: 2rem; max-width: 60rem; {"}"}
                li {"{"} margin: .3rem 0; {"}"}
                .when {"{"} color: #666; font-size: .9em; {"}"}
              </style>
            </head>
            <body>
              <h1>Benchmark reports</h1>
              <h2>Last run: {WebUtility.HtmlEncode(run)}, {startedUtc.ToString("yyyy-MM-dd HH:mm 'UTC'", CultureInfo.InvariantCulture)}</h2>
              {List(thisRun, "No report was written.")}
              <h2>Earlier runs</h2>
              {List(earlier, "None.")}
            </body>
            </html>
            """;
        File.WriteAllText(Path.Combine(folder, "index.html"), html);

        return Environment.GetEnvironmentVariable(UrlVariable) is { Length: > 0 } url ? url : Path.Combine(folder, "index.html");
    }

    private static string List(List<FileInfo> reports, string none)
    {
        if (reports.Count == 0)
        {
            return $"<p>{none}</p>";
        }

        var items = reports.Select(f =>
            $"""<li><a href="{Uri.EscapeDataString(f.Name)}">{WebUtility.HtmlEncode(Title(f.Name))}</a> <span class="when">{f.LastWriteTimeUtc.ToString("yyyy-MM-dd HH:mm 'UTC'", CultureInfo.InvariantCulture)}</span></li>""");
        return $"<ul>{string.Concat(items)}</ul>";
    }

    /// <summary>SalesforceSync.Benchmarks.InboxDrainBenchmarks-report.html → InboxDrainBenchmarks.</summary>
    private static string Title(string file)
    {
        var name = file[..^"-report.html".Length];
        return name[(name.LastIndexOf('.') + 1)..];
    }
}
