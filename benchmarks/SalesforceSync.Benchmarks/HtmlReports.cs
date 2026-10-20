using System.Globalization;
using System.Net;
using System.Text.RegularExpressions;
using BenchmarkDotNet.Reports;

namespace SalesforceSync.Benchmarks;

/// <summary>
/// Makes BenchmarkDotNet's HTML reports (results/) readable without knowing BenchmarkDotNet: each report of the run gets
/// what its benchmark measures, why a benchmark failed (BenchmarkDotNet only shows NA) and what every column means.
/// Then index.html lists this run's reports first, then the earlier ones: BenchmarkDotNet writes one report per
/// benchmark class and overwrites it on the next run of that class, so the index is the one page that says which
/// reports came from the last run. The Aspire dashboard serves the folder and links the index (AppHost,
/// "benchmark-reports").
/// </summary>
public static class HtmlReports
{
    /// <summary>Environment variable with the index's URL, set by the AppHost; printed at the end of a run.</summary>
    public const string UrlVariable = "BENCHMARK_REPORT_URL";

    private const string Style = """
        <style>
          body { font-family: system-ui, sans-serif; margin: 2rem; max-width: 70rem; }
          .explain { line-height: 1.45; }
          .explain dt { font-weight: 600; margin-top: .6rem; }
          .explain dd { margin-left: 1.5rem; }
          .failed { border: 1px solid #c33; background: #fff4f4; padding: .2rem 1rem 1rem; margin: 1rem 0; }
          .failed pre { white-space: pre-wrap; font-size: .85em; }
          .when { color: #666; font-size: .9em; }
        </style>
        """;

    private static readonly Dictionary<string, string> Columns = new()
    {
        ["Method"] = "The benchmark: what one operation is (see What this benchmark measures).",
        ["Pods"] = "Worker processes sharing the inbox, like Kubernetes replicas: each has its own connection pool, " +
            "rate limiter and HTTP client. They run in one benchmark process here.",
        ["Workers"] = "Worker loops per pod, each claiming and sending its own batches.",
        ["Events"] = "Account change events (Kafka messages). Each is split into account, relationship and contact " +
            "events, so the inbox gets more rows than this.",
        ["Accounts"] = "How many different accounts the events are spread over. Fewer accounts mean more changes per " +
            "account: more events overwrite one not yet sent (coalescing), and more workers want the same account.",
        ["LatencyMs"] = "The mock Salesforce org's simulated round trip per request, in milliseconds. 0 shows the cost " +
            "of the sync and Postgres alone.",
        ["Mean"] = "Average time of one operation over the measured iterations (the warm-up excluded). Lower is better.",
        ["Error"] = "Half of the 99.9% confidence interval of the mean: the true mean is within Mean ± Error. With 3 " +
            "or 5 iterations it is wide, often wider than the Mean, so it does not mean anything went wrong; compare " +
            "rows by Mean, and run again before trusting a small difference.",
        ["StdDev"] = "How much the iterations differed from each other.",
        ["Median"] = "The middle iteration's time.",
        ["Gen0"] = "Garbage collections of the youngest generation per 1,000 operations. In the drain reports an " +
            "operation is a whole drain: 2000.0000 means about 2 collections per drain.",
        ["Gen1"] = "Garbage collections of generation 1 per 1,000 operations.",
        ["Gen2"] = "Full garbage collections per 1,000 operations.",
        ["Allocated"] = "Memory allocated by one operation in the benchmark process (all pods together; Postgres's " +
            "memory not included)."
    };

    /// <returns>Where to find the index (the URL when served, else the file); null if no report was written.</returns>
    public static string? Finish(IReadOnlyList<Summary> summaries, string run, DateTime startedUtc)
    {
        var folder = summaries.Select(s => s.ResultsDirectoryPath).FirstOrDefault(Directory.Exists);
        if (folder is null)
        {
            return null;
        }

        var failed = new Dictionary<string, (int Failed, int Total)>();
        foreach (var summary in summaries)
        {
            if (Explain(summary) is { } report)
            {
                failed[report] = (summary.Reports.Count(r => !r.Success), summary.Reports.Length);
            }
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
              {Style}
            </head>
            <body>
              <h1>Benchmark reports</h1>
              <p>One report per benchmark: open one for what it measures and how to read its table.</p>
              <h2>Last run: {WebUtility.HtmlEncode(run)}, {startedUtc.ToString("yyyy-MM-dd HH:mm 'UTC'", CultureInfo.InvariantCulture)}</h2>
              {List(thisRun, failed, "No report was written.")}
              <h2>Earlier runs</h2>
              {List(earlier, failed, "None.")}
            </body>
            </html>
            """;
        File.WriteAllText(Path.Combine(folder, "index.html"), html);

        return Environment.GetEnvironmentVariable(UrlVariable) is { Length: > 0 } url ? url : Path.Combine(folder, "index.html");
    }

    /// <summary>Adds the explanations to the summary's HTML report.</summary>
    /// <returns>The report's file name; null if there is none.</returns>
    private static string? Explain(Summary summary)
    {
        var type = summary.BenchmarksCases.FirstOrDefault()?.Descriptor.Type;
        var file = type is null ? null : Path.Combine(summary.ResultsDirectoryPath, $"{type.FullName}-report.html");
        if (file is null || !File.Exists(file))
        {
            return null;
        }

        var html = File.ReadAllText(file);
        var columns = Regex.Matches(html, "<th>(.*?)</th>").Select(m => m.Groups[1].Value.Trim()).ToList();
        var above = $"""
            <div class="explain">
            <p><a href="index.html">All benchmark reports</a></p>
            {Failures(summary)}
            <h2>What this benchmark measures</h2>
            <p>{Measures(type!.Name)}</p>
            <p>The lines below: BenchmarkDotNet's version, the machine and the .NET runtime, then the job's settings.
            WarmupCount is the unmeasured first run, IterationCount the measured runs that make the Mean. A Host line
            ending in [AttachedDebugger] means a debugger was attached to the process that started the run (started
            from an IDE); the measured process is the Job line's, without it.</p>
            </div>
            """;
        var below = $"""
            <div class="explain">
            <h2>The columns</h2>
            <dl>{string.Concat(columns.Where(Columns.ContainsKey).Select(c => $"<dt>{c}</dt><dd>{WebUtility.HtmlEncode(Columns[c])}</dd>"))}
            <dt>NA</dt><dd>No result: that benchmark failed. Why is at the top of this page.</dd>
            </dl>
            </div>
            """;
        html = html.Replace("</head>", Style + "</head>").Replace("<body>", "<body>" + above).Replace("</table>", "</table>" + below);
        File.WriteAllText(file, html);
        return Path.GetFileName(file);
    }

    private static string Measures(string benchmark)
    {
        return benchmark switch
        {
            nameof(InboxDrainBenchmarks) =>
                "Each row is one setup. The inbox is first filled with every event (not measured), then Pods × Workers " +
                "worker loops send it all to a mock Salesforce org and the inbox is empty. One operation, the Mean, is " +
                "that drain. After each drain the result is checked: every record in the org at its newest version, " +
                "deleted ones gone, no record ever overwritten by an older version, no two workers on one entity at " +
                "once. A failed check fails the benchmark.",
            nameof(StreamingIngestBenchmarks) =>
                "Like production: events keep arriving while the pods drain, stored as the Kafka consumer stores them, " +
                "so the same account is changed again while a worker is sending it. One operation, the Mean, is from " +
                "the first event stored until every event is sent, with the same correctness check as the drain.",
            nameof(MicroBenchmarks) =>
                "Small pieces of the sync's hot path, in memory, without a database or network: parsing Salesforce's " +
                "Sforce-Limit-Info header, reading and validating one event, and comparing a number and a date-time " +
                "with the value Salesforce returned. One operation, the Mean, is one call.",
            _ => "See the benchmark class of this name."
        };
    }

    private static string Failures(Summary summary)
    {
        var failed = summary.Reports.Where(r => !r.Success).ToList();
        var invalid = summary.ValidationErrors.Where(e => e.IsCritical).Select(e => e.Message).ToList();
        if (failed.Count == 0 && invalid.Count == 0)
        {
            return "";
        }

        var items = failed.Select(r =>
            $"<li>{WebUtility.HtmlEncode(string.Join(", ", r.BenchmarkCase.Parameters.Items.Select(p => $"{p.Name}={p.Value}")))}" +
            $"<pre>{WebUtility.HtmlEncode(Reason(r))}</pre></li>");
        return $"""
            <div class="failed">
            <h2>{failed.Count} of {summary.Reports.Length} benchmarks failed (NA in the table)</h2>
            {string.Concat(invalid.Select(m => $"<pre>{WebUtility.HtmlEncode(m)}</pre>"))}
            <ul>{string.Concat(items)}</ul>
            <p>The whole output: {WebUtility.HtmlEncode(summary.LogFilePath)}, also in the benchmarks resource's console
            log in the Aspire dashboard.</p>
            </div>
            """;
    }

    private static string Reason(BenchmarkReport report)
    {
        if (!report.GenerateResult.IsGenerateSuccess)
        {
            return $"The benchmark project could not be generated: {report.GenerateResult.GenerateException?.Message}";
        }

        if (!report.BuildResult.IsBuildSuccess)
        {
            return $"The benchmark project did not build: {report.BuildResult.ErrorMessage}";
        }

        // The exception's message lines, not its stack trace. The benchmark process's output is in Results (its lines
        // that don't start with "//"), not in StandardOutput.
        var errors = report.ExecuteResults
            .SelectMany(r => r.Errors.Concat(r.Results))
            .Where(l => l.Contains("Exception", StringComparison.Ordinal) && !l.TrimStart().StartsWith("at ", StringComparison.Ordinal))
            .Distinct()
            .Take(5)
            .ToList();
        return errors.Count > 0
            ? string.Join(Environment.NewLine, errors)
            : report.ExecuteResults.LastOrDefault()?.ExitCode is { } code and not 0
                ? $"The benchmark process exited with code {code}."
                : "No measurement was recorded.";
    }

    private static string List(List<FileInfo> reports, Dictionary<string, (int Failed, int Total)> failed, string none)
    {
        if (reports.Count == 0)
        {
            return $"<p>{none}</p>";
        }

        var items = reports.Select(f =>
        {
            var failures = failed.TryGetValue(f.Name, out var n) && n.Failed > 0 ? $" <strong>{n.Failed} of {n.Total} failed</strong>" : "";
            return $"""<li><a href="{Uri.EscapeDataString(f.Name)}">{WebUtility.HtmlEncode(Title(f.Name))}</a>{failures} <span class="when">{f.LastWriteTimeUtc.ToString("yyyy-MM-dd HH:mm 'UTC'", CultureInfo.InvariantCulture)}</span></li>""";
        });
        return $"<ul>{string.Concat(items)}</ul>";
    }

    /// <summary>SalesforceSync.Benchmarks.InboxDrainBenchmarks-report.html → InboxDrainBenchmarks.</summary>
    private static string Title(string file)
    {
        var name = file[..^"-report.html".Length];
        return name[(name.LastIndexOf('.') + 1)..];
    }
}
