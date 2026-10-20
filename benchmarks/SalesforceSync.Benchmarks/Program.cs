using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Running;
using SalesforceSync.Benchmarks;
using SalesforceSync.LoadGenerator;

// Run: dotnet run -c Release -- --run quick|drain|full, or no args to choose in console. Other args pass to
// BenchmarkDotNet as is (e.g. --filter '*Micro*').
// Drain benchmarks need Postgres: set ConnectionStrings__benchdb (Aspire does it).
// Optimizations validator disabled so project also launches from Aspire dashboard (Debug host); BenchmarkDotNet
// still builds and runs measured code in Release.
// Run: dotnet run -c Release -- --load [...]: sustained-load simulation (see SustainedLoad), not a BenchmarkDotNet run
// Either run takes --template my.json: your own aggregate example (see AggregateTemplate, example-aggregate.json);
// child benchmark processes get it via environment.
var templateAt = Array.IndexOf(args, "--template");
if (templateAt >= 0)
{
    if (templateAt + 1 >= args.Length)
    {
        Console.Error.WriteLine("--template needs the path of a JSON file");
        return 1;
    }

    Environment.SetEnvironmentVariable(AggregateTemplate.Variable, Path.GetFullPath(args[templateAt + 1]));
    args = [.. args.Take(templateAt), .. args.Skip(templateAt + 2)];
}

if (Environment.GetEnvironmentVariable(AggregateTemplate.Variable) is { Length: > 0 } templatePath)
{
    try
    {
        _ = AggregateTemplate.Current; // refuse now, with problems, not in every benchmark process
    }
    catch (Exception ex) when (ex is InvalidDataException or IOException)
    {
        Console.Error.WriteLine(ex.Message);
        return 1;
    }

    Console.WriteLine($"// aggregates follow the template {templatePath}");
}

if (args.Contains("--load"))
{
    await new SustainedLoad(SustainedLoad.Options.Parse(args)).RunAsync();
    return 0;
}

var config = DefaultConfig.Instance.WithOptions(ConfigOptions.DisableOptimizationsValidator);
var run = BenchmarkRun.Choose(ref args);
var started = DateTime.UtcNow;
// typeof(Program) ambiguous with worker's top-level Program (CS0436): name a benchmark type
var summaries = BenchmarkSwitcher.FromAssembly(typeof(InboxDrainBenchmarks).Assembly)
    .Run(run?.Filter.Prepend("--filter").Concat(args).ToArray() ?? args, config).ToList();
if (HtmlReports.Finish(summaries, run?.Name ?? string.Join(' ', args), started) is { } report)
{
    Console.WriteLine($"// HTML report: {report}");
}
return 0;
