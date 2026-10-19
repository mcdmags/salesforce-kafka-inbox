using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Running;
using SalesforceSync.Benchmarks;

// Run: dotnet run -c Release -- --run quick|drain|full, or with no arguments to choose in the console. Any other
// arguments go to BenchmarkDotNet as they are (e.g. --filter '*Micro*').
// The drain benchmarks need Postgres: set ConnectionStrings__benchdb (Aspire does this for you).
// The optimizations validator is disabled so the project can also be launched from the Aspire
// dashboard (Debug host); BenchmarkDotNet still builds and runs the measured code in Release.
// Run: dotnet run -c Release -- --load [...]: the sustained-load simulation (see SustainedLoad), not a BenchmarkDotNet run
// Either run takes --template my.json: your own example of the aggregates to publish (see AggregateTemplate and
// example-aggregate.json); the child benchmark processes get it through the environment.
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
        _ = AggregateTemplate.Current; // refused now, with what is wrong, rather than in every benchmark process
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
// typeof(Program) would be ambiguous with the worker's top-level Program (CS0436), so name a benchmark type
BenchmarkSwitcher.FromAssembly(typeof(InboxDrainBenchmarks).Assembly).Run(run?.Filter.Prepend("--filter").Concat(args).ToArray() ?? args, config);
return 0;
