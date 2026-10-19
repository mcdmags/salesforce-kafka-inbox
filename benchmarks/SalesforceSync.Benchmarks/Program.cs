using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Running;
using SalesforceSync.Benchmarks;

// Run: dotnet run -c Release -- --run quick|drain|full, or with no arguments to choose in the console. Any other
// arguments go to BenchmarkDotNet as they are (e.g. --filter '*Micro*').
// The drain benchmarks need Postgres: set ConnectionStrings__benchdb (Aspire does this for you).
// The optimizations validator is disabled so the project can also be launched from the Aspire
// dashboard (Debug host); BenchmarkDotNet still builds and runs the measured code in Release.
// Run: dotnet run -c Release -- --load [...]: the sustained-load simulation (see SustainedLoad), not a BenchmarkDotNet run
if (args.Contains("--load"))
{
    await new SustainedLoad(SustainedLoad.Options.Parse(args)).RunAsync();
    return;
}

var config = DefaultConfig.Instance.WithOptions(ConfigOptions.DisableOptimizationsValidator);
var run = BenchmarkRun.Choose(ref args);
// typeof(Program) would be ambiguous with the worker's top-level Program (CS0436), so name a benchmark type
BenchmarkSwitcher.FromAssembly(typeof(InboxDrainBenchmarks).Assembly).Run(run?.Filter.Prepend("--filter").Concat(args).ToArray() ?? args, config);