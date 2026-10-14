using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Running;

// Run: dotnet run -c Release -- --filter '*'
// The drain benchmark needs Postgres: set ConnectionStrings__benchdb (Aspire does this for you).
// The optimizations validator is disabled so the project can also be launched from the Aspire
// dashboard (Debug host); BenchmarkDotNet still builds and runs the measured code in Release.
var config = DefaultConfig.Instance.WithOptions(ConfigOptions.DisableOptimizationsValidator);
// typeof(Program) would be ambiguous with the worker's top-level Program (CS0436), so name a benchmark type
BenchmarkSwitcher.FromAssembly(typeof(SalesforceSync.Benchmarks.InboxDrainBenchmarks).Assembly).Run(args, config);