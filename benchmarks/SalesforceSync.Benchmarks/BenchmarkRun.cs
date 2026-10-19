namespace SalesforceSync.Benchmarks;

/// <summary>
/// The named runs, from a few minutes to the whole suite. The chosen run goes to the benchmark processes in
/// <see cref="Variable"/>; <see cref="IsQuick"/> reads it to pick the shorter parameter lists.
/// </summary>
public sealed record BenchmarkRun(string Name, string Description, string[] Filter)
{
    public const string Variable = "BENCHMARK_RUN";

    public static readonly BenchmarkRun[] All =
    [
        new("quick", "drain 1 and 4 workers x 1 and 3 pods, 100 accounts, no latency, plus one streaming case; 3 iterations (about 2 minutes)",
            ["*InboxDrain*", "*StreamingIngest*"]),
        new("drain", "every drain and streaming case (about 20 minutes)", ["*InboxDrain*", "*StreamingIngest*"]),
        new("full", "drain, streaming and micro benchmarks (about 30 minutes)", ["*"])
    ];

    public static bool IsQuick => Environment.GetEnvironmentVariable(Variable) == "quick";

    /// <summary>
    /// The run named by <c>--run</c> (removed from <paramref name="args"/>); with no arguments and a console, the one
    /// chosen there. Null: no run, the arguments go to BenchmarkDotNet unchanged.
    /// </summary>
    public static BenchmarkRun? Choose(ref string[] args)
    {
        var at = Array.IndexOf(args, "--run");
        string? name = null;
        if (at >= 0 && at + 1 < args.Length)
        {
            name = args[at + 1];
            args = [.. args[..at], .. args[(at + 2)..]];
        }
        else if (args.Length == 0 && !Console.IsInputRedirected)
        {
            name = Ask();
        }
        if (name is null)
        {
            return null;
        }

        var run = All.FirstOrDefault(r => r.Name == name)
            ?? throw new ArgumentException($"Unknown run '{name}': {string.Join(", ", All.Select(r => r.Name))}");
        Environment.SetEnvironmentVariable(Variable, run.Name);
        return run;
    }

    private static string Ask()
    {
        Console.WriteLine("Benchmark run:");
        for (var i = 0; i < All.Length; i++)
        {
            Console.WriteLine($"  {i + 1}. {All[i].Name}: {All[i].Description}");
        }
        while (true)
        {
            Console.Write($"Choose 1-{All.Length} [1]: ");
            var answer = Console.ReadLine()?.Trim();
            if (string.IsNullOrEmpty(answer))
            {
                return All[0].Name;
            }
            if (int.TryParse(answer, out var n) && n >= 1 && n <= All.Length)
            {
                return All[n - 1].Name;
            }
            if (All.Any(r => r.Name == answer))
            {
                return answer;
            }
        }
    }
}