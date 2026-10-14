namespace SalesforceSync.Benchmarks;

internal static class BenchmarkDb
{
    /// <summary>
    /// Aspire injects ConnectionStrings__benchdb. Use a dedicated database: the drain benchmark
    /// truncates the inbox tables and resets sf_rate_limit.
    /// </summary>
    public static string ConnectionString =>
        Environment.GetEnvironmentVariable("ConnectionStrings__benchdb")
        ?? Environment.GetEnvironmentVariable("SFSYNC_BENCH_DB")
        ?? "Host=localhost;Port=5432;Database=benchdb;Username=postgres;Password=postgres";
}