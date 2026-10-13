using System.Globalization;
using System.Text.Json.Nodes;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Engines;
using BenchmarkDotNet.Jobs;
using Dapper;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;
using SalesforceSync.FakeSalesforce;
using SalesforceSync.Inbox;
using SalesforceSync.RateLimiting;
using SalesforceSync.Salesforce;

namespace SalesforceSync.Benchmarks;

/// <summary>
/// End-to-end: put N events into the real Postgres inbox and drain it with P "pods" x W parallel InboxWorkers
/// through the real AccountProfileSync client and rate limiter. Only the Salesforce HTTP endpoint is fake.
/// A pod is an independent service provider (own connection pool, rate limiter, HTTP client), like a separate
/// process; all pods share the Postgres inbox and one Salesforce org.
/// <para>
/// Each iteration also verifies correctness. Nothing in the org rejects an out-of-order write, so a race would
/// land: every account must end at its newest version, the org's write history must show each account's
/// versions only moving forward, the claim audit (sql/claim-audit.sql) must show no two overlapping claims on
/// the same account, and no event may be superseded (events are ingested in order, so that would mean an
/// account's sync watermark ran ahead of its events).
/// </para>
/// </summary>
[Config(typeof(DrainConfig))]
[MemoryDiagnoser]
public abstract class DrainBenchmarkBase
{
    public sealed class DrainConfig : ManualConfig
    {
        public DrainConfig()
        {
            AddJob(Job.Default
                .WithStrategy(RunStrategy.Monitoring)
                .WithLaunchCount(1)
                .WithWarmupCount(1)
                .WithIterationCount(5)
                .WithInvocationCount(1)
                .WithUnrollFactor(1));
        }
    }

    /// <summary>Worker loops per pod.</summary>
    public abstract int Workers { get; set; }

    /// <summary>Independent worker processes sharing the inbox (Kubernetes replicas).</summary>
    public abstract int Pods { get; set; }

    public abstract int Events { get; set; }

    /// <summary>Fewer accounts = more updates per account = more coalescing and ordering pressure.</summary>
    public abstract int Accounts { get; set; }

    /// <summary>Simulated Salesforce round trip.</summary>
    public abstract int LatencyMs { get; set; }

    /// <summary>False: the whole inbox is loaded before draining. True: events keep arriving while pods drain.</summary>
    protected abstract bool StreamIngest { get; }

    private const int IngestChunk = 20;

    private NpgsqlDataSource _db = default!;
    private FakeSalesforceStore _store = default!;
    private List<ServiceProvider> _pods = [];
    private List<(string Account, long Version, string Payload)> _events = [];
    private Dictionary<string, long> _expected = new();

    [GlobalSetup]
    public void GlobalSetup()
    {
        _db = NpgsqlDataSource.Create(BenchmarkDb.ConnectionString);
        InboxSchema.EnsureCreatedAsync(_db).GetAwaiter().GetResult();
        using (var conn = _db.OpenConnection())
            conn.Execute(File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "claim-audit.sql")));

        _store = new FakeSalesforceStore(new FakeSalesforceOptions
        {
            Latency = TimeSpan.FromMilliseconds(LatencyMs),
            EnforceDailyLimit = false
        });

        _pods = Enumerable.Range(0, Pods).Select(_ => BuildPod()).ToList();
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        foreach (var pod in _pods) pod.Dispose();
        _db.Dispose();
    }

    [IterationSetup]
    public void IterationSetup() => SetupAsync().GetAwaiter().GetResult();

    [IterationCleanup]
    public void IterationCleanup() => VerifyAsync().GetAwaiter().GetResult();

    protected async Task DrainAsync()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5));
        var ingest = StreamIngest ? Task.Run(() => IngestAsync(cts.Token)) : Task.CompletedTask;
        var loops = _pods.SelectMany(pod => Enumerable.Range(0, Workers)
            .Select(_ => Task.Run(() => WorkerLoopAsync(pod, ingest, cts.Token))));
        await Task.WhenAll(loops.Append(ingest));
    }

    /// <summary>Inserts the events in small chunks, in per-account version order, as fast as possible (like the Kafka consumer).</summary>
    private async Task IngestAsync(CancellationToken ct)
    {
        const string sql = """
            insert into sf_inbox (account_ext_id, version, payload)
            select a, v, p::jsonb from unnest(@a::text[], @v::bigint[], @p::text[]) as t(a, v, p)
            """;

        await using var conn = await _db.OpenConnectionAsync(ct);
        foreach (var chunk in _events.Chunk(IngestChunk))
        {
            await conn.ExecuteAsync(new CommandDefinition(sql, new
            {
                a = chunk.Select(e => e.Account).ToArray(),
                v = chunk.Select(e => e.Version).ToArray(),
                p = chunk.Select(e => e.Payload).ToArray()
            }, cancellationToken: ct));
        }
    }

    private async Task WorkerLoopAsync(ServiceProvider pod, Task ingest, CancellationToken ct)
    {
        while (true)
        {
            ct.ThrowIfCancellationRequested();

            int processed;
            await using (var scope = pod.CreateAsyncScope())
            {
                var worker = scope.ServiceProvider.GetRequiredService<InboxWorker>();
                processed = await worker.RunOnceAsync(ct);
            }

            if (processed > 0) continue;
            if (ingest.IsFaulted) return;
            if (ingest.IsCompleted && !await HasPendingAsync(ct)) return;
            await Task.Delay(2, ct); // other workers hold the remaining accounts, or more events are on the way
        }
    }

    private async Task<bool> HasPendingAsync(CancellationToken ct)
    {
        await using var conn = await _db.OpenConnectionAsync(ct);
        return await conn.ExecuteScalarAsync<bool>(new CommandDefinition(
            "select exists (select 1 from sf_inbox where status = 'pending')", cancellationToken: ct));
    }

    // ---------- Setup ----------

    /// <summary>One "pod": its own connection pool, limiter and HTTP stack; shares Postgres and the org.</summary>
    private ServiceProvider BuildPod()
    {
        var services = new ServiceCollection();
        services.AddLogging(); // no providers: silent
        services.AddSingleton(_ => NpgsqlDataSource.Create(BenchmarkDb.ConnectionString));
        services.AddSingleton(_store);

        services.Configure<SyncOptions>(o =>
        {
            o.Workers = Workers;
            o.MaxConcurrentRequests = Workers * Pods; // global cap across all pods
            o.BatchSize = StreamIngest ? 10 : 75;     // streaming: small batches, so accounts are claimed again and again
            o.Lease = TimeSpan.FromMinutes(5);
        });
        services.Configure<RateLimitOptions>(_ => { });

        services.AddSingleton<SalesforceRateLimiter>();
        services.AddSingleton(sp => new SalesforceConcurrencySlots(
            sp.GetRequiredService<NpgsqlDataSource>(),
            sp.GetRequiredService<IOptions<SyncOptions>>().Value.MaxConcurrentRequests));
        services.AddTransient<SalesforceRateLimitHandler>();

        services.AddHttpClient<AccountProfileSync>(c => c.BaseAddress = new Uri("https://fake.my.salesforce.com"))
            .AddHttpMessageHandler<SalesforceRateLimitHandler>()
            .ConfigurePrimaryHttpMessageHandler(sp => new FakeSalesforceHandler(sp.GetRequiredService<FakeSalesforceStore>()));

        services.AddTransient<InboxWorker>();
        return services.BuildServiceProvider();
    }

    private async Task SetupAsync()
    {
        _store.Reset();

        await using var conn = await _db.OpenConnectionAsync();
        await conn.ExecuteAsync("truncate sf_inbox restart identity; truncate sf_sync_state; truncate sf_claim_audit");
        // Effectively unlimited bucket: measure the pipeline, not the throttle
        await conn.ExecuteAsync("""
            update sf_rate_limit
            set tokens = 1e9, capacity = 1e9, base_refill_per_sec = 1e9, refill_per_sec = 1e9,
                paused_until = null, updated_at = clock_timestamp()
            where name = 'salesforce'
            """);

        (_events, _expected) = Generate();
        if (!StreamIngest) await PreloadAsync(conn);
    }

    /// <summary>Random account per event with occasional bursts, versions increasing per account.</summary>
    private (List<(string, long, string)> Events, Dictionary<string, long> Newest) Generate()
    {
        var random = new Random(42);
        var versions = new Dictionary<string, long>(StringComparer.Ordinal);
        var events = new List<(string, long, string)>(Events);

        while (events.Count < Events)
        {
            var key = $"ACC-{random.Next(Accounts):D6}";
            var burst = random.NextDouble() < 0.1 ? random.Next(2, 6) : 1;

            for (var i = 0; i < burst && events.Count < Events; i++)
            {
                var version = versions[key] = versions.GetValueOrDefault(key) + 1;
                var payload = new JsonObject
                {
                    ["profileExternalId"] = key.Replace("ACC-", "PRF-"),
                    ["version"] = version,
                    ["account"] = new JsonObject
                    {
                        ["Name"] = $"Account {key}",
                        ["Description"] = $"v{version}"
                    },
                    ["profile"] = new JsonObject { ["Score__c"] = version }
                };
                events.Add((key, version, payload.ToJsonString()));
            }
        }

        return (events, versions);
    }

    private async Task PreloadAsync(NpgsqlConnection conn)
    {
        await using var importer = await conn.BeginBinaryImportAsync(
            "copy sf_inbox (account_ext_id, version, payload) from stdin (format binary)");
        foreach (var (account, version, payload) in _events)
        {
            await importer.StartRowAsync();
            await importer.WriteAsync(account, NpgsqlDbType.Text);
            await importer.WriteAsync(version, NpgsqlDbType.Bigint);
            await importer.WriteAsync(payload, NpgsqlDbType.Jsonb);
        }
        await importer.CompleteAsync();
    }

    // ---------- Verification ----------

    private async Task VerifyAsync()
    {
        var wrong = new List<string>();
        var regressions = 0;
        foreach (var (acct, expected) in _expected)
        {
            // Versions in the order the org received them: Description "vN" on the Account, Score__c = N on the Profile
            var accountVersions = (_store.History("Account", acct) ?? [])
                .Where(w => w.ContainsKey("Description"))
                .Select(w => long.Parse(((string)w["Description"]!)[1..])).ToList();
            var profileVersions = (_store.History("Profile__c", acct.Replace("ACC-", "PRF-")) ?? [])
                .Where(w => w.ContainsKey("Score__c"))
                .Select(w => Number(w["Score__c"])).ToList();

            if (!IsIncreasing(accountVersions) || !IsIncreasing(profileVersions)) regressions++;
            if (accountVersions.LastOrDefault() != expected || profileVersions.LastOrDefault() != expected)
                wrong.Add($"{acct}: expected v{expected}, account v{accountVersions.LastOrDefault()}, profile v{profileVersions.LastOrDefault()}");
        }

        await using var conn = await _db.OpenConnectionAsync();
        var rows = await conn.ExecuteScalarAsync<long>("select count(*) from sf_inbox");
        var notDone = await conn.ExecuteScalarAsync<long>("select count(*) from sf_inbox where status <> 'done'");
        var claims = await conn.ExecuteScalarAsync<long>("select count(*) from sf_claim_audit");
        var overlaps = await conn.ExecuteScalarAsync<long>("select count(*) from sf_claim_overlaps");

        var stats = _store.GetStats();
        Console.WriteLine(
            $"// [{(StreamIngest ? "streaming" : "preloaded")} pods={Pods} workers={Workers} accounts={Accounts} latency={LatencyMs}ms] " +
            $"events={Events} {stats} coalescing={(double)Events / Math.Max(1, stats.GraphsSucceeded):F1} events/graph " +
            $"claims={claims} overlappingClaims={overlaps} notDone={notDone} wrong={wrong.Count} regressions={regressions}");

        if (wrong.Count > 0 || rows != Events || notDone > 0 || overlaps > 0 || regressions > 0)
            throw new InvalidOperationException(
                $"Correctness check failed: {wrong.Count} accounts not at newest version, {notDone} of {rows}/{Events} " +
                $"inbox rows not done, {overlaps} overlapping claims, {regressions} accounts received an older version " +
                "after a newer one. " + string.Join("; ", wrong.Take(5)));
    }

    private static bool IsIncreasing(List<long> versions) => versions.Zip(versions.Skip(1)).All(p => p.First < p.Second);

    private static long Number(JsonNode? node) =>
        (long)decimal.Parse(node!.ToJsonString(), NumberStyles.Float, CultureInfo.InvariantCulture);
}

/// <summary>Whole inbox loaded up front, then drained.</summary>
public class InboxDrainBenchmarks : DrainBenchmarkBase
{
    [Params(1, 4, 8)] public override int Workers { get; set; }
    [Params(1, 3)] public override int Pods { get; set; }
    [Params(5_000)] public override int Events { get; set; }
    [Params(100, 1_000)] public override int Accounts { get; set; }
    [Params(0, 25)] public override int LatencyMs { get; set; }
    protected override bool StreamIngest => false;

    [Benchmark(Description = "Drain inbox")]
    public Task DrainInbox() => DrainAsync();
}

/// <summary>
/// Events keep arriving (in per-account version order, like Kafka) while the pods drain, so the same account is
/// claimed, released and claimed again many times by different workers in different pods. This is the race the
/// head-row claim must prevent.
/// </summary>
public class StreamingIngestBenchmarks : DrainBenchmarkBase
{
    [Params(4, 8)] public override int Workers { get; set; }
    [Params(1, 3)] public override int Pods { get; set; }
    [Params(5_000)] public override int Events { get; set; }
    [Params(50)] public override int Accounts { get; set; }
    [Params(5)] public override int LatencyMs { get; set; }
    protected override bool StreamIngest => true;

    [Benchmark(Description = "Drain while ingesting")]
    public Task DrainWhileIngesting() => DrainAsync();
}
