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
using SalesforceSync;
using SalesforceSync.Data;
using SalesforceSync.FakeSalesforce;
using SalesforceSync.Inbox;
using SalesforceSync.RateLimiting;
using SalesforceSync.Salesforce;
using SalesforceSync.Sync;

namespace SalesforceSync.Benchmarks;

/// <summary>
/// End-to-end: put N events (Account changes, and Account_Relationship__c changes between those accounts) into the
/// real Postgres inbox and drain it with P "pods" x W parallel worker loops. A pod is wired exactly like the worker
/// (<see cref="SyncServiceCollectionExtensions"/>, rate limiter included) and its loops run the worker's own pass
/// (<see cref="InboxWorkerService.SyncOnceAsync"/>) over every registered entity type. Only the Salesforce HTTP
/// endpoint is fake. A pod is an independent service provider (own connection pool, rate limiter, HTTP client), like a separate
/// process; all pods share the Postgres inbox and one Salesforce org.
/// <para>
/// Each iteration also verifies correctness. Nothing in the org rejects an out-of-order write, so a race would
/// land: every account must end at its newest version, the org's write history must show each account's
/// versions only moving forward, every relationship must end at its newest version linked to the right two
/// Accounts, the claim audit (sql/claim-audit.sql) must show no two overlapping claims on the same entity, and
/// every inbox row must end done.
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

    /// <summary>Share of events that change a relationship between two already-seen accounts (like the load generator).</summary>
    private const double RelationshipShare = 0.15;

    private NpgsqlDataSource _db = default!;
    private FakeSalesforceStore _store = default!;
    private List<ServiceProvider> _pods = [];
    private List<InboxRow> _events = [];
    private Dictionary<(string Type, string Key), long> _expected = new();
    private Dictionary<string, (string Account, string Related)> _relationships = new();

    /// <summary>An event: its entity type and the raw payload the inbox stores.</summary>
    private sealed record InboxRow(string Type, string Payload);

    /// <summary>meta.time of version N of an entity: base time + N ms, so per-entity time order = version order.</summary>
    private static readonly DateTimeOffset BaseTime = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private static JsonObject Meta(long version) => new()
    {
        ["time"] = BaseTime.AddMilliseconds(version).ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'", CultureInfo.InvariantCulture)
    };

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
            .Select(loop => Task.Run(() => WorkerLoopAsync(pod, loop, ingest, cts.Token))));
        await Task.WhenAll(loops.Append(ingest));
    }

    /// <summary>Inserts the events in small chunks, in per-account version order, as fast as possible (like the Kafka consumer).</summary>
    private async Task IngestAsync(CancellationToken ct)
    {
        const string sql = "insert into sf_inbox (payload) select p::jsonb from unnest(@p::text[]) as u(p)";

        await using var conn = await _db.OpenConnectionAsync(ct);
        foreach (var chunk in _events.Chunk(IngestChunk))
        {
            await conn.ExecuteAsync(new CommandDefinition(sql, new { p = chunk.Select(e => e.Payload).ToArray() }, cancellationToken: ct));
        }
    }

    private async Task WorkerLoopAsync(ServiceProvider pod, int loop, Task ingest, CancellationToken ct)
    {
        var scopes = pod.GetRequiredService<IServiceScopeFactory>();
        for (var iteration = 0; ; iteration++)
        {
            ct.ThrowIfCancellationRequested();

            // The worker's own pass: one batch of every registered entity type (accounts, relationships)
            var processed = await InboxWorkerService.SyncOnceAsync(scopes, loop + iteration, ct);

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

    /// <summary>
    /// One "pod": its own connection pool, limiter and HTTP stack; shares Postgres and the org. Wired with the worker's
    /// registrations; only options, the data source and the primary HTTP handler (the mock org) differ.
    /// </summary>
    private ServiceProvider BuildPod()
    {
        var services = new ServiceCollection();
        services.AddLogging(); // no providers: silent
        services.AddSingleton(_ => NpgsqlDataSource.Create(BenchmarkDb.ConnectionString));
        services.AddSingleton(_store);

        services.Configure<SalesforceOptions>(o => o.InstanceUrl = "https://fake.my.salesforce.com");
        services.Configure<SyncOptions>(o =>
        {
            o.Workers = Workers;
            o.MaxConcurrentRequests = Workers * Pods; // global cap across all pods
            o.BatchSize = StreamIngest ? 10 : 75;     // streaming: small batches, so entities are claimed again and again
            o.Lease = TimeSpan.FromMinutes(5);
            // A relationship can be claimed before its Accounts are in the org (DEPENDENCY_NOT_SYNCED): retry it
            // within milliseconds rather than the production 30 s, so the benchmark measures the pipeline
            o.RetryBaseDelay = TimeSpan.FromMilliseconds(10);
            o.MaxRetryDelay = TimeSpan.FromMilliseconds(500);
            o.MaxRetries = 50;
        });
        services.Configure<RateLimitOptions>(_ => { });

        services.AddSalesforceClient()
            .AddSalesforceRateLimiting() // the bucket is opened up per iteration (SetupAsync)
            .ConfigurePrimaryHttpMessageHandler(sp => new FakeSalesforceHandler(sp.GetRequiredService<FakeSalesforceStore>()));
        services.AddInboxSync();
        return services.BuildServiceProvider();
    }

    private async Task SetupAsync()
    {
        _store.Reset();

        await using var conn = await _db.OpenConnectionAsync();
        await conn.ExecuteAsync("truncate sf_inbox restart identity; truncate sf_sync_state; truncate sf_api_usage; truncate sf_claim_audit");
        // Effectively unlimited bucket and no daily budget: measure the pipeline, not the throttle
        await conn.ExecuteAsync("""
            update sf_rate_limit
            set tokens = 1e9, capacity = 1e9, base_refill_per_sec = 1e9, refill_per_sec = 1e9,
                daily_budget = null, paused_until = null, updated_at = clock_timestamp()
            where name = 'salesforce'
            """);

        (_events, _expected) = Generate();
        if (!StreamIngest) await PreloadAsync(conn);
    }

    /// <summary>
    /// Random account per event with occasional bursts, versions increasing per entity. About 15% of events change
    /// a relationship between two accounts that already have an event (as in Kafka, the account event comes first,
    /// but a worker may still claim the relationship before the account has synced).
    /// </summary>
    private (List<InboxRow> Events, Dictionary<(string, string), long> Newest) Generate()
    {
        var random = new Random(42);
        var versions = new Dictionary<(string, string), long>();
        var seen = new List<string>();
        var events = new List<InboxRow>(Events);
        _relationships = new Dictionary<string, (string, string)>(StringComparer.Ordinal);

        while (events.Count < Events)
        {
            if (seen.Count > 1 && random.NextDouble() < RelationshipShare)
            {
                var a = seen[random.Next(seen.Count)];
                var b = seen[random.Next(seen.Count)];
                if (a == b) continue;
                var relKey = $"REL-{a[4..]}-{b[4..]}";
                _relationships[relKey] = (a, b);
                var relVersion = versions[(AccountRelationshipSyncService.Type, relKey)] =
                    versions.GetValueOrDefault((AccountRelationshipSyncService.Type, relKey)) + 1;
                var relPayload = new JsonObject
                {
                    ["meta"] = Meta(relVersion),
                    [AccountRelationshipSyncService.Type] = new JsonObject
                    {
                        ["externalId"] = relKey,
                        ["accountExternalId"] = a,
                        ["relatedAccountExternalId"] = b,
                        ["Name"] = $"v{relVersion}",
                        ["Type__c"] = "Partner",
                        ["Active__c"] = true
                    }
                };
                events.Add(new InboxRow(AccountRelationshipSyncService.Type, relPayload.ToJsonString()));
                continue;
            }

            var key = $"ACC-{random.Next(Accounts):D6}";
            var burst = random.NextDouble() < 0.1 ? random.Next(2, 6) : 1;

            for (var i = 0; i < burst && events.Count < Events; i++)
            {
                var version = versions[(AccountSyncService.Type, key)] = versions.GetValueOrDefault((AccountSyncService.Type, key)) + 1;
                if (version == 1) seen.Add(key);
                // No profile.externalId: the Profile__c takes the account's
                var payload = new JsonObject
                {
                    ["meta"] = Meta(version),
                    [AccountSyncService.Type] = new JsonObject
                    {
                        ["externalId"] = key,
                        ["Name"] = $"Account {key}",
                        ["Description"] = $"v{version}",
                        ["profile"] = new JsonObject { ["Score__c"] = version }
                    }
                };
                events.Add(new InboxRow(AccountSyncService.Type, payload.ToJsonString()));
            }
        }

        return (events, versions);
    }

    private async Task PreloadAsync(NpgsqlConnection conn)
    {
        await using var importer = await conn.BeginBinaryImportAsync(
            "copy sf_inbox (payload) from stdin (format binary)");
        foreach (var e in _events)
        {
            await importer.StartRowAsync();
            await importer.WriteAsync(e.Payload, NpgsqlDbType.Jsonb);
        }
        await importer.CompleteAsync();
    }

    // ---------- Verification ----------

    private async Task VerifyAsync()
    {
        var wrong = new List<string>();
        var regressions = 0;
        foreach (var ((type, acct), expected) in _expected)
        {
            if (type == AccountRelationshipSyncService.Type)
            {
                if (!VerifyRelationship(acct, expected, ref regressions, out var problem)) wrong.Add(problem);
                continue;
            }

            // Versions in the order the org received them: Description "vN" on the Account, Score__c = N on the Profile
            var accountVersions = (_store.History("Account", acct) ?? [])
                .Where(w => w.ContainsKey("Description"))
                .Select(w => long.Parse(((string)w["Description"]!)[1..])).ToList();
            var profileVersions = (_store.History("Profile__c", acct) ?? [])
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
        // retry_count counts attempts (bumped at claim), so retries = attempts beyond the first. Mostly relationships
        // claimed before their Accounts were in the org (DEPENDENCY_NOT_SYNCED).
        var retries = await conn.ExecuteScalarAsync<long>("select coalesce(sum(greatest(retry_count - 1, 0)), 0) from sf_inbox");
        var relationshipEvents = _events.Count(e => e.Type == AccountRelationshipSyncService.Type);

        var stats = _store.GetStats();
        Console.WriteLine(
            $"// [{(StreamIngest ? "streaming" : "preloaded")} pods={Pods} workers={Workers} accounts={Accounts} latency={LatencyMs}ms] " +
            $"events={Events} relationshipEvents={relationshipEvents} {stats} coalescing={(double)Events / Math.Max(1, stats.GraphsSucceeded):F1} events/graph " +
            $"claims={claims} retries={retries} overlappingClaims={overlaps} notDone={notDone} wrong={wrong.Count} regressions={regressions}");

        if (wrong.Count > 0 || rows != Events || notDone > 0 || overlaps > 0 || regressions > 0)
            throw new InvalidOperationException(
                $"Correctness check failed: {wrong.Count} entities not at newest version, {notDone} of {rows}/{Events} " +
                $"inbox rows not done, {overlaps} overlapping claims, {regressions} entities received an older version " +
                "after a newer one. " + string.Join("; ", wrong.Take(5)));
    }

    /// <summary>Newest version (Name "vN"), only moving forward, and linked to the right two Accounts.</summary>
    private bool VerifyRelationship(string key, long expected, ref int regressions, out string problem)
    {
        problem = "";
        var versions = (_store.History(AccountRelationshipSyncService.RelationshipObject, key) ?? [])
            .Where(w => w.ContainsKey("Name"))
            .Select(w => long.Parse(((string)w["Name"]!)[1..])).ToList();
        if (!IsIncreasing(versions)) regressions++;

        var (account, related) = _relationships[key];
        var record = _store.FindByExternalId(AccountRelationshipSyncService.RelationshipObject, key);
        var accountId = (string?)_store.FindByExternalId(AccountSyncService.AccountObject, account)?["Id"];
        var relatedId = (string?)_store.FindByExternalId(AccountSyncService.AccountObject, related)?["Id"];
        if (versions.LastOrDefault() == expected && record is not null && accountId is not null &&
            (string?)record["Account__c"] == accountId && (string?)record["Related_Account__c"] == relatedId)
            return true;

        problem = $"{key}: expected v{expected} linking {account}->{related}, got v{versions.LastOrDefault()} " +
                  $"linking {record?["Account__c"]}->{record?["Related_Account__c"]}";
        return false;
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
