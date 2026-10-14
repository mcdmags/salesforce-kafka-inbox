using System.Globalization;
using System.Text.Json.Nodes;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Engines;
using BenchmarkDotNet.Jobs;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using NpgsqlTypes;
using SalesforceSync.Data;
using SalesforceSync.FakeSalesforce;
using SalesforceSync.Inbox;
using SalesforceSync.RateLimiting;
using SalesforceSync.Salesforce;
using SalesforceSync.Sync;
using static SalesforceSync.Data.SqlCommands;

namespace SalesforceSync.Benchmarks;

/// <summary>
/// End-to-end: put N events (Account changes, and Account_Relationship__c changes between those accounts) into the
/// real Postgres inbox and drain it with P "pods", each running the worker's one sequential loop. A pod is wired
/// exactly like the worker (<see cref="SyncServiceCollectionExtensions"/>, rate limiter included) and its loop runs the
/// worker's own pass (<see cref="InboxWorkerService.SyncOnceAsync"/>) over every registered entity type. Only the Salesforce HTTP
/// endpoint is fake. A pod is an independent service provider (own connection pool, rate limiter, HTTP client), like a separate
/// process; all pods share the Postgres inbox and one Salesforce org.
/// <para>
/// Each iteration also verifies correctness. Nothing in the org rejects an out-of-order write, so a race would
/// land: every account must end at its newest version, the org's write history must show each account's
/// versions only moving forward (no regressions) and no version written twice (no duplicates: two workers that
/// processed the same entity at once), every relationship must end at its newest version linked to the right two
/// Accounts, and every inbox row must end done or superseded (an older event read together with a newer one of its
/// entity). This branch has no race handling, so only one pod is safe; more pods show what goes wrong.
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

    /// <summary>Independent worker processes sharing the inbox (Kubernetes replicas), one sequential loop each.</summary>
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
    private IDbContextFactory<SyncDbContext> _contexts = default!;
    private FakeSalesforceStore _store = default!;
    private List<ServiceProvider> _pods = [];
    private List<IngestRow> _events = [];
    private Dictionary<(string Type, string Key), long> _expected = [];
    private Dictionary<string, (string Account, string Related)> _relationships = [];

    // The tables as the EF model names them (only the model is read: nothing connects to this address)
    private static readonly string AccountTable = TableOf<AccountInboxRow>();
    private static readonly string RelationshipTable = TableOf<AccountRelationshipInboxRow>();
    private static readonly string[] InboxTables = [AccountTable, RelationshipTable];

    private static string TableOf<TRow>() where TRow : InboxRow
    {
        using var model = new SyncDbContext(new DbContextOptionsBuilder<SyncDbContext>().UseNpgsql("Host=unused").Options);
        return model.TableOf<TRow>();
    }

    /// <summary>An event as the consumer stores it: the inbox table of its type, the entity's id and the raw payload.</summary>
    private sealed record IngestRow(string Table, string EntityId, string Payload);

    /// <summary>
    /// meta of version N of an entity: time = base time + N ms, so per-entity time order = version order; the first
    /// version is the entity's insert, the later ones updates.
    /// </summary>
    private static readonly DateTimeOffset BaseTime = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private static JsonObject Meta(long version)
    {
        return new()
        {
            ["time"] = BaseTime.AddMilliseconds(version).ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'", CultureInfo.InvariantCulture),
            ["type"] = version == 1 ? "inserted" : "updated"
        };
    }

    [GlobalSetup]
    public void GlobalSetup()
    {
        _db = NpgsqlDataSource.Create(BenchmarkDb.ConnectionString);
        _contexts = SyncDbContext.CreateFactory(_db);
        InboxSchema.EnsureCreatedAsync(_contexts, InboxTables).GetAwaiter().GetResult();
        _store = new FakeSalesforceStore(new FakeSalesforceOptions
        {
            Latency = TimeSpan.FromMilliseconds(LatencyMs),
            EnforceDailyLimit = false
        });

        _pods = [.. Enumerable.Range(0, Pods).Select(_ => BuildPod())];
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        foreach (var pod in _pods)
        {
            pod.Dispose();
        }

        _db.Dispose();
    }

    [IterationSetup]
    public void IterationSetup()
    {
        SetupAsync().GetAwaiter().GetResult();
    }

    [IterationCleanup]
    public void IterationCleanup()
    {
        VerifyAsync().GetAwaiter().GetResult();
    }

    protected async Task DrainAsync()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5));
        var ingest = StreamIngest ? Task.Run(() => IngestAsync(cts.Token)) : Task.CompletedTask;
        var loops = _pods.Select(pod => Task.Run(() => WorkerLoopAsync(pod, ingest, cts.Token)));
        await Task.WhenAll(loops.Append(ingest));
    }

    /// <summary>Inserts the events in small chunks, in per-account version order, as fast as possible (like the Kafka consumer).</summary>
    private async Task IngestAsync(CancellationToken ct)
    {
        await using var db = await _contexts.CreateDbContextAsync(ct);
        foreach (var chunk in _events.Chunk(IngestChunk))
        {
            foreach (var table in chunk.GroupBy(e => e.Table))
            {
                await db.Database.ExecuteAsync(
                    $"insert into {table.Key} (entity_id, payload) select e, p::jsonb from unnest(@e::text[], @p::text[]) as u(e, p)", ct,
                    Param("e", table.Select(r => r.EntityId).ToArray()), Param("p", table.Select(r => r.Payload).ToArray()));
            }
        }
    }

    private async Task WorkerLoopAsync(ServiceProvider pod, Task ingest, CancellationToken ct)
    {
        var scopes = pod.GetRequiredService<IServiceScopeFactory>();
        while (true)
        {
            ct.ThrowIfCancellationRequested();

            // The worker's own pass: one batch of every registered entity type (accounts, relationships)
            var processed = await InboxWorkerService.SyncOnceAsync(scopes, ct);

            if (processed > 0)
            {
                continue;
            }

            if (ingest.IsFaulted)
            {
                return;
            }

            if (ingest.IsCompleted && !await HasPendingAsync(ct))
            {
                return;
            }

            await Task.Delay(2, ct); // the rest is backing off, or more events are on the way
        }
    }

    private async Task<bool> HasPendingAsync(CancellationToken ct)
    {
        await using var db = await _contexts.CreateDbContextAsync(ct);
        return await db.Database.QuerySingleAsync<bool>(
            string.Join(" or ", InboxTables.Select(t => $"exists (select 1 from {t} where status = 'pending')")).Insert(0, "select "), ct);
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
            o.BatchSize = StreamIngest ? 10 : 75; // streaming: small batches, so entities are read again and again
            // A relationship can be read before its Accounts are in the org (DEPENDENCY_NOT_SYNCED): retry it
            // within milliseconds rather than the production 30 s, so the benchmark measures the pipeline
            o.RetryBaseDelay = TimeSpan.FromMilliseconds(10);
            o.MaxRetryDelay = TimeSpan.FromMilliseconds(500);
            o.MaxRetries = 50;
        });
        services.Configure<RateLimitOptions>(_ => { });
        // Required by the limiter (0 is a budget of nothing); the budget is then the mock org's reported limit
        services.Configure<SalesforceLimitsOptions>(o => o.DailyApiAllocation = 1_000_000_000);

        services.AddSalesforceClient()
            .AddSalesforceRateLimiting() // the bucket is opened up per iteration (SetupAsync)
            .ConfigurePrimaryHttpMessageHandler(sp => new FakeSalesforceHandler(sp.GetRequiredService<FakeSalesforceStore>()));
        services.AddInboxSync();
        return services.BuildServiceProvider();
    }

    private async Task SetupAsync()
    {
        _store.Reset();

        await using var db = await _contexts.CreateDbContextAsync();
        await db.Database.OpenConnectionAsync();
        foreach (var table in InboxTables)
        {
            await db.Database.ExecuteAsync($"truncate {table} restart identity", default);
        }
        await db.Database.ExecuteAsync("truncate sf_sync_state; truncate sf_api_usage", default);
        // Effectively unlimited bucket and no daily budget: measure the pipeline, not the throttle
        await db.Database.ExecuteAsync("""
            update sf_rate_limit
            set tokens = 1e9, capacity = 1e9, base_refill_per_sec = 1e9, refill_per_sec = 1e9,
                daily_budget = null, paused_until = null, updated_at = clock_timestamp()
            where name = 'salesforce'
            """, default);

        (_events, _expected) = Generate();
        if (!StreamIngest)
        {
            await PreloadAsync((NpgsqlConnection)db.Database.GetDbConnection());
        }
    }

    /// <summary>
    /// Random account per event with occasional bursts, versions increasing per entity. About 15% of events change
    /// a relationship between two accounts that already have an event (as in Kafka, the account event comes first,
    /// but a worker may still read the relationship before the account has synced).
    /// </summary>
    private (List<IngestRow> Events, Dictionary<(string, string), long> Newest) Generate()
    {
        var random = new Random(42);
        var versions = new Dictionary<(string, string), long>();
        var seen = new List<string>();
        var events = new List<IngestRow>(Events);
        _relationships = new Dictionary<string, (string, string)>(StringComparer.Ordinal);

        while (events.Count < Events)
        {
            if (seen.Count > 1 && random.NextDouble() < RelationshipShare)
            {
                var a = seen[random.Next(seen.Count)];
                var b = seen[random.Next(seen.Count)];
                if (a == b)
                {
                    continue;
                }

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
                        ["name"] = $"v{relVersion}",
                        ["type"] = "Partner",
                        ["isActive"] = true
                    }
                };
                events.Add(new IngestRow(RelationshipTable, relKey, relPayload.ToJsonString()));
                continue;
            }

            var key = $"ACC-{random.Next(Accounts):D6}";
            var burst = random.NextDouble() < 0.1 ? random.Next(2, 6) : 1;

            for (var i = 0; i < burst && events.Count < Events; i++)
            {
                var version = versions[(AccountSyncService.Type, key)] = versions.GetValueOrDefault((AccountSyncService.Type, key)) + 1;
                if (version == 1)
                {
                    seen.Add(key);
                }
                // No profile.externalId: the Profile__c takes the account's
                var payload = new JsonObject
                {
                    ["meta"] = Meta(version),
                    [AccountSyncService.Type] = new JsonObject
                    {
                        ["externalId"] = key,
                        ["name"] = $"Account {key}",
                        ["description"] = $"v{version}",
                        ["profile"] = new JsonObject { ["score"] = version }
                    }
                };
                events.Add(new IngestRow(AccountTable, key, payload.ToJsonString()));
            }
        }

        return (events, versions);
    }

    private async Task PreloadAsync(NpgsqlConnection conn)
    {
        foreach (var table in _events.GroupBy(e => e.Table))
        {
            await using var importer = await conn.BeginBinaryImportAsync(
                $"copy {table.Key} (entity_id, payload) from stdin (format binary)");
            foreach (var e in table)
            {
                await importer.StartRowAsync();
                await importer.WriteAsync(e.EntityId, NpgsqlDbType.Text);
                await importer.WriteAsync(e.Payload, NpgsqlDbType.Jsonb);
            }
            await importer.CompleteAsync();
        }
    }

    // ---------- Verification ----------

    private async Task VerifyAsync()
    {
        var wrong = new List<string>();
        var regressions = 0;
        var duplicates = 0;
        foreach (var ((type, acct), expected) in _expected)
        {
            if (type == AccountRelationshipSyncService.Type)
            {
                if (!VerifyRelationship(acct, expected, ref regressions, ref duplicates, out var problem))
                {
                    wrong.Add(problem);
                }

                continue;
            }

            // Versions in the order the org received them: Description "vN" on the Account, Score__c = N on the Profile
            var accountVersions = (_store.History("Account", acct) ?? [])
                .Where(w => w.ContainsKey("Description"))
                .Select(w => long.Parse(((string)w["Description"]!)[1..])).ToList();
            var profileVersions = (_store.History("Profile__c", acct) ?? [])
                .Where(w => w.ContainsKey("Score__c"))
                .Select(w => Number(w["Score__c"])).ToList();

            CountSteps(accountVersions, ref regressions, ref duplicates);
            CountSteps(profileVersions, ref regressions, ref duplicates);

            if (accountVersions.LastOrDefault() != expected || profileVersions.LastOrDefault() != expected)
            {
                wrong.Add($"{acct}: expected v{expected}, account v{accountVersions.LastOrDefault()}, profile v{profileVersions.LastOrDefault()}");
            }
        }

        await using var db = await _contexts.CreateDbContextAsync();
        async Task<long> Count(string sql)
        {
            return await db.Database.QuerySingleAsync<long>(sql, default);
        }
        // One query over both inbox tables; "superseded" is a normal end for an older event read with a newer one
        var inbox = string.Join(" union all ", InboxTables.Select(t => $"select status, retry_count from {t}"));
        var rows = await Count($"select count(*) from ({inbox}) i");
        var notDone = await Count($"select count(*) from ({inbox}) i where status not in ('done', 'superseded')");
        var superseded = await Count($"select count(*) from ({inbox}) i where status = 'superseded'");
        // A done row is an entity synced from its newest event in a batch; the superseded rows are the events coalesced
        // into it (or dropped by the watermark). Events per done row is therefore the coalescing, per entity sync:
        // counting records written instead would mix in that an account event writes two records (Account, Profile).
        var done = await Count($"select count(*) from ({inbox}) i where status = 'done'");
        // retry_count counts failed attempts. Mostly relationships read before their Accounts were in the org
        // (DEPENDENCY_NOT_SYNCED).
        var retries = await Count($"select coalesce(sum(retry_count), 0) from ({inbox}) i");
        var relationshipEvents = _events.Count(e => e.Table == RelationshipTable);

        var stats = _store.GetStats();
        Console.WriteLine(
            $"// [{(StreamIngest ? "streaming" : "preloaded")} pods={Pods} workers=1 accounts={Accounts} latency={LatencyMs}ms] " +
            $"events={Events} relationshipEvents={relationshipEvents} {stats} done={done} superseded={superseded} " +
            $"coalescing={(double)Events / Math.Max(1, done):F2} events/entity sync " +
            $"retries={retries} notDone={notDone} wrong={wrong.Count} regressions={regressions} duplicates={duplicates}");

        if (wrong.Count > 0 || rows != Events || notDone > 0 || regressions > 0 || duplicates > 0)
        {
            throw new InvalidOperationException(
                $"Correctness check failed: {wrong.Count} entities not at newest version, {notDone} of {rows}/{Events} " +
                $"inbox rows not done, {regressions} times a record received an older version after a newer one, " +
                $"{duplicates} times a record received the same version again. " + string.Join("; ", wrong.Take(5)));
        }
    }

    private const string RelationshipObject = "Account_Relationship__c";

    /// <summary>Newest version (Name "vN"), only moving forward, and linked to the right two Accounts.</summary>
    private bool VerifyRelationship(string key, long expected, ref int regressions, ref int duplicates, out string problem)
    {
        problem = "";
        var versions = (_store.History(RelationshipObject, key) ?? [])
            .Where(w => w.ContainsKey("Name"))
            .Select(w => long.Parse(((string)w["Name"]!)[1..])).ToList();
        CountSteps(versions, ref regressions, ref duplicates);

        var (account, related) = _relationships[key];
        var record = _store.FindByExternalId(RelationshipObject, key);
        var accountId = (string?)_store.FindByExternalId("Account", account)?["Id"];
        var relatedId = (string?)_store.FindByExternalId("Account", related)?["Id"];
        if (versions.LastOrDefault() == expected && record is not null && accountId is not null &&
            (string?)record["Account__c"] == accountId && (string?)record["Related_Account__c"] == relatedId)
        {
            return true;
        }

        problem = $"{key}: expected v{expected} linking {account}->{related}, got v{versions.LastOrDefault()} " +
                  $"linking {record?["Account__c"]}->{record?["Related_Account__c"]}";
        return false;
    }

    /// <summary>
    /// A record's versions in write order: a step back is a regression (an older event written after a newer one), a
    /// version written again a duplicate (two workers wrote the same event).
    /// </summary>
    private static void CountSteps(List<long> versions, ref int regressions, ref int duplicates)
    {
        var steps = versions.Zip(versions.Skip(1)).ToList();
        regressions += steps.Count(p => p.Second < p.First);
        duplicates += steps.Count(p => p.Second == p.First);
    }

    private static long Number(JsonNode? node)
    {
        return (long)decimal.Parse(node!.ToJsonString(), NumberStyles.Float, CultureInfo.InvariantCulture);
    }
}

/// <summary>Whole inbox loaded up front, then drained.</summary>
public class InboxDrainBenchmarks : DrainBenchmarkBase
{
    [Params(1)] public override int Pods { get; set; }
    [Params(5_000)] public override int Events { get; set; }
    [Params(100, 1_000)] public override int Accounts { get; set; }
    [Params(0, 25)] public override int LatencyMs { get; set; }
    protected override bool StreamIngest => false;

    [Benchmark(Description = "Drain inbox")]
    public Task DrainInbox()
    {
        return DrainAsync();
    }
}

/// <summary>
/// The race demonstration: three sequential workers (pods) drain one inbox. Nothing keeps them off each other's
/// entities, so the correctness check is expected to fail; its stats line shows how (duplicates, regressions, wrong
/// versions). The full version on main prevents this with leases and the per-entity claim lock.
/// </summary>
public class UnsafeMultiPodDrainBenchmarks : DrainBenchmarkBase
{
    [Params(3)] public override int Pods { get; set; }
    [Params(5_000)] public override int Events { get; set; }
    [Params(100, 1_000)] public override int Accounts { get; set; }
    [Params(0, 25)] public override int LatencyMs { get; set; }
    protected override bool StreamIngest => false;

    [Benchmark(Description = "Drain inbox, 3 unsynchronized workers")]
    public Task DrainInbox()
    {
        return DrainAsync();
    }
}

/// <summary>
/// Events keep arriving (in per-account version order, like Kafka) while the worker drains, so the same account is
/// read again and again in small batches.
/// </summary>
public class StreamingIngestBenchmarks : DrainBenchmarkBase
{
    [Params(1)] public override int Pods { get; set; }
    [Params(5_000)] public override int Events { get; set; }
    [Params(50)] public override int Accounts { get; set; }
    [Params(5)] public override int LatencyMs { get; set; }
    protected override bool StreamIngest => true;

    [Benchmark(Description = "Drain while ingesting")]
    public Task DrainWhileIngesting()
    {
        return DrainAsync();
    }
}