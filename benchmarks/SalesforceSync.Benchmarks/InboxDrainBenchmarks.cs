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
/// every inbox row must end done or superseded (an older event claimed together with a newer one of its entity).
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
                .WithIterationCount(BenchmarkRun.IsQuick ? 3 : 5)
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

    /// <summary>
    /// <see cref="SyncOptions.CoalesceDelay"/>. Only events stored while the pods drain get it: preloaded entities are
    /// due at once.
    /// </summary>
    public abstract int CoalesceDelayMs { get; set; }

    /// <summary>False: the whole inbox is loaded before draining. True: events keep arriving while pods drain.</summary>
    protected abstract bool StreamIngest { get; }

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
    // Every inbox of the model, not only the two the events go to: AddInboxSync runs a worker per synced object, and
    // each one claims from its own table (the others stay empty)
    private static readonly string[] InboxTables = AllInboxTables();

    private static string TableOf<TRow>() where TRow : InboxRow
    {
        using var model = Model();
        return model.TableOf<TRow>();
    }

    private static string[] AllInboxTables()
    {
        using var model = Model();
        return [.. model.InboxTables];
    }

    private static SyncDbContext Model()
    {
        return new SyncDbContext(new DbContextOptionsBuilder<SyncDbContext>().UseNpgsql("Host=unused").Options);
    }

    /// <summary>
    /// An event as the consumer stores it: the inbox table and entity type (in sf_sync_state) of its type, the entity's
    /// id, the event's time (its meta.time) and the raw payload.
    /// </summary>
    private sealed record IngestRow(string Table, string Type, string EntityId, DateTimeOffset EventTime, string Payload);

    /// <summary>
    /// meta of version N of an entity: time = base time + N ms, so per-entity time order = version order; the first
    /// version is the entity's insert, the later ones updates.
    /// </summary>
    private static readonly DateTimeOffset BaseTime = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private static DateTimeOffset Time(long version)
    {
        return BaseTime.AddMilliseconds(version);
    }

    private static JsonObject Meta(long version)
    {
        return new()
        {
            ["time"] = Time(version).ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'", CultureInfo.InvariantCulture),
            ["type"] = version == 1 ? "inserted" : "updated"
        };
    }

    [GlobalSetup]
    public void GlobalSetup()
    {
        _db = NpgsqlDataSource.Create(BenchmarkDb.ConnectionString);
        _contexts = SyncDbContext.CreateFactory(_db);
        SyncSchema.MigrateAsync(_contexts).GetAwaiter().GetResult();
        // A plain command: EF's raw SQL formats {...} as parameter placeholders, and the script has JSON literals
        using (var audit = _db.CreateCommand(File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "claim-audit.sql"))))
        {
            audit.ExecuteNonQuery();
        }

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
        var loops = _pods.SelectMany(pod => Enumerable.Range(0, Workers)
            .Select(loop => Task.Run(() => WorkerLoopAsync(pod, loop, ingest, cts.Token))));
        await Task.WhenAll(loops.Append(ingest));
    }

    /// <summary>Stores the events one at a time, in per-account version order, as fast as possible (like the Kafka consumer).</summary>
    private async Task IngestAsync(CancellationToken ct)
    {
        await using var db = await _contexts.CreateDbContextAsync(ct);
        foreach (var e in _events)
        {
            await db.Database.ExecuteAsync(InboxSql.For(e.Table).Store, ct, new
            {
                entityType = e.Type,
                entityId = e.EntityId,
                eventTime = e.EventTime,
                payload = e.Payload,
                coalesceDelay = TimeSpan.FromMilliseconds(CoalesceDelayMs)
            });
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

            await Task.Delay(2, ct); // other workers hold the remaining accounts, or more events are on the way
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
            o.Workers = Workers;
            o.MaxConcurrentRequests = Workers * Pods; // global cap across all pods
            o.BatchSize = StreamIngest ? 10 : 75;     // streaming: small batches, so entities are claimed again and again
            o.Lease = TimeSpan.FromMinutes(5);
            o.CoalesceDelay = TimeSpan.FromMilliseconds(CoalesceDelayMs);
            // A relationship can be claimed before its Accounts are in the org (DEPENDENCY_NOT_SYNCED): retry it
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
        await db.Database.ExecuteAsync("truncate sf_sync_state; truncate sf_api_usage; truncate sf_claim_audit", default);
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
    /// but a worker may still claim the relationship before the account has synced).
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
                var relVersion = versions[(AccountRelationshipSync.Type, relKey)] =
                    versions.GetValueOrDefault((AccountRelationshipSync.Type, relKey)) + 1;
                var relPayload = new JsonObject
                {
                    ["meta"] = Meta(relVersion),
                    [AccountRelationshipSync.Type] = new JsonObject
                    {
                        ["externalId"] = relKey,
                        ["accountExternalId"] = a,
                        ["relatedAccountExternalId"] = b,
                        ["name"] = $"v{relVersion}",
                        ["type"] = "Partner",
                        ["isActive"] = true
                    }
                };
                events.Add(new IngestRow(RelationshipTable, AccountRelationshipSync.Type, relKey, Time(relVersion), relPayload.ToJsonString()));
                continue;
            }

            var key = $"ACC-{random.Next(Accounts):D6}";
            var burst = random.NextDouble() < 0.1 ? random.Next(2, 6) : 1;

            for (var i = 0; i < burst && events.Count < Events; i++)
            {
                var version = versions[(AccountSync.Type, key)] = versions.GetValueOrDefault((AccountSync.Type, key)) + 1;
                if (version == 1)
                {
                    seen.Add(key);
                }
                // No profile.externalId: the Profile__c takes the account's
                var payload = new JsonObject
                {
                    ["meta"] = Meta(version),
                    [AccountSync.Type] = new JsonObject
                    {
                        ["externalId"] = key,
                        ["name"] = $"Account {key}",
                        ["description"] = $"v{version}",
                        ["profile"] = new JsonObject { ["score"] = version }
                    }
                };
                events.Add(new IngestRow(AccountTable, AccountSync.Type, key, Time(version), payload.ToJsonString()));
            }
        }

        return (events, versions);
    }

    private async Task PreloadAsync(NpgsqlConnection conn)
    {
        foreach (var inbox in _events.GroupBy(e => (e.Table, e.Type)))
        {
            await using (var importer = await conn.BeginBinaryImportAsync(
                             $"copy {inbox.Key.Table} (entity_id, event_time, payload) from stdin (format binary)"))
            {
                foreach (var e in inbox)
                {
                    await importer.StartRowAsync();
                    await importer.WriteAsync(e.EntityId, NpgsqlDbType.Text);
                    await importer.WriteAsync(e.EventTime, NpgsqlDbType.TimestampTz);
                    await importer.WriteAsync(e.Payload, NpgsqlDbType.Jsonb);
                }
                await importer.CompleteAsync();
            }
            // What storing does for each event: its entity is due
            await using var due = new NpgsqlCommand(
                $"insert into sf_sync_state (entity_type, entity_id, next_attempt_at) select distinct @type, entity_id, now() from {inbox.Key.Table}",
                conn);
            due.Parameters.AddWithValue("type", inbox.Key.Type);
            await due.ExecuteNonQueryAsync();
        }
    }

    // ---------- Verification ----------

    private async Task VerifyAsync()
    {
        var wrong = new List<string>();
        var regressions = 0;
        foreach (var ((type, acct), expected) in _expected)
        {
            if (type == AccountRelationshipSync.Type)
            {
                if (!VerifyRelationship(acct, expected, ref regressions, out var problem))
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

            if (!IsIncreasing(accountVersions) || !IsIncreasing(profileVersions))
            {
                regressions++;
            }

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
        // One query over both inbox tables; "superseded" is a normal end for an older event claimed with a newer one
        var inbox = string.Join(" union all ", InboxTables.Select(t => $"select status, attempt_count from {t}"));
        var rows = await Count($"select count(*) from ({inbox}) i");
        var notDone = await Count($"select count(*) from ({inbox}) i where status not in ('done', 'superseded')");
        var superseded = await Count($"select count(*) from ({inbox}) i where status = 'superseded'");
        // A done row is an entity synced from its newest claimed event; the superseded rows are the events coalesced
        // into it (or dropped by the watermark). Events per done row is therefore the coalescing, per entity sync:
        // counting records written instead would mix in that an account event writes two records (Account, Profile).
        var done = await Count($"select count(*) from ({inbox}) i where status = 'done'");
        var claims = await Count("select count(*) from sf_claim_audit");
        var overlaps = await Count("select count(*) from sf_claim_overlaps");
        // attempt_count counts claims of the event, so retries = attempts beyond the first. Mostly relationships
        // claimed before their Accounts were in the org (DEPENDENCY_NOT_SYNCED).
        var retries = await Count($"select coalesce(sum(greatest(attempt_count - 1, 0)), 0) from ({inbox}) i");
        var relationshipEvents = _events.Count(e => e.Table == RelationshipTable);

        var stats = _store.GetStats();
        Console.WriteLine(
            $"// [{(StreamIngest ? "streaming" : "preloaded")} pods={Pods} workers={Workers} accounts={Accounts} latency={LatencyMs}ms coalesceDelay={CoalesceDelayMs}ms] " +
            $"events={Events} relationshipEvents={relationshipEvents} {stats} done={done} superseded={superseded} " +
            $"coalescing={(double)Events / Math.Max(1, done):F2} events/entity sync " +
            $"claims={claims} retries={retries} overlappingClaims={overlaps} notDone={notDone} wrong={wrong.Count} regressions={regressions}");

        if (wrong.Count > 0 || rows != Events || notDone > 0 || overlaps > 0 || regressions > 0)
        {
            throw new InvalidOperationException(
                $"Correctness check failed: {wrong.Count} entities not at newest version, {notDone} of {rows}/{Events} " +
                $"inbox rows not done, {overlaps} overlapping claims, {regressions} entities received an older version " +
                "after a newer one. " + string.Join("; ", wrong.Take(5)));
        }
    }

    private const string RelationshipObject = "Account_Relationship__c";

    /// <summary>Newest version (Name "vN"), only moving forward, and linked to the right two Accounts.</summary>
    private bool VerifyRelationship(string key, long expected, ref int regressions, out string problem)
    {
        problem = "";
        var versions = (_store.History(RelationshipObject, key) ?? [])
            .Where(w => w.ContainsKey("Name"))
            .Select(w => long.Parse(((string)w["Name"]!)[1..])).ToList();
        if (!IsIncreasing(versions))
        {
            regressions++;
        }

        var (account, related) = _relationships[key];
        // Linked by the Accounts' external ids (Text foreign keys), both of which exist
        var record = _store.FindByExternalId(RelationshipObject, key);
        if (versions.LastOrDefault() == expected && record is not null &&
            _store.FindByExternalId("Account", account) is not null && _store.FindByExternalId("Account", related) is not null &&
            (string?)record["AccountId__c"] == account && (string?)record["RelatedAccountId__c"] == related)
        {
            return true;
        }

        problem = $"{key}: expected v{expected} linking {account}->{related}, got v{versions.LastOrDefault()} " +
                  $"linking {record?["AccountId__c"]}->{record?["RelatedAccountId__c"]}";
        return false;
    }

    private static bool IsIncreasing(List<long> versions)
    {
        return versions.Zip(versions.Skip(1)).All(p => p.First < p.Second);
    }

    private static long Number(JsonNode? node)
    {
        return (long)decimal.Parse(node!.ToJsonString(), NumberStyles.Float, CultureInfo.InvariantCulture);
    }
}

/// <summary>Whole inbox loaded up front, then drained.</summary>
public class InboxDrainBenchmarks : DrainBenchmarkBase
{
    [ParamsSource(nameof(WorkerCounts))] public override int Workers { get; set; }
    [Params(1, 3)] public override int Pods { get; set; }
    [Params(5_000)] public override int Events { get; set; }
    [ParamsSource(nameof(AccountCounts))] public override int Accounts { get; set; }
    [ParamsSource(nameof(Latencies))] public override int LatencyMs { get; set; }
    public override int CoalesceDelayMs { get; set; } // preloaded: no event is stored while draining
    protected override bool StreamIngest => false;

    public static int[] WorkerCounts => BenchmarkRun.IsQuick ? [1, 4] : [1, 4, 8];
    public static int[] AccountCounts => BenchmarkRun.IsQuick ? [100] : [100, 1_000];
    public static int[] Latencies => BenchmarkRun.IsQuick ? [0] : [0, 25];

    [Benchmark(Description = "Drain inbox")]
    public Task DrainInbox()
    {
        return DrainAsync();
    }
}

/// <summary>
/// Events keep arriving (in per-account version order, like Kafka) while the pods drain, so the same account is
/// claimed, released and claimed again many times by different workers in different pods. This is the race the
/// head-row claim must prevent.
/// </summary>
public class StreamingIngestBenchmarks : DrainBenchmarkBase
{
    [ParamsSource(nameof(WorkerCounts))] public override int Workers { get; set; }
    [ParamsSource(nameof(PodCounts))] public override int Pods { get; set; }
    [Params(5_000)] public override int Events { get; set; }
    [Params(50)] public override int Accounts { get; set; }
    [Params(5)] public override int LatencyMs { get; set; }
    [ParamsSource(nameof(CoalesceDelays))] public override int CoalesceDelayMs { get; set; }
    protected override bool StreamIngest => true;

    public static int[] WorkerCounts => BenchmarkRun.IsQuick ? [4] : [4, 8];
    public static int[] PodCounts => BenchmarkRun.IsQuick ? [1] : [1, 3];
    public static int[] CoalesceDelays => [(int)new SyncOptions().CoalesceDelay.TotalMilliseconds];

    [Benchmark(Description = "Drain while ingesting")]
    public Task DrainWhileIngesting()
    {
        return DrainAsync();
    }
}