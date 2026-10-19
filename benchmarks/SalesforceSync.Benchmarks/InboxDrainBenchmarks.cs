using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Engines;
using BenchmarkDotNet.Jobs;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using NpgsqlTypes;
using SalesforceSync.Data;
using SalesforceSync.Data.Inbox;
using SalesforceSync.FakeSalesforce;
using SalesforceSync.Inbox;
using SalesforceSync.Infrastructure.Salesforce;
using SalesforceSync.Sync;

namespace SalesforceSync.Benchmarks;

/// <summary>
/// End-to-end: put N account aggregates (<see cref="AggregateWorkload"/>: Account changes, relationships and contacts
/// added, changed, removed and re-added, a few whole accounts deleted), split by <see cref="AccountAggregate.TrySplit"/>
/// into the per-entity events the consumer stores, into the real Postgres inboxes, and drain them with P "pods" x W
/// parallel worker loops. A pod is wired exactly like the worker
/// (<see cref="SyncServiceCollectionExtensions"/>, rate limiter included); its loops run the worker's own pass
/// (<see cref="InboxWorkerService.SyncOnceAsync"/>) over every registered entity type. Only the Salesforce HTTP
/// endpoint is fake. A pod is an independent service provider (own connection pool, rate limiter, HTTP client), like
/// a separate process. All pods share the Postgres inbox and one Salesforce org.
/// <para>
/// Each iteration also verifies correctness. The org rejects no out-of-order write, so a race would land. Checks:
/// every account, relationship and contact the aggregates end with is in the org at its newest version (a
/// relationship or contact names the right Accounts); every one they removed or deleted is absent; the org's write
/// history shows each record's versions only moving forward; the claim audit (sql/claim-audit.sql) shows no two
/// overlapping claims on the same entity; every entity's one inbox row holds its newest event and ends done.
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

    /// <summary>Account aggregates (Kafka messages); each is split into one or more entity events.</summary>
    public abstract int Events { get; set; }

    /// <summary>
    /// Fewer accounts = more updates per account = more coalescing (events overwriting an unclaimed row) and ordering
    /// pressure.
    /// </summary>
    public abstract int Accounts { get; set; }

    /// <summary>Simulated Salesforce round trip.</summary>
    public abstract int LatencyMs { get; set; }

    /// <summary>False: the whole inbox is loaded before draining. True: events keep arriving while pods drain.</summary>
    protected abstract bool StreamIngest { get; }

    private NpgsqlDataSource _db = default!;
    private IDbContextFactory<SyncDbContext> _contexts = default!;
    private FakeSalesforceStore _store = default!;
    private List<ServiceProvider> _pods = [];
    private Dictionary<string, ISalesforceEntitySync> _syncs = [];
    private List<IngestRow> _events = [];
    private AggregateWorkload _workload = default!;
    private Dictionary<string, DateTimeOffset> _newest = [];

    // The tables as the EF model names them (only the model is read: nothing connects to this address)
    private static readonly Dictionary<string, string> InboxOf = FromModel(model => new Dictionary<string, string>
    {
        [AccountSync.Type] = model.TableOf<AccountInboxRow>(),
        [AccountRelationshipSync.Type] = model.TableOf<AccountRelationshipInboxRow>(),
        [ContactSync.Type] = model.TableOf<ContactInboxRow>()
    });
    // Every inbox of the model: AddInboxSync runs a worker per synced object, and each one claims from its own table
    private static readonly string[] InboxTables = FromModel(model => model.InboxTables.ToArray());

    private static T FromModel<T>(Func<SyncDbContext, T> read)
    {
        using var model = SyncDbContext.ForModel();
        return read(model);
    }

    /// <summary>
    /// An entity event as the consumer stores it: entity type and its inbox table, entity id, event time (the
    /// aggregate's meta.time) and payload.
    /// </summary>
    private sealed record IngestRow(string EntityType, string Table, string EntityId, DateTimeOffset EventTime, string Payload);

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
            EnforceDailyLimit = false,
            // The pods have no OAuth step (no auth handler): not what is measured
            RequireAccessToken = false
        });

        _pods = [.. Enumerable.Range(0, Pods).Select(_ => BuildPod())];
        // What the consumer reads and stores an entity event with, by entity type
        _syncs = _pods[0].GetServices<ISalesforceEntitySync>().ToDictionary(s => s.EntityType);
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

    /// <summary>
    /// Stores the entity events one at a time, aggregate after aggregate, as fast as possible, like the Kafka
    /// consumer (<see cref="ISalesforceEntitySync.StoreEventAsync"/>).
    /// </summary>
    private async Task IngestAsync(CancellationToken ct)
    {
        foreach (var e in _events)
        {
            await _syncs[e.EntityType].StoreEventAsync(e.EntityId, e.EventTime, e.Payload, ct);
        }
    }

    private async Task WorkerLoopAsync(ServiceProvider pod, int loop, Task ingest, CancellationToken ct)
    {
        var scopes = pod.GetRequiredService<IServiceScopeFactory>();
        for (var iteration = 0; ; iteration++)
        {
            ct.ThrowIfCancellationRequested();

            // The worker's own pass: one batch of every registered entity type (accounts, relationships, contacts)
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
            "SELECT " + string.Join(" OR ", InboxTables.Select(t => $"EXISTS (SELECT 1 FROM {t} WHERE next_attempt_at IS NOT NULL)")), ct);
    }

    // ---------- Setup ----------

    /// <summary>
    /// One "pod": own connection pool, limiter and HTTP stack; shares Postgres and the org. Wired with the worker's
    /// registrations; only options, data source and primary HTTP handler (the mock org) differ.
    /// </summary>
    private ServiceProvider BuildPod()
    {
        var services = new ServiceCollection();
        services.AddLogging(); // no providers: silent
        services.AddSingleton(_ => NpgsqlDataSource.Create(BenchmarkDb.ConnectionString));
        services.AddSingleton(_store);

        services.Configure<SalesforceOptions>(o =>
        {
            o.InstanceUrl = "https://fake.my.salesforce.com";
            o.BaseApiPath = "/services/data";
            o.ApiVersion = "v62.0";
        });
        services.Configure<SyncOptions>(o =>
        {
            o.Workers = Workers;
            if (StreamIngest)
            {
                o.BatchSize = 10;     // streaming: small batches, so entities are claimed again and again
            }

            o.Lease = TimeSpan.FromMinutes(5);
            // A relationship can be claimed before its Accounts are in the org (DEPENDENCY_NOT_SYNCED): retry it
            // within milliseconds rather than the production 30 s, so the benchmark measures the pipeline
            o.RetryBaseDelay = TimeSpan.FromMilliseconds(10);
            o.MaxRetryDelay = TimeSpan.FromMilliseconds(500);
            o.MaxRetries = 50;
        });
        services.AddSalesforceClient()
            .AddSalesforceRateLimiting()
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
            await db.Database.ExecuteAsync($"TRUNCATE {table} RESTART IDENTITY", default);
        }
        await db.Database.ExecuteAsync("TRUNCATE sf_claim_audit", default);

        (_workload, _events) = Generate(Events, Accounts, _syncs);
        // Per entity, its newest event: what its one inbox row must end with
        _newest = _events.GroupBy(e => e.EntityId).ToDictionary(g => g.Key, g => g.Max(e => e.EventTime), StringComparer.Ordinal);
        if (!StreamIngest)
        {
            await PreloadAsync((NpgsqlConnection)db.Database.GetDbConnection());
        }
    }

    /// <summary>
    /// The workload's aggregates as the entity events the consumer stores, in publish order: split by
    /// <see cref="AccountAggregate.TrySplit"/>, read by their type's sync (entity id, time) as the consumer does. A
    /// relationship or contact may be claimed before its Account is in the org (DEPENDENCY_NOT_SYNCED, retried).
    /// </summary>
    private static (AggregateWorkload Workload, List<IngestRow> Events) Generate(int aggregates, int accounts,
        Dictionary<string, ISalesforceEntitySync> syncs)
    {
        var workload = AggregateWorkload.Generate(aggregates, accounts);
        var events = new List<IngestRow>();
        foreach (var aggregate in workload.Aggregates)
        {
            if (!AccountAggregate.TrySplit(aggregate, out var split, out var problem))
            {
                throw new InvalidOperationException($"The workload made an invalid aggregate: {problem}");
            }

            foreach (var e in split)
            {
                if (!syncs[e.EntityType].TryReadEvent(e.Json, out var entityId, out var time, out problem))
                {
                    throw new InvalidOperationException($"The split made an invalid {e.EntityType} event: {problem}");
                }

                events.Add(new IngestRow(e.EntityType, InboxOf[e.EntityType], entityId!, time, e.Json));
            }
        }

        return (workload, events);
    }

    /// <summary>
    /// Loads the events to match storing them one by one (InboxRepository.AddAsync): per entity one row with its
    /// newest event (of two equally new, the one stored later), entity due. Copied into a temporary table first,
    /// since one insert can't overwrite a row twice.
    /// </summary>
    private async Task PreloadAsync(NpgsqlConnection conn)
    {
        foreach (var inbox in _events.GroupBy(e => e.Table))
        {
            await using (var stage = new NpgsqlCommand(
                             "CREATE TEMP TABLE preload (seq bigserial, entity_id text, event_time timestamptz, payload jsonb)", conn))
            {
                await stage.ExecuteNonQueryAsync();
            }
            await using (var importer = await conn.BeginBinaryImportAsync(
                             "COPY preload (entity_id, event_time, payload) FROM STDIN (FORMAT binary)"))
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
            await using var store = new NpgsqlCommand($"""
                INSERT INTO {inbox.Key} (entity_id, event_time, payload, next_attempt_at)
                SELECT DISTINCT ON (entity_id) entity_id, event_time, payload, now() FROM preload ORDER BY entity_id, event_time DESC, seq DESC;
                DROP TABLE preload;
                """, conn);
            await store.ExecuteNonQueryAsync();
        }
    }

    // ---------- Verification ----------

    private async Task VerifyAsync()
    {
        var (wrong, regressions) = _workload.Check(_store);

        await using var db = await _contexts.CreateDbContextAsync();
        async Task<long> Count(string sql)
        {
            return await db.Database.QuerySingleAsync<long>(sql, default);
        }
        // One query over both inbox tables, one row per entity
        var inbox = string.Join(" UNION ALL ", InboxTables.Select(t => $"SELECT entity_id, event_time, next_attempt_at, attempt_count FROM {t}"));
        var rows = await Count($"SELECT count(*) FROM ({inbox}) i");
        var notDone = await Count($"SELECT count(*) FROM ({inbox}) i WHERE next_attempt_at IS NOT NULL");
        // Every entity's row holds its newest event (the entity ids of the types never collide: ACC-, REL- and CON-)
        var stale = (await db.Database.QueryAsync<StoredEvent>($"SELECT entity_id AS EntityId, event_time AS EventTime FROM ({inbox}) i", default))
            .Count(r => _newest.GetValueOrDefault(r.EntityId) != r.EventTime);
        // An entity claim takes the entity's row once its newest events have overwritten it, so entity events per claim
        // is the coalescing (claims also count retries and deferrals). Counting records written instead would mix in
        // that an account event writes two records (Account, Profile).
        var claims = await Count("SELECT count(*) FROM sf_claim_audit");
        var overlaps = await Count("SELECT count(*) FROM sf_claim_overlaps");
        // attempt_count counts the failures of the row's event, so retries = attempt_count (of the last event
        // only: a newer event starts again at 0). Mostly relationships claimed before their Accounts were in the org
        // (DEPENDENCY_NOT_SYNCED).
        var retries = await Count($"SELECT coalesce(sum(attempt_count), 0) FROM ({inbox}) i");
        var relationshipEvents = _events.Count(e => e.EntityType == AccountRelationshipSync.Type);
        var contactEvents = _events.Count(e => e.EntityType == ContactSync.Type);
        // A delete is the split's event with meta.type deleted (no payload field is named type with that value)
        var deleteEvents = _events.Count(e => e.Payload.Contains("\"type\":\"deleted\"", StringComparison.Ordinal));
        var deleted = _workload.Final.Values.Count(e => !e.Present);

        var stats = _store.GetStats();
        Console.WriteLine(
            $"// [{(StreamIngest ? "streaming" : "preloaded")} pods={Pods} workers={Workers} accounts={Accounts} latency={LatencyMs}ms] " +
            $"events={Events} entityEvents={_events.Count} relationshipEvents={relationshipEvents} contactEvents={contactEvents} " +
            $"deleteEvents={deleteEvents} deletedEntities={deleted} {stats} entities={rows} " +
            $"coalescing={(double)_events.Count / Math.Max(1, claims):F2} events/entity claim " +
            $"claims={claims} retries={retries} overlappingClaims={overlaps} notDone={notDone} stale={stale} " +
            $"wrong={wrong.Count} regressions={regressions}");

        if (wrong.Count > 0 || rows != _newest.Count || stale > 0 || notDone > 0 || overlaps > 0 || regressions > 0)
        {
            throw new InvalidOperationException(
                $"Correctness check failed: {wrong.Count} entities not at newest version or not deleted, {rows} inbox rows for {_newest.Count} " +
                $"entities, {stale} not at the newest event, {notDone} not done, {overlaps} overlapping claims, {regressions} " +
                "entities received an older version after a newer one. " + string.Join("; ", wrong.Take(5)));
        }
    }

    /// <summary>An inbox row's entity and event time, as the verification reads them.</summary>
    private sealed record StoredEvent(string EntityId, DateTimeOffset EventTime);
}

/// <summary>Whole inbox loaded up front, then drained.</summary>
public class InboxDrainBenchmarks : DrainBenchmarkBase
{
    [ParamsSource(nameof(WorkerCounts))] public override int Workers { get; set; }
    [Params(1, 3)] public override int Pods { get; set; }
    [Params(5_000)] public override int Events { get; set; }
    [ParamsSource(nameof(AccountCounts))] public override int Accounts { get; set; }
    [ParamsSource(nameof(Latencies))] public override int LatencyMs { get; set; }
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
/// Aggregates keep arriving (in publish order, like Kafka, each split and stored as the consumer does) while the
/// pods drain. The same entity is claimed, released and claimed again many times by different workers in different
/// pods. Its row is overwritten while a worker writes the event it claimed, including a delete over an upsert and an
/// upsert over a delete: the races the lease and the outcomes' check of the claimed event time must handle.
/// </summary>
public class StreamingIngestBenchmarks : DrainBenchmarkBase
{
    [ParamsSource(nameof(WorkerCounts))] public override int Workers { get; set; }
    [ParamsSource(nameof(PodCounts))] public override int Pods { get; set; }
    [Params(5_000)] public override int Events { get; set; }
    [Params(50)] public override int Accounts { get; set; }
    [Params(5)] public override int LatencyMs { get; set; }
    protected override bool StreamIngest => true;

    public static int[] WorkerCounts => BenchmarkRun.IsQuick ? [4] : [4, 8];
    public static int[] PodCounts => BenchmarkRun.IsQuick ? [1] : [1, 3];

    [Benchmark(Description = "Drain while ingesting")]
    public Task DrainWhileIngesting()
    {
        return DrainAsync();
    }
}