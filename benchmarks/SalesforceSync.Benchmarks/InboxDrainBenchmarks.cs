using System.Diagnostics.CodeAnalysis;
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
using SalesforceSync.Persistence;
using SalesforceSync.Sync;
using SalesforceSync.ToSalesforce;

namespace SalesforceSync.Benchmarks;

/// <summary>
/// End-to-end: N account aggregates (<see cref="AggregateWorkload"/>: Account changes, relationships/contacts added,
/// changed, removed, re-added, a few accounts deleted), split by the types' ingests into per-entity events as the
/// consumer stores them, into real Postgres inboxes, drained by P "pods" x W parallel worker loops. Pod wired exactly
/// like the worker (<see cref="SyncServiceCollectionExtensions"/>); its loops run the worker's
/// own pass (<see cref="InboxWorkerService.SyncOnceAsync"/>) over every registered entity type. Only the Salesforce
/// HTTP endpoint is fake. Pod = independent service provider (own connection pool, HTTP client), like a
/// separate process. Pods share the Postgres inbox and one Salesforce org.
/// <para>
/// Each iteration also verifies correctness. Org rejects no out-of-order write, so a race would land. Checks: every
/// account, relationship, contact the aggregates end with is in org at newest version (relationship/contact names
/// right Accounts); every removed/deleted one absent; org write history shows each record's versions only moving
/// forward; claim audit (sql/claim-audit.sql) shows no overlapping claims on one entity; every entity's one inbox row
/// holds its newest event and ends done.
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

    /// <summary>Independent worker processes sharing inbox (Kubernetes replicas).</summary>
    public abstract int Pods { get; set; }

    /// <summary>Account aggregates (Kafka messages); each splits into ≥1 entity events.</summary>
    public abstract int Events { get; set; }

    /// <summary>
    /// Fewer accounts = more updates per account = more coalescing (events overwriting unclaimed row) and ordering
    /// pressure.
    /// </summary>
    public abstract int Accounts { get; set; }

    /// <summary>Simulated Salesforce round trip.</summary>
    public abstract int LatencyMs { get; set; }

    /// <summary>Entities per batch while streaming; null = worker default.</summary>
    protected virtual int? BatchSize => 10;

    /// <summary>False: whole inbox loaded before draining. True: events keep arriving while pods drain.</summary>
    protected abstract bool StreamIngest { get; }

    private NpgsqlDataSource _db = default!;
    protected IDbContextFactory<SyncDbContext> Contexts { get; private set; } = default!;
    protected FakeSalesforceStore Org { get; private set; } = default!;
    private List<ServiceProvider> _pods = [];
    private List<IAggregateIngest> _ingests = [];
    private List<(AggregateChange Change, int Events)> _aggregates = [];

    /// <summary>Workload aggregates as consumer reads them, and entity events each makes.</summary>
    protected IReadOnlyList<(AggregateChange Change, int Events)> Aggregates => _aggregates;
    private List<IngestRow> _events = [];
    protected IReadOnlyList<IngestRow> EntityEvents => _events;
    private AggregateWorkload _workload = default!;
    private Dictionary<string, DateTimeOffset> _newest = [];

    // Tables as EF model names them (only model read: nothing connects)
    private static readonly Dictionary<string, string> InboxOf = FromModel(model => new Dictionary<string, string>
    {
        [AccountSync.Type] = model.TableOf<AccountInboxRow>(),
        [AccountRelationshipSync.Type] = model.TableOf<AccountRelationshipInboxRow>(),
        [ContactSync.Type] = model.TableOf<ContactInboxRow>()
    });
    // Every inbox of model: AddInboxSync runs a worker per synced object, each claims from own table
    protected static readonly string[] InboxTables = FromModel(model => model.InboxTables.ToArray());

    private static T FromModel<T>(Func<SyncDbContext, T> read)
    {
        using var model = SyncDbContext.ForModel();
        return read(model);
    }

    /// <summary>
    /// Entity event as consumer stores it: entity type, its inbox table, entity id, event time (aggregate's meta.time),
    /// payload.
    /// </summary>
    protected sealed record IngestRow(string EntityType, string Table, string EntityId, DateTimeOffset EventTime, string Payload);

    [GlobalSetup]
    public void GlobalSetup()
    {
        _db = NpgsqlDataSource.Create(BenchmarkDb.ConnectionString);
        Contexts = SyncDbContext.CreateFactory(_db);
        SyncSchema.MigrateAsync(Contexts).GetAwaiter().GetResult();
        // Plain command: EF raw SQL formats {...} as parameter placeholders; script has JSON literals
        using (var audit = _db.CreateCommand(File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "claim-audit.sql"))))
        {
            audit.ExecuteNonQuery();
        }

        Org = new FakeSalesforceStore(new FakeSalesforceOptions
        {
            Latency = TimeSpan.FromMilliseconds(LatencyMs),
            EnforceDailyLimit = false,
            // Pods have no OAuth step (no auth handler): not measured
            RequireAccessToken = false
        });

        _pods = [.. Enumerable.Range(0, Pods).Select(_ => BuildPod())];
        // What consumer stores an aggregate's events with, one per entity type
        _ingests = [.. _pods[0].GetServices<IAggregateIngest>()];
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

    protected virtual TimeSpan DrainTimeout => TimeSpan.FromMinutes(5);

    protected async Task DrainAsync()
    {
        using var cts = new CancellationTokenSource(DrainTimeout);
        var ingest = StreamIngest ? Task.Run(() => IngestAsync(cts.Token)) : Task.CompletedTask;
        var loops = _pods.SelectMany(pod => Enumerable.Range(0, Workers)
            .Select(loop => Task.Run(() => WorkerLoopAsync(pod, loop, ingest, cts.Token))));
        await Task.WhenAll(loops.Append(ingest));
    }

    /// <summary>
    /// Stores aggregates one at a time, as fast as possible, like the Kafka consumer
    /// (<see cref="IAggregateIngest.IngestAsync"/>).
    /// </summary>
    protected virtual async Task IngestAsync(CancellationToken ct)
    {
        foreach (var (change, _) in _aggregates)
        {
            await IngestAsync(change, ct);
        }
    }

    protected async Task IngestAsync(AggregateChange change, CancellationToken ct)
    {
        foreach (var ingest in _ingests)
        {
            if (await ingest.IngestAsync(change, ct) is { Count: > 0 } problems)
            {
                throw new InvalidOperationException($"The {ingest.EntityType} ingest skipped an event: {problems[0]}");
            }
        }
    }

    private async Task WorkerLoopAsync(ServiceProvider pod, int loop, Task ingest, CancellationToken ct)
    {
        var scopes = pod.GetRequiredService<IServiceScopeFactory>();
        for (var iteration = 0; ; iteration++)
        {
            ct.ThrowIfCancellationRequested();

            // Worker's own pass: one batch of every registered entity type
            int processed;
            try
            {
                processed = await InboxWorkerService.SyncOnceAsync(scopes, loop + iteration, ct);
            }
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                // Dead worker holds its lease until expiry; drain would just time out
                Console.Error.WriteLine($"// WORKER DIED {ex}");
                throw;
            }

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

            await Task.Delay(2, ct); // others hold remaining accounts, or more events coming
        }
    }

    private async Task<bool> HasPendingAsync(CancellationToken ct)
    {
        await using var db = await Contexts.CreateDbContextAsync(ct);
        return await db.Database.QuerySingleAsync<bool>(
            "SELECT " + string.Join(" OR ", InboxTables.Select(t => $"EXISTS (SELECT 1 FROM {t} WHERE next_attempt_at IS NOT NULL)")), ct);
    }

    // ---------- Setup ----------

    /// <summary>
    /// One "pod": own connection pool, limiter, HTTP stack; shares Postgres and org. Worker's registrations; only
    /// options, data source, primary HTTP handler (mock org) differ.
    /// </summary>
    private ServiceProvider BuildPod()
    {
        var services = new ServiceCollection();
        services.AddLogging(); // no providers: silent
        services.AddSingleton(_ => NpgsqlDataSource.Create(BenchmarkDb.ConnectionString));
        services.AddSingleton(Org);

        services.Configure<SalesforceOptions>(o =>
        {
            o.InstanceUrl = "https://fake.my.salesforce.com";
            o.BaseApiPath = "/services/data";
            o.ApiVersion = "v62.0";
        });
        services.Configure<SyncOptions>(o =>
        {
            o.Workers = Workers;
            if (StreamIngest && BatchSize is { } size)
            {
                o.BatchSize = size;     // streaming: small batches → entities claimed again and again
            }

            o.Lease = TimeSpan.FromMinutes(5);
            // Relationship can be claimed before its Accounts are in org (DEPENDENCY_NOT_SYNCED): retry in ms, not
            // production 30 s, so benchmark measures pipeline
            o.RetryBaseDelay = TimeSpan.FromMilliseconds(10);
            o.MaxRetryDelay = TimeSpan.FromMilliseconds(500);
            o.MaxRetries = 50;
        });
        services.AddSalesforceClient()
            .ConfigurePrimaryHttpMessageHandler(sp => new FakeSalesforceHandler(sp.GetRequiredService<FakeSalesforceStore>()));
        services.AddInboxSync();
        return services.BuildServiceProvider();
    }

    private async Task SetupAsync()
    {
        Org.Reset();

        await using var db = await Contexts.CreateDbContextAsync();
        await db.Database.OpenConnectionAsync();
        foreach (var table in InboxTables)
        {
            await db.Database.ExecuteAsync($"TRUNCATE {table} RESTART IDENTITY", default);
        }
        await db.Database.ExecuteAsync("TRUNCATE sf_claim_audit", default);

        (_workload, _events, _aggregates) = Generate(Events, Accounts, _pods[0].GetRequiredService<AccountAggregate>());
        // Per entity newest event: what its one inbox row must end with
        _newest = _events.GroupBy(e => e.EntityId).ToDictionary(g => g.Key, g => g.Max(e => e.EventTime), StringComparer.Ordinal);
        if (!StreamIngest)
        {
            await PreloadAsync((NpgsqlConnection)db.Database.GetDbConnection());
        }
    }

    /// <summary>
    /// Workload aggregates as consumer reads them, and entity events they make, in publish order: split by types'
    /// ingests, each read by its type's reader (entity id, time). Relationship/contact may be claimed before its
    /// Account is in org (DEPENDENCY_NOT_SYNCED, retried).
    /// </summary>
    private static (AggregateWorkload Workload, List<IngestRow> Events, List<(AggregateChange, int)> Aggregates) Generate(
        int aggregates, int accounts, AccountAggregate accountAggregate)
    {
        var workload = AggregateWorkload.Generate(aggregates, accounts);
        var events = new List<IngestRow>();
        var changes = new List<(AggregateChange, int)>();
        foreach (var aggregate in workload.Aggregates)
        {
            if (!accountAggregate.TryRead(aggregate, out var change, out var problem))
            {
                throw new InvalidOperationException($"The workload made an invalid aggregate: {problem}");
            }

            var split = Split(change);
            changes.Add((change, split.Count));
            foreach (var e in split)
            {
                if (!TryReadEntity(e, out var entityId, out var time, out problem))
                {
                    throw new InvalidOperationException($"The split made an invalid {e.EntityType} event: {problem}");
                }

                events.Add(new IngestRow(e.EntityType, InboxOf[e.EntityType], entityId, time, e.Json));
            }
        }

        return (workload, events, changes);
    }

    private sealed record EntityEvent(string EntityType, string Json);

    // Only Project is called, which never touches the inbox, so a null inbox is safe
    private static readonly AccountIngest AccountIngest = new(null!);
    private static readonly AccountRelationshipIngest RelationshipIngest = new(null!);
    private static readonly ContactIngest ContactIngest = new(null!);

    /// <summary>Aggregate's entity events, in type order (Account, relationships, contacts).</summary>
    private static List<EntityEvent> Split(AggregateChange change)
    {
        return
        [
            .. AccountIngest.Project(change).Select(e => new EntityEvent(AccountSync.Type, AggregateProjection.Serialize(e))),
            .. RelationshipIngest.Project(change).Select(e => new EntityEvent(AccountRelationshipSync.Type, AggregateProjection.Serialize(e))),
            .. ContactIngest.Project(change).Select(e => new EntityEvent(ContactSync.Type, AggregateProjection.Serialize(e)))
        ];
    }

    private static bool TryReadEntity(EntityEvent e, [NotNullWhen(true)] out string? entityId, out DateTimeOffset time,
        [NotNullWhen(false)] out string? problem)
    {
        (entityId, time, problem) = (null, default, null);
        var ok = e.EntityType switch
        {
            AccountSync.Type => ReadOf(AccountEvent.Reader, e.Json, out entityId, out time, out problem),
            AccountRelationshipSync.Type => ReadOf(AccountRelationshipEvent.Reader, e.Json, out entityId, out time, out problem),
            ContactSync.Type => ReadOf(ContactEvent.Reader, e.Json, out entityId, out time, out problem),
            _ => false
        };
        problem ??= ok ? null : $"unknown entity type {e.EntityType}";
        return ok;
    }

    private static bool ReadOf<TEvent>(EventReader<TEvent> reader, string json, out string? entityId, out DateTimeOffset time,
        out string? problem)
    {
        var ok = reader.TryRead(json, out var read, out problem);
        (entityId, time) = (read?.EntityId, read?.Time ?? default);
        return ok;
    }

    /// <summary>
    /// Loads events to match storing one by one (InboxRepository.AddAsync): per entity one row with newest event
    /// (equally new: later stored), entity due. Via temp table: one insert can't overwrite a row twice.
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
        var (wrong, regressions) = _workload.Check(Org);

        await using var db = await Contexts.CreateDbContextAsync();
        async Task<long> Count(string sql)
        {
            return await db.Database.QuerySingleAsync<long>(sql, default);
        }
        // One query over all inbox tables, one row per entity
        var inbox = string.Join(" UNION ALL ", InboxTables.Select(t => $"SELECT entity_id, event_time, next_attempt_at, attempt_count FROM {t}"));
        var rows = await Count($"SELECT count(*) FROM ({inbox}) i");
        var notDone = await Count($"SELECT count(*) FROM ({inbox}) i WHERE next_attempt_at IS NOT NULL");
        // Every entity row holds newest event (ids never collide across types: ACC-, REL-, CON-)
        var stale = (await db.Database.QueryAsync<StoredEvent>($"SELECT entity_id AS EntityId, event_time AS EventTime FROM ({inbox}) i", default))
            .Count(r => _newest.GetValueOrDefault(r.EntityId) != r.EventTime);
        // Claim takes entity row after newest events overwrote it → entity events per claim = coalescing (claims also
        // count retries, deferrals). Counting records written would mix in account event writing two (Account, Profile).
        var claims = await Count("SELECT count(*) FROM sf_claim_audit");
        var batches = await Count("SELECT count(*) FROM (SELECT DISTINCT inbox, claim_txid FROM sf_claim_audit) b");
        var overlaps = await Count("SELECT count(*) FROM sf_claim_overlaps");
        // attempt_count = failures of row's event, so retries = attempt_count (last event only: newer event restarts
        // at 0). Mostly relationships claimed before their Accounts were in org (DEPENDENCY_NOT_SYNCED).
        var retries = await Count($"SELECT coalesce(sum(attempt_count), 0) FROM ({inbox}) i");
        var relationshipEvents = _events.Count(e => e.EntityType == AccountRelationshipSync.Type);
        var contactEvents = _events.Count(e => e.EntityType == ContactSync.Type);
        // Delete = split event with meta.type deleted (no payload field named type has that value)
        var deleteEvents = _events.Count(e => e.Payload.Contains("\"type\":\"deleted\"", StringComparison.Ordinal));
        var deleted = _workload.Final.Values.Count(e => !e.Present);

        var stats = Org.GetStats();
        Console.WriteLine(
            $"// [{(StreamIngest ? "streaming" : "preloaded")} pods={Pods} workers={Workers} accounts={Accounts} latency={LatencyMs}ms] " +
            $"events={Events} entityEvents={_events.Count} relationshipEvents={relationshipEvents} contactEvents={contactEvents} " +
            $"deleteEvents={deleteEvents} deletedEntities={deleted} {stats} entities={rows} " +
            $"coalescing={(double)_events.Count / Math.Max(1, claims):F2} events/entity claim " +
            $"claims={claims} claimBatches={batches} retries={retries} overlappingClaims={overlaps} notDone={notDone} stale={stale} " +
            $"wrong={wrong.Count} regressions={regressions}");

        if (wrong.Count > 0 || rows != _newest.Count || stale > 0 || notDone > 0 || overlaps > 0 || regressions > 0)
        {
            throw new InvalidOperationException(
                $"Correctness check failed: {wrong.Count} entities not at newest version or not deleted, {rows} inbox rows for {_newest.Count} " +
                $"entities, {stale} not at the newest event, {notDone} not done, {overlaps} overlapping claims, {regressions} " +
                "entities received an older version after a newer one. " + string.Join("; ", wrong.Take(5)));
        }
    }

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
/// Aggregates keep arriving (publish order, like Kafka, each split and stored as consumer does) while pods drain.
/// Same entity claimed, released, reclaimed many times by different workers in different pods. Its row is overwritten
/// while a worker writes the event it claimed, incl. delete over upsert and upsert over delete: races the lease and
/// the outcomes' claimed-event-time check must handle.
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