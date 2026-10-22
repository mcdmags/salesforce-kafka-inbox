using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Npgsql;
using SalesforceSync.Data;
using SalesforceSync.Data.Inbox;

namespace SalesforceSync.Benchmarks.ClaimComparison;

/// <summary>
/// Our inbox repository against the "simple claim" one (<see cref="SimpleClaimRepository"/>), both behind
/// <see cref="IRepository{TRow}"/> and driven by the same worker loop. Salesforce is a delay and an outcome per
/// entity, so only the repository differs. Each scenario tests one claim about the difference; results are printed as
/// Markdown. Run: <c>dotnet run -c Release -- --claims</c> (database <c>claimsdb</c>, created if missing).
/// </summary>
internal static class ClaimScenarios
{
    private const string SimpleTable = "simple_inbox";
    private static readonly TimeSpan Idle = TimeSpan.FromMilliseconds(20);

    public static async Task RunAsync()
    {
        var connection = await CreateDatabaseAsync("claimsdb");
        await using var data = NpgsqlDataSource.Create(connection);
        var contexts = SyncDbContext.CreateFactory(data);
        await SyncSchema.MigrateAsync(contexts);

        ISide ours = new OurSide(contexts);
        ISide simple = new SimpleSide(contexts, SimpleKind.PrimaryKeyOnly);
        ISide indexed = new SimpleSide(contexts, SimpleKind.PartialIndex);
        ISide improved = new SimpleSide(contexts, SimpleKind.Improved);

        Console.WriteLine($"# Claim comparison, {DateTime.UtcNow:yyyy-MM-dd HH:mm} UTC");
        await ClaimCostAsync([ours, simple, indexed, improved]);
        await DrainAsync([ours, simple, indexed, improved]);
        await PoisonRowsAsync([ours, indexed, improved]);
        await OutageAsync([ours, indexed, improved]);
        await NewerEventWhileSendingAsync([ours, indexed, improved]);
        await ClockSkewAsync([ours, indexed, improved]);
        await LateResultAsync([ours, indexed, improved]);
    }

    // ---------- Scenarios ----------

    /// <summary>Claim time as synced rows pile up: due rows found by an index, or by reading past every synced row.</summary>
    private static async Task ClaimCostAsync(ISide[] sides)
    {
        Header("1. Claim time as synced rows pile up",
            "180 due rows per claim, 20 claims (each batch marked done between claims, not timed). Synced rows are the table's history: entities already in Salesforce.",
            "| Synced rows | Repository | Claim median (ms) | Claim max (ms) |", "|---:|---|---:|---:|");
        foreach (var synced in new[] { 0, 100_000, 1_000_000 })
        {
            foreach (var side in sides)
            {
                await side.ResetAsync();
                await side.LoadAsync("S-", synced, synced: true, DateTimeOffset.UtcNow.AddDays(-30));
                await side.LoadAsync("D-", 180 * 20, synced: false, DateTimeOffset.UtcNow.AddDays(-1));
                await side.AnalyzeAsync();
                var repo = side.Create(new Settings(BatchSize: 180));
                var times = new List<double>();
                for (var i = 0; i < 20; i++)
                {
                    var watch = Stopwatch.StartNew();
                    var claim = await repo.ClaimAsync(CancellationToken.None);
                    times.Add(watch.Elapsed.TotalMilliseconds);
                    await repo.MarkDoneAsync(claim, claim.EntityIds, CancellationToken.None);
                }

                times.Sort();
                Row(synced.ToString("N0"), side.Name, $"{times[times.Count / 2]:F1}", $"{times[^1]:F1}");
            }
        }
    }

    /// <summary>Throughput when every send succeeds: one outcome statement per batch, or one per entity.</summary>
    private static async Task DrainAsync(ISide[] sides)
    {
        Header("2. Drain: every send succeeds",
            "20,000 due rows, no synced history, 4 workers, batch 180, Salesforce answers at once.",
            "| Repository | Time (s) | Rows/s | Outcome statements |", "|---|---:|---:|---:|");
        foreach (var side in sides)
        {
            await side.ResetAsync();
            await side.LoadAsync("D-", 20_000, synced: false, DateTimeOffset.UtcNow.AddDays(-1));
            await side.AnalyzeAsync();
            var run = new Run();
            var watch = Stopwatch.StartNew();
            await RunWorkersAsync(Workers(side, new Settings(BatchSize: 180), 4), run, TimeSpan.Zero, _ => true,
                async () => (await side.StateAsync("D-")).Synced == 20_000, TimeSpan.FromMinutes(5));
            var seconds = watch.Elapsed.TotalSeconds;
            Row(side.Name, $"{seconds:F1}", $"{20_000 / seconds:N0}", side is OurSide ? $"{run.Batches:N0} (one per batch)" : $"{run.Sends:N0} (one per entity)");
        }
    }

    /// <summary>Rows that always fail, with the oldest events: retried at once at the head of every claim, or backed off.</summary>
    private static async Task PoisonRowsAsync(ISide[] sides)
    {
        Header("3. Rows that always fail",
            "100 rows that always fail (oldest events) and 5,000 good rows. 4 workers, batch 180, 20 ms per Salesforce call, 8 attempts. Ours and the improved one back off 30 s after the first failure (our production default).",
            "| Repository | Good rows synced (s) | Sends of failing rows | Failing rows given up | Failing rows show their error |", "|---|---:|---:|---:|---|");
        foreach (var side in sides)
        {
            await side.ResetAsync();
            await side.LoadAsync("P-", 100, synced: false, DateTimeOffset.UtcNow.AddDays(-2));
            await side.LoadAsync("G-", 5_000, synced: false, DateTimeOffset.UtcNow.AddDays(-1));
            await side.AnalyzeAsync();
            var run = new Run();
            var watch = Stopwatch.StartNew();
            await RunWorkersAsync(Workers(side, new Settings(BatchSize: 180, MaxAttempts: 8, RetryBaseDelay: TimeSpan.FromSeconds(30)), 4), run, TimeSpan.FromMilliseconds(20), e => !e.EntityId.StartsWith("P-", StringComparison.Ordinal),
                async () => (await side.StateAsync("G-")).Synced == 5_000, TimeSpan.FromMinutes(2));
            var seconds = watch.Elapsed.TotalSeconds;
            var poison = await side.StateAsync("P-");
            Row(side.Name, $"{seconds:F1}", run.SendsOf("P-").ToString("N0"), poison.GivenUp.ToString("N0"), $"{await side.WithErrorAsync("P-")} of 100");
        }
    }

    /// <summary>Salesforce down for 10 s: retries spread out by backoff, or spent within the outage.</summary>
    private static async Task OutageAsync(ISide[] sides)
    {
        Header("4. Salesforce down for 10 seconds",
            "2,000 due rows, 4 workers, batch 180, 50 ms per call, every send fails for the first 10 s, then succeeds. 8 attempts. Ours and the improved one back off 1 s, doubling (scaled down from 30 s).",
            "| Repository | Synced | Given up (never claimed again) | Time to settle (s) |", "|---|---:|---:|---:|");
        foreach (var side in sides)
        {
            await side.ResetAsync();
            await side.LoadAsync("D-", 2_000, synced: false, DateTimeOffset.UtcNow.AddDays(-1));
            await side.AnalyzeAsync();
            var settings = new Settings(BatchSize: 180, MaxAttempts: 8, RetryBaseDelay: TimeSpan.FromSeconds(1));
            var outageEnds = DateTimeOffset.UtcNow.AddSeconds(10);
            var watch = Stopwatch.StartNew();
            await RunWorkersAsync(Workers(side, settings, 4), new Run(), TimeSpan.FromMilliseconds(50), _ => DateTimeOffset.UtcNow >= outageEnds,
                async () => DateTimeOffset.UtcNow >= outageEnds && (await side.StateAsync("D-")).Waiting == 0, TimeSpan.FromMinutes(2));
            var state = await side.StateAsync("D-");
            Row(side.Name, state.Synced.ToString("N0"), state.GivenUp.ToString("N0"), $"{watch.Elapsed.TotalSeconds:F1}");
        }
    }

    /// <summary>A newer event stored while its entity's older one is being sent: synced right after, or after the lease.</summary>
    private static async Task NewerEventWhileSendingAsync(ISide[] sides)
    {
        Header("5. A newer event arrives while the older one is being sent",
            "20 entities, 1 worker, batch 20, 500 ms per call, lease 20 s (the improved one: 1 min, its lease is whole minutes). 200 ms into the first send, a newer event is stored for each entity.",
            "| Repository | Newer event synced after (median, s) | Results that found no matching row |", "|---|---:|---:|");
        foreach (var side in sides)
        {
            await side.ResetAsync();
            var first = DateTimeOffset.UtcNow.AddDays(-1);
            await side.LoadAsync("N-", 20, synced: false, first);
            var repo = side.Create(new Settings(BatchSize: 20, Lease: TimeSpan.FromSeconds(20)));
            var run = new Run();
            var stored = new DateTimeOffset[20];
            var newer = Task.Run(async () =>
            {
                while (Volatile.Read(ref run.Batches) == 0)
                {
                    await Task.Delay(5);
                }

                await Task.Delay(200);
                for (var i = 0; i < 20; i++)
                {
                    await repo.AddAsync($"N-{i + 1}", first.AddMilliseconds(i + 1).AddSeconds(1), """{"v":2}""", CancellationToken.None);
                    stored[i] = DateTimeOffset.UtcNow;
                }
            });
            await RunWorkersAsync([repo], run, TimeSpan.FromMilliseconds(500), _ => true,
                () => Task.FromResult(run.Synced.Keys.Count(k => IsNewer(k.Payload)) == 20),
                TimeSpan.FromMinutes(1));
            await newer;
            var waits = Enumerable.Range(0, 20)
                .Select(i => run.Synced.FirstOrDefault(s => s.Key.EntityId == $"N-{i + 1}" && IsNewer(s.Key.Payload)).Value - stored[i])
                .Select(w => w.TotalSeconds).Order().ToList();
            Row(side.Name, $"{waits[10]:F1}", side.NoMatchingJob(repo).ToString("N0"));
        }
    }

    /// <summary>The second event's payload (jsonb writes it back with a space).</summary>
    private static bool IsNewer(string payload)
    {
        return payload.Replace(" ", "", StringComparison.Ordinal) == """{"v":2}""";
    }

    /// <summary>Two workers, one whose app clock runs 30 s ahead: does it take rows the other still holds?</summary>
    private static async Task ClockSkewAsync(ISide[] sides)
    {
        Header("6. A pod whose clock runs 30 s ahead",
            "2,000 due rows, 2 workers, batch 50, lease 20 s (the improved one: 1 min). One worker's calls take 2 s; the other's take 500 ms and its clock runs 30 s ahead. Ours and the improved one use the database clock: both workers are the same.",
            "| Repository | Entities sent by two workers at once | Sends | Results that found no matching row |", "|---|---:|---:|---:|");
        foreach (var side in sides)
        {
            await side.ResetAsync();
            await side.LoadAsync("D-", 2_000, synced: false, DateTimeOffset.UtcNow.AddDays(-1));
            await side.AnalyzeAsync();
            var settings = new Settings(BatchSize: 50, Lease: TimeSpan.FromSeconds(20));
            var normal = side.Create(settings);
            var ahead = side.Create(settings, () => DateTimeOffset.UtcNow.AddSeconds(30));
            var run = new Run();
            await RunWorkersAsync([(normal, TimeSpan.FromSeconds(2)), (ahead, TimeSpan.FromMilliseconds(500))], run, _ => true,
                async () => (await side.StateAsync("D-")).Synced == 2_000, TimeSpan.FromMinutes(3));
            Row(side.Name, run.Overlaps.ToString("N0"), run.Sends.ToString("N0"), (side.NoMatchingJob(normal) + side.NoMatchingJob(ahead)).ToString("N0"));
        }
    }

    /// <summary>
    /// A worker whose lease ran out reports a failure while a second worker is sending the same rows: does its late
    /// result free them for a third?
    /// </summary>
    private static async Task LateResultAsync(ISide[] sides)
    {
        Header("7. A late result after the lease ran out",
            "100 rows, lease 2 s (the improved one: 1 min). Worker A claims them; 0.2 s after its lease ran out, worker B claims them again and starts sending. 0.8 s later A reports a failure; 0.3 s after that worker C claims. Each worker is its own repository instance.",
            "| Repository | Rows B took over | Rows C claimed while B was still sending | A's late results refused |", "|---|---:|---:|---:|");
        foreach (var side in sides)
        {
            await side.ResetAsync();
            await side.LoadAsync("L-", 100, synced: false, DateTimeOffset.UtcNow.AddDays(-1));
            var settings = new Settings(BatchSize: 100, Lease: TimeSpan.FromSeconds(2));
            var (workerA, workerB, workerC) = (side.Create(settings), side.Create(settings), side.Create(settings));
            var a = await workerA.ClaimAsync(CancellationToken.None);
            await Task.Delay(side.LeaseOf(settings) + TimeSpan.FromMilliseconds(200));
            var b = await workerB.ClaimAsync(CancellationToken.None);
            await Task.Delay(800);
            await workerA.MarkFailedAsync(a, [.. a.EntityIds.Select(id => new FailedEvent(id, "timeout"))], CancellationToken.None);
            await Task.Delay(300);
            var c = await workerC.ClaimAsync(CancellationToken.None);
            Row(side.Name, b.Events.Count.ToString("N0"), c.EntityIds.Intersect(b.EntityIds).Count().ToString("N0"),
                side is OurSide ? "100 (skipped)" : side.NoMatchingJob(workerA).ToString("N0"));
        }
    }

    // ---------- Worker loop ----------

    /// <summary><paramref name="count"/> workers, each with its own repository instance (as each worker has its own scope).</summary>
    private static IRepository<AccountInboxRow>[] Workers(ISide side, Settings settings, int count)
    {
        return [.. Enumerable.Range(0, count).Select(_ => side.Create(settings))];
    }

    private sealed record Settings(int BatchSize, TimeSpan? Lease = null, int MaxAttempts = 8, TimeSpan? RetryBaseDelay = null);

    /// <summary>What the workers did: sends per entity, overlapping sends, when each event was synced.</summary>
    private sealed class Run
    {
        public readonly ConcurrentDictionary<string, int> InFlight = new(StringComparer.Ordinal);
        public readonly ConcurrentDictionary<string, int> SendCounts = new(StringComparer.Ordinal);
        public readonly ConcurrentDictionary<(string EntityId, string Payload), DateTimeOffset> Synced = new();
        public int Overlaps, Sends, Batches;

        public int SendsOf(string prefix)
        {
            return SendCounts.Where(s => s.Key.StartsWith(prefix, StringComparison.Ordinal)).Sum(s => s.Value);
        }
    }

    /// <summary>Runs one loop per repository until <paramref name="finished"/> or <paramref name="cap"/>.</summary>
    private static Task RunWorkersAsync(IRepository<AccountInboxRow>[] workers, Run run, TimeSpan latency, Func<InboxEvent, bool> succeeds,
        Func<Task<bool>> finished, TimeSpan cap)
    {
        return RunWorkersAsync([.. workers.Select(w => (w, latency))], run, succeeds, finished, cap);
    }

    /// <summary>As above, each worker with its own Salesforce call time.</summary>
    private static async Task RunWorkersAsync((IRepository<AccountInboxRow> Repo, TimeSpan Latency)[] workers, Run run, Func<InboxEvent, bool> succeeds,
        Func<Task<bool>> finished, TimeSpan cap)
    {
        using var stop = new CancellationTokenSource(cap);
        var loops = workers.Select(w => Task.Run(() => WorkerAsync(w.Repo, run, w.Latency, succeeds, stop.Token))).ToList();
        while (!stop.IsCancellationRequested && !await finished())
        {
            await Delay(TimeSpan.FromMilliseconds(50), stop.Token);
        }

        await stop.CancelAsync();
        await Task.WhenAll(loops);
    }

    /// <summary>Claim, "send" (a delay, then an outcome per entity), record: the shape of SalesforceBatchSyncService.</summary>
    private static async Task WorkerAsync(IRepository<AccountInboxRow> repo, Run run, TimeSpan latency, Func<InboxEvent, bool> succeeds, CancellationToken stop)
    {
        while (!stop.IsCancellationRequested)
        {
            InboxClaim claim;
            try
            {
                claim = await repo.ClaimAsync(stop);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            if (claim.Events.Count == 0)
            {
                await Delay(Idle, stop);
                continue;
            }

            Interlocked.Increment(ref run.Batches);
            foreach (var id in claim.EntityIds)
            {
                if (run.InFlight.AddOrUpdate(id, 1, (_, n) => n + 1) > 1)
                {
                    Interlocked.Increment(ref run.Overlaps);
                }

                run.SendCounts.AddOrUpdate(id, 1, (_, n) => n + 1);
            }

            await Delay(latency, CancellationToken.None);
            Interlocked.Add(ref run.Sends, claim.Events.Count);
            var ok = claim.Events.Where(succeeds).ToList();
            foreach (var e in claim.Events)
            {
                run.InFlight.AddOrUpdate(e.EntityId, 0, (_, n) => n - 1);
            }

            foreach (var e in ok)
            {
                run.Synced.TryAdd((e.EntityId, e.Payload), DateTimeOffset.UtcNow);
            }

            await repo.MarkDoneAsync(claim, [.. ok.Select(e => e.EntityId)], CancellationToken.None);
            await repo.MarkFailedAsync(claim, [.. claim.Events.Except(ok).Select(e => new FailedEvent(e.EntityId, "TEST_FAILURE: send failed"))],
                CancellationToken.None);
        }
    }

    private static async Task Delay(TimeSpan delay, CancellationToken ct)
    {
        try
        {
            await Task.Delay(delay, ct);
        }
        catch (OperationCanceledException)
        {
        }
    }

    // ---------- The two tables ----------

    /// <param name="Synced">In Salesforce with its newest event.</param>
    /// <param name="GivenUp">Not synced, and no claim will return it again.</param>
    /// <param name="Waiting">Not synced, and a claim will return it (now or later).</param>
    private sealed record State(int Synced, int GivenUp, int Waiting);

    private interface ISide
    {
        string Name { get; }

        IRepository<AccountInboxRow> Create(Settings settings, Func<DateTimeOffset>? clock = null);

        Task ResetAsync();

        /// <summary><paramref name="count"/> rows prefix1.., one event each, 1 ms apart from <paramref name="first"/>.</summary>
        Task LoadAsync(string prefix, int count, bool synced, DateTimeOffset first);

        Task AnalyzeAsync();

        Task<State> StateAsync(string prefix);

        /// <summary>Rows of <paramref name="prefix"/> whose last error is stored on them.</summary>
        Task<int> WithErrorAsync(string prefix);

        int NoMatchingJob(IRepository<AccountInboxRow> repo);

        /// <summary>The lease a claim gets with these settings.</summary>
        TimeSpan LeaseOf(Settings settings)
        {
            return settings.Lease ?? TimeSpan.FromMinutes(5);
        }
    }

    private enum SimpleKind
    {
        PrimaryKeyOnly,
        PartialIndex,
        Improved
    }

    private sealed class OurSide(IDbContextFactory<SyncDbContext> contexts) : ISide
    {
        private static readonly string Table = TableOf();

        public string Name => "Ours (InboxRepository)";

        public IRepository<AccountInboxRow> Create(Settings s, Func<DateTimeOffset>? clock = null)
        {
            return new InboxRepository<AccountInboxRow>(contexts, Options.Create(new SyncOptions
            {
                BatchSize = s.BatchSize,
                Lease = s.Lease ?? TimeSpan.FromMinutes(5),
                MaxRetries = s.MaxAttempts,
                RetryBaseDelay = s.RetryBaseDelay ?? TimeSpan.FromSeconds(30)
            }));
        }

        public Task ResetAsync()
        {
            return ExecuteAsync(contexts, $"TRUNCATE {Table}");
        }

        public Task LoadAsync(string prefix, int count, bool synced, DateTimeOffset first)
        {
            return ExecuteAsync(contexts, $"""
                INSERT INTO {Table} (entity_id, payload, event_time, attempt_count, last_updated, next_attempt_at)
                SELECT @prefix || g, @payload::jsonb, @first + g * interval '1 millisecond', 0, now(),
                       CASE WHEN @synced THEN NULL ELSE @first + g * interval '1 millisecond' END
                FROM generate_series(1, @count) g
                """, new { prefix, payload = """{"v":1}""", first, synced, count });
        }

        public Task AnalyzeAsync()
        {
            return ExecuteAsync(contexts, $"VACUUM ANALYZE {Table}");
        }

        public Task<State> StateAsync(string prefix)
        {
            return contexts.RunAsync(async (db, t) => (await db.Database.QueryAsync<State>($"""
                SELECT count(*) FILTER (WHERE next_attempt_at IS NULL AND last_error IS NULL)::int AS Synced,
                       count(*) FILTER (WHERE next_attempt_at IS NULL AND last_error IS NOT NULL)::int AS GivenUp,
                       count(*) FILTER (WHERE next_attempt_at IS NOT NULL)::int AS Waiting
                FROM {Table} WHERE entity_id LIKE @prefix || '%'
                """, t, new { prefix })).Single(), CancellationToken.None);
        }

        public Task<int> WithErrorAsync(string prefix)
        {
            return contexts.RunAsync((db, t) => db.Database.QuerySingleAsync<int>(
                $"SELECT count(*)::int FROM {Table} WHERE entity_id LIKE @prefix || '%' AND last_error IS NOT NULL", t, new { prefix }), CancellationToken.None);
        }

        public int NoMatchingJob(IRepository<AccountInboxRow> repo)
        {
            return 0; // a result for a row no longer this claim's is skipped by design, not an error
        }

        private static string TableOf()
        {
            using var model = SyncDbContext.ForModel();
            return model.TableOf<AccountInboxRow>();
        }
    }

    private sealed class SimpleSide(IDbContextFactory<SyncDbContext> contexts, SimpleKind kind) : ISide
    {
        public string Name => kind switch
        {
            SimpleKind.PrimaryKeyOnly => "Simple (primary key only)",
            SimpleKind.PartialIndex => "Simple + partial index",
            _ => "Simple, improved"
        };

        public IRepository<AccountInboxRow> Create(Settings s, Func<DateTimeOffset>? clock = null)
        {
            var lease = s.Lease ?? TimeSpan.FromMinutes(5);
            if (kind == SimpleKind.Improved)
            {
                // Database clock: an app clock has no say
                var repository = new ImprovedClaimRepository(contexts, SimpleTable, s.RetryBaseDelay ?? TimeSpan.FromSeconds(30), TimeSpan.FromHours(1));
                return new ImprovedClaimAdapter(repository, lease, s.BatchSize, s.MaxAttempts);
            }

            return new SimpleClaimRepository(contexts, SimpleTable, lease, s.BatchSize, s.MaxAttempts)
            {
                Clock = clock ?? (() => DateTimeOffset.UtcNow)
            };
        }

        public TimeSpan LeaseOf(Settings settings)
        {
            var lease = settings.Lease ?? TimeSpan.FromMinutes(5);
            return kind == SimpleKind.Improved ? TimeSpan.FromMinutes(ImprovedClaimAdapter.Minutes(lease)) : lease;
        }

        public async Task ResetAsync()
        {
            await ExecuteAsync(contexts, SimpleClaimRepository.CreateTableSql(SimpleTable));
            if (kind != SimpleKind.PrimaryKeyOnly)
            {
                await ExecuteAsync(contexts, SimpleClaimRepository.PartialIndexSql(SimpleTable));
            }
        }

        public Task LoadAsync(string prefix, int count, bool synced, DateTimeOffset first)
        {
            return ExecuteAsync(contexts, $"""
                INSERT INTO {SimpleTable} (entity_id, data, event_timestamp, last_synced_event_timestamp, process_count)
                SELECT @prefix || g, @payload::jsonb, @first + g * interval '1 millisecond',
                       CASE WHEN @synced THEN @first + g * interval '1 millisecond' ELSE '-infinity' END, 0
                FROM generate_series(1, @count) g
                """, new { prefix, payload = """{"v":1}""", first, synced, count });
        }

        public Task AnalyzeAsync()
        {
            return ExecuteAsync(contexts, $"VACUUM ANALYZE {SimpleTable}");
        }

        public Task<State> StateAsync(string prefix)
        {
            return contexts.RunAsync(async (db, t) => (await db.Database.QueryAsync<State>($"""
                SELECT count(*) FILTER (WHERE last_synced_event_timestamp >= event_timestamp)::int AS Synced,
                       count(*) FILTER (WHERE last_synced_event_timestamp < event_timestamp AND process_count >= @max)::int AS GivenUp,
                       count(*) FILTER (WHERE last_synced_event_timestamp < event_timestamp AND process_count < @max)::int AS Waiting
                FROM {SimpleTable} WHERE entity_id LIKE @prefix || '%'
                """, t, new { prefix, max = 8 })).Single(), CancellationToken.None);
        }

        public Task<int> WithErrorAsync(string prefix)
        {
            return contexts.RunAsync((db, t) => db.Database.QuerySingleAsync<int>(
                $"SELECT count(*)::int FROM {SimpleTable} WHERE entity_id LIKE @prefix || '%' AND last_error IS NOT NULL", t, new { prefix }), CancellationToken.None);
        }

        public int NoMatchingJob(IRepository<AccountInboxRow> repo)
        {
            return repo switch
            {
                SimpleClaimRepository simple => simple.NoMatchingJob,
                ImprovedClaimAdapter improved => improved.NoMatchingJob,
                _ => 0
            };
        }
    }

    // ---------- Helpers ----------

    private static Task ExecuteAsync(IDbContextFactory<SyncDbContext> contexts, string sql, object? args = null)
    {
        return contexts.RunAsync((db, t) => db.Database.ExecuteAsync(sql, t, args), CancellationToken.None);
    }

    /// <summary>The benchmark connection string pointed at <paramref name="name"/>, which is created if missing.</summary>
    private static async Task<string> CreateDatabaseAsync(string name)
    {
        var target = new NpgsqlConnectionStringBuilder(BenchmarkDb.ConnectionString) { Database = name };
        var admin = new NpgsqlConnectionStringBuilder(BenchmarkDb.ConnectionString) { Database = "postgres" };
        await using var connection = new NpgsqlConnection(admin.ConnectionString);
        await connection.OpenAsync();
        await using var exists = new NpgsqlCommand("SELECT 1 FROM pg_database WHERE datname = @name", connection);
        exists.Parameters.AddWithValue("name", name);
        if (await exists.ExecuteScalarAsync() is null)
        {
            await using var create = new NpgsqlCommand($"CREATE DATABASE {name}", connection);
            await create.ExecuteNonQueryAsync();
        }

        return target.ConnectionString;
    }

    private static void Header(string title, string setup, string columns, string rule)
    {
        Console.WriteLine();
        Console.WriteLine($"## {title}");
        Console.WriteLine();
        Console.WriteLine(setup);
        Console.WriteLine();
        Console.WriteLine(columns);
        Console.WriteLine(rule);
    }

    private static void Row(params string[] cells)
    {
        Console.WriteLine($"| {string.Join(" | ", cells)} |");
    }
}
