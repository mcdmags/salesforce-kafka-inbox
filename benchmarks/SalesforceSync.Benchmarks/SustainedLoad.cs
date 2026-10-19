using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using SalesforceSync.Data;

namespace SalesforceSync.Benchmarks;

/// <summary>
/// What the drain benchmarks don't show: the pods under a steady stream with bursts, as in production. Account aggregates
/// keep arriving at a set rate (Kafka messages per second, in publish order, each split and stored as the consumer does)
/// over many accounts, with a burst now and then, while the pods run the worker's own pass at the worker's own defaults
/// (batches of up to 180 entities) against a Salesforce that takes real time per call. Once a second the queue is sampled:
/// how many entities wait and how long the oldest has waited. After the last message the pods drain what is left, and the
/// run is checked like every drain benchmark (every account, relationship and contact at its newest version, no
/// overlapping claims). Run: <c>dotnet run -c Release -- --load [--rate 300] [--seconds 120] [--accounts 20000] [--pods 3]
/// [--workers 4] [--latency 100] [--burst 4] [--burst-seconds 10] [--burst-every 40]</c>.
/// </summary>
public sealed class SustainedLoad : DrainBenchmarkBase
{
    private readonly Options _options;
    private readonly List<Sample> _samples = [];

    public SustainedLoad(Options options)
    {
        _options = options;
        Workers = options.Workers;
        Pods = options.Pods;
        Accounts = options.Accounts;
        LatencyMs = options.LatencyMs;
        Events = (int)(options.Rate * options.Seconds * (1 + ((options.Burst - 1) * options.BurstSeconds / options.BurstEvery)));
    }

    public override int Workers { get; set; }
    public override int Pods { get; set; }
    public override int Events { get; set; }
    public override int Accounts { get; set; }
    public override int LatencyMs { get; set; }
    protected override bool StreamIngest => true;
    protected override int? BatchSize => null;
    protected override TimeSpan DrainTimeout => TimeSpan.FromMinutes(30);

    /// <summary>The run's settings; the burst multiplies the rate for BurstSeconds at the start of every BurstEvery seconds.</summary>
    public sealed record Options(double Rate, int Seconds, int Accounts, int Pods, int Workers, int LatencyMs, double Burst,
        double BurstSeconds, double BurstEvery, bool Diagnostics = false)
    {
        public static Options Parse(string[] args)
        {
            double Get(string name, double fallback)
            {
                var at = Array.IndexOf(args, name);
                return at >= 0 && at + 1 < args.Length ? double.Parse(args[at + 1], System.Globalization.CultureInfo.InvariantCulture) : fallback;
            }

            return new Options(Get("--rate", 300), (int)Get("--seconds", 120), (int)Get("--accounts", 20_000), (int)Get("--pods", 3),
                (int)Get("--workers", 4), (int)Get("--latency", 100), Get("--burst", 4), Get("--burst-seconds", 10), Get("--burst-every", 40), args.Contains("--diagnostics"));
        }

        /// <summary>Messages per second at <paramref name="second"/> into the run.</summary>
        public double RateAt(double second)
        {
            return Burst > 1 && second % BurstEvery < BurstSeconds ? Rate * Burst : Rate;
        }
    }

    /// <summary>The due entities: how many, and how long the oldest has been due.</summary>
    private sealed record Queue(long Waiting, double OldestSeconds);

    private sealed record Sample(double Second, long Waiting, double OldestWaitSeconds, long Stored);

    public async Task RunAsync()
    {
        Console.WriteLine($"// load: {_options}");
        GlobalSetup();
        try
        {
            IterationSetup();
            Console.WriteLine($"// {Events} aggregates -> {EntityEvents.Count} entity events over {Accounts} accounts, " +
                              $"{Pods} pods x {Workers} workers, {LatencyMs} ms per Salesforce call");
            var clock = Stopwatch.StartNew();
            using var stop = new CancellationTokenSource();
            var sampler = Task.Run(() => SampleAsync(clock, stop.Token));
            await DrainAsync();
            var total = clock.Elapsed.TotalSeconds;
            await stop.CancelAsync();
            await sampler;

            Report(total);
            IterationCleanup(); // the correctness checks and the claim summary line
        }
        finally
        {
            GlobalCleanup();
        }
    }

    /// <summary>Stores the entity events at the run's rate: an event's turn comes as the aggregates before it have.</summary>
    protected override async Task IngestAsync(CancellationToken ct)
    {
        var clock = Stopwatch.StartNew();
        var due = 0.0; // seconds into the run when the next aggregate is due
        foreach (var (change, events) in Aggregates)
        {
            var wait = due - clock.Elapsed.TotalSeconds;
            if (wait > 0.002)
            {
                await Task.Delay(TimeSpan.FromSeconds(wait), ct);
            }

            await IngestAsync(change, ct);
            Interlocked.Add(ref _stored, events);
            due += 1 / _options.RateAt(due);
        }

        _lastStoredAt = clock.Elapsed.TotalSeconds;
    }

    private double _lastCpu, _lastGcPause;
    private long _stored;
    private double _lastStoredAt;

    /// <summary>Once a second: entities due and not yet done, and how long the one waiting longest has been due.</summary>
    private async Task SampleAsync(Stopwatch clock, CancellationToken ct)
    {
        var sql = string.Join(" UNION ALL ", InboxTables.Select(t => $"SELECT next_attempt_at FROM {t} WHERE next_attempt_at <= now()"));
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(1), ct);
                await using var db = await Contexts.CreateDbContextAsync(ct);
                var queue = await db.Database.QuerySingleAsync<Queue>(
                    $"SELECT count(*) AS Waiting, coalesce(extract(epoch FROM now() - min(next_attempt_at)), 0)::float8 AS OldestSeconds FROM ({sql}) due", ct);
                var sample = new Sample(clock.Elapsed.TotalSeconds, queue.Waiting, queue.OldestSeconds, Interlocked.Read(ref _stored));
                _samples.Add(sample);

                // Where the time goes: the claim's own lookup, the dead rows it walks past, and this process
                var probe = Stopwatch.StartNew();
                await db.Database.QuerySingleAsync<Queue>(
                    $"SELECT count(*) AS Waiting, 0::float8 AS OldestSeconds FROM (SELECT entity_id FROM {InboxTables[0]} WHERE next_attempt_at <= now() ORDER BY next_attempt_at LIMIT 180) x", ct);
                var probeMs = probe.Elapsed.TotalMilliseconds;
                var dead = await db.Database.QuerySingleAsync<Queue>(
                    $"SELECT coalesce(sum(n_dead_tup), 0)::bigint AS Waiting, 0::float8 AS OldestSeconds FROM pg_stat_user_tables WHERE relname LIKE 'sf\\_%inbox'", ct);
                var cpu = Process.GetCurrentProcess().TotalProcessorTime.TotalSeconds;
                var gcPause = GC.GetTotalPauseDuration().TotalSeconds;
                if (_samples.Count % 5 == 0 || _options.Diagnostics)
                {
                    Console.WriteLine($"// t={sample.Second,5:F0}s stored={sample.Stored,7} waiting={sample.Waiting,6} oldest wait={sample.OldestWaitSeconds,6:F1}s " +
                                      $"api={Org.GetStats().ApiCalls} | claim probe {probeMs:F1} ms, dead rows {dead.Waiting}, cpu {cpu - _lastCpu:F1}s/s, " +
                                      $"gc pause {gcPause - _lastGcPause:F2}s/s (gen2 {GC.CollectionCount(2)}), pool threads {ThreadPool.ThreadCount}, " +
                                      $"queued {ThreadPool.PendingWorkItemCount}, heap {GC.GetTotalMemory(false) / 1_000_000} MB");
                }

                (_lastCpu, _lastGcPause) = (cpu, gcPause);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    private void Report(double total)
    {
        var waits = _samples.Select(s => s.OldestWaitSeconds).OrderBy(x => x).ToList();
        double Percentile(double p)
        {
            return waits.Count == 0 ? 0 : waits[Math.Min(waits.Count - 1, (int)(p * waits.Count))];
        }

        var stats = Org.GetStats();
        var stored = Interlocked.Read(ref _stored);
        Console.WriteLine();
        Console.WriteLine($"// ===== sustained load: {_options.Rate:F0} msg/s (x{_options.Burst:F0} for {_options.BurstSeconds:F0} s every {_options.BurstEvery:F0} s), " +
                          $"{Accounts} accounts, {Pods} pods x {Workers} workers, {LatencyMs} ms per call =====");
        Console.WriteLine($"// ingest     : {stored} entity events in {_lastStoredAt:F1} s ({stored / Math.Max(1, _lastStoredAt):F0} events/s)");
        Console.WriteLine($"// finished   : {total:F1} s, so {Math.Max(0, total - _lastStoredAt):F1} s after the last event (the tail)");
        Console.WriteLine($"// queue      : max waiting {_samples.Max(s => s.Waiting)}, oldest wait p50 {Percentile(0.5):F1} s, p95 {Percentile(0.95):F1} s, max {waits.LastOrDefault():F1} s");
        Console.WriteLine($"// salesforce : {stats.ApiCalls} calls ({(double)stats.ApiCalls / Math.Max(1, stored):F2} per event), {stats.WriteRequests} write requests, " +
                          $"{stats.Queries} queries, {stats.RecordsSucceeded} records written, {stats.RecordsFailed} failed");
    }
}
