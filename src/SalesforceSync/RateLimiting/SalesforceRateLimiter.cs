using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using SalesforceSync.Data;

namespace SalesforceSync.RateLimiting;

/// <summary>
/// Token bucket + limit guard stored in Postgres, shared by every worker in every process.
/// The bucket's speed is adjusted from the Sforce-Limit-Info header Salesforce returns on each call, and every
/// call is counted against this integration's share of the org's daily allocation over a rolling
/// <see cref="RateLimitOptions.BudgetWindow"/> (24 hours by default). The
/// allocation is the lower of the configured one (<see cref="SalesforceLimitsOptions.DailyApiAllocation"/>, the
/// maximum) and the limit Salesforce reports in that header (<c>api-usage=25/5000</c>: 5,000).
/// <para>
/// Every change locks the shared <c>sf_rate_limit</c> row and reads the database clock in one statement
/// (<see cref="RateLimitSql.Lock"/>), decides in C# (<see cref="TokenBucket"/>) and writes the row back in the same
/// transaction. Every acquire in every process queues for that lock, so it is held for as few round trips as possible.
/// Each unit is retried as a whole on transient errors; a retry after an unknown commit can only over-count usage, the
/// safe direction.
/// </para>
/// </summary>
public sealed class SalesforceRateLimiter(
    IDbContextFactory<SyncDbContext> contexts,
    IOptions<RateLimitOptions> options,
    IOptions<SalesforceLimitsOptions> limits,
    ILogger<SalesforceRateLimiter> log)
{
    /// <summary>The least wait between attempts to acquire.</summary>
    private static readonly TimeSpan MinWait = TimeSpan.FromMilliseconds(50);

    private readonly RateLimitOptions _opt = options.Value;
    private readonly SalesforceLimitsOptions _limits = limits.Value;
    private readonly object _reportLock = new();
    private DateTime _lastReportUtc = DateTime.MinValue;
    private ThrottleDecision _lastDecision = ThrottleDecision.Normal;
    private double _ownDailyRatio;
    private long _loggedAllocation;

    /// <summary>This integration's calls per budget window under the configured allocation alone: the maximum.</summary>
    public long DailyBudget => BudgetFor(null);

    /// <summary>
    /// This integration's calls per budget window: <see cref="RateLimitOptions.DailyBudgetShare"/> of the lower of
    /// the configured allocation and <paramref name="reportedAllocation"/> (Sforce-Limit-Info api-usage's limit).
    /// </summary>
    public long BudgetFor(long? reportedAllocation)
    {
        var allocation = _limits.DailyApiAllocation;
        if (reportedAllocation is > 0 and var reported && reported < allocation)
        {
            allocation = reported;
        }
        return (long)Math.Floor(allocation * _opt.DailyBudgetShare);
    }

    /// <summary>
    /// Writes the configured bucket size, rate and daily budget to the shared row (every process applies the same
    /// configuration at startup), the budget capped by the allocation Salesforce last reported. The current throttle
    /// factor and stored tokens are kept.
    /// </summary>
    public async Task ApplyConfigurationAsync(CancellationToken ct)
    {
        await UpdateAsync((row, _) =>
        {
            // Keeps the current throttle factor (refill_per_sec / base_refill_per_sec) under the new base rate. The
            // budget is the configured one, lowered to the share of the allocation Salesforce last reported.
            var factor = row.BaseRefillPerSec > 0 ? row.RefillPerSec / row.BaseRefillPerSec : 1;
            row.Capacity = _opt.BurstCapacity;
            row.Tokens = Math.Min(row.Tokens, row.Capacity);
            row.RefillPerSec = _opt.MaxRequestsPerSecond * factor;
            row.BaseRefillPerSec = _opt.MaxRequestsPerSecond;
            row.DailyBudget = BudgetFor(row.ApiMax);
        }, ct);

        log.LogInformation(
            "Salesforce limits: at most {Allocation} calls/24h for the org (lower if Salesforce reports less), budget {Budget} calls " +
            "per {Window} for this integration ({Share:P0}), {Rate} calls/s, burst {Burst}, max {Concurrent} concurrent long-running requests",
            _limits.DailyApiAllocation, DailyBudget, _opt.BudgetWindow, _opt.DailyBudgetShare,
            _opt.MaxRequestsPerSecond, _opt.BurstCapacity, _limits.ConcurrentLongRunningLimit);
    }

    /// <summary>
    /// Takes a token for one call, waiting up to <see cref="RateLimitOptions.MaxWait"/> for it, else throws
    /// <see cref="RateLimitedException"/>.
    /// </summary>
    public async Task AcquireAsync(CancellationToken ct)
    {
        const int oneCall = 1;
        var deadline = DateTime.UtcNow + _opt.MaxWait;
        while (true)
        {
            var (decision, used, budget) = await contexts.RunInTransactionAsync(async (db, t) =>
            {
                // Every acquire in every process waits for this row lock, so it is held for four round trips only: the
                // lock (with the clock), the usage, the write when granted, and the commit
                var (row, now) = await LockAsync(db, t);
                // Read after the lock, in a statement of its own: it sees every usage increment committed by
                // whoever held the lock before. With the calls already counted in the current bucket, null if none.
                var since = now - _opt.CountedWindow;
                var bucket = TokenBucket.BucketOf(now, _opt.UsageBucket);
                var usage = await db.Set<ApiUsageBucket>().Where(b => b.Bucket >= since)
                    .GroupBy(_ => 1)
                    .Select(g => new
                    {
                        Used = g.Sum(b => b.Calls),
                        Oldest = g.Min(b => b.Bucket),
                        Current = g.Max(b => b.Bucket == bucket ? (long?)b.Calls : null),
                    })
                    .SingleOrDefaultAsync(t);
                var used = usage?.Used ?? 0;
                var decision = TokenBucket.Decide(row, now, used, usage?.Oldest, oneCall, _opt);
                if (decision.Granted)
                {
                    row.Tokens = decision.Tokens;
                    row.UpdatedAt = now;
                    used += oneCall;
                    // The row lock serializes every acquire in every process, so no one can insert this bucket
                    // between the read and the write: no ON CONFLICT needed. One SaveChanges, one batch.
                    var counted = new ApiUsageBucket { Bucket = bucket, Calls = (usage?.Current ?? 0) + oneCall };
                    if (usage?.Current is null)
                    {
                        db.Add(counted);
                    }
                    else
                    {
                        db.Update(counted);
                    }
                    await db.SaveChangesAsync(t);
                }
                return (decision, used, row.DailyBudget);
            }, ct);
            if (budget > 0)
            {
                Volatile.Write(ref _ownDailyRatio, (double)used / budget.Value);
            }

            if (decision.Granted)
            {
                return;
            }

            var wait = (decision.Wait > MinWait ? decision.Wait : MinWait)
                       + TimeSpan.FromMilliseconds(Random.Shared.Next(0, 100)); // jitter
            if (DateTime.UtcNow + wait > deadline)
            {
                throw new RateLimitedException(wait);
            }

            await Task.Delay(wait, ct);
        }
    }

    public Task<bool> IsPausedAsync(CancellationToken ct)
    {
        return contexts.RunAsync((db, t) => db.Database.QuerySingleAsync<bool>(RateLimitSql.IsPaused, t), ct);
    }

    public Task PauseAsync(TimeSpan duration, CancellationToken ct)
    {
        return UpdateAsync((row, now) => Pause(row, now, duration), ct);
    }

    /// <summary>
    /// Records the limits Salesforce reported and adjusts the shared bucket: slows down near the daily or per-app
    /// allocation, pauses near exhaustion. api-bursts is recorded but not acted on (see <see cref="SalesforceLimitInfo"/>).
    /// </summary>
    public async Task ReportLimitsAsync(SalesforceLimitInfo info, CancellationToken ct)
    {
        LogAllocation(info);
        var decision = Decide(info, Volatile.Read(ref _ownDailyRatio));

        // Throttle writes: every response carries the header, but Postgres only needs it occasionally,
        // unless the throttle level just changed.
        lock (_reportLock)
        {
            var now = DateTime.UtcNow;
            if (decision == _lastDecision && now - _lastReportUtc < _opt.ReportInterval)
            {
                return;
            }

            if (decision != _lastDecision)
            {
                LogTransition(_lastDecision, decision, info);
            }

            _lastDecision = decision;
            _lastReportUtc = now;
        }

        await UpdateAsync((row, now) =>
        {
            row.RefillPerSec = Math.Max(row.BaseRefillPerSec * decision.Speed, 0.01);
            if (decision.Pause > TimeSpan.Zero)
            {
                Pause(row, now, decision.Pause);
            }
            row.ApiUsed = info.ApiUsage?.Used ?? row.ApiUsed;
            row.ApiMax = info.ApiUsage?.Max ?? row.ApiMax;
            row.BurstUsed = info.ApiBursts?.Used ?? row.BurstUsed;
            row.BurstMax = info.ApiBursts?.Max ?? row.BurstMax;
            row.AppUsed = info.PerAppUsage?.Used ?? row.AppUsed;
            row.AppMax = info.PerAppUsage?.Max ?? row.AppMax;
            row.AppName = info.AppName ?? row.AppName;
            // The budget under the allocation this response reported, if it reported one
            row.DailyBudget = info.ApiUsage is { Max: > 0 } usage ? BudgetFor(usage.Max) : row.DailyBudget;
            row.UsageUpdatedAt = now;
        }, ct);
    }

    /// <summary>
    /// Locks the row, refills its tokens at the current rate (so a change of rate or capacity applies from now on),
    /// applies <paramref name="change"/>, and commits; first deletes the usage buckets that have left the window.
    /// </summary>
    private Task UpdateAsync(Action<RateLimitState, DateTimeOffset> change, CancellationToken ct)
    {
        return contexts.RunInTransactionAsync(async (db, t) =>
        {
            // Before the lock, so as not to hold it longer: acquires only read and write buckets in the window
            await db.Set<ApiUsageBucket>()
                .Where(b => b.Bucket + _opt.CountedWindow < RateLimitSql.ClockTimestamp())
                .ExecuteDeleteAsync(t);
            var (row, now) = await LockAsync(db, t);
            row.Tokens = TokenBucket.Refill(row, now);
            row.UpdatedAt = now;
            change(row, now);
            await db.SaveChangesAsync(t);
        }, ct);
    }

    /// <summary>The shared row, locked until the transaction ends, and the database clock once it is locked: one statement.</summary>
    private static async Task<(RateLimitState Row, DateTimeOffset Now)> LockAsync(SyncDbContext db, CancellationToken t)
    {
        var locked = await db.Set<RateLimitState>().FromSqlRaw(RateLimitSql.Lock)
            .Select(r => new { Row = r, Now = RateLimitSql.ClockTimestamp() })
            .SingleAsync(t);
        return (locked.Row, locked.Now);
    }

    /// <summary>Pauses until <paramref name="duration"/> from now, unless already paused for longer.</summary>
    private static void Pause(RateLimitState row, DateTimeOffset now, TimeSpan duration)
    {
        var until = now + duration;
        if (row.PausedUntil is null || row.PausedUntil < until)
        {
            row.PausedUntil = until;
        }
    }

    /// <param name="ownDailyRatio">This integration's calls in the budget window / its budget.</param>
    /// <param name="info">What Salesforce last reported in Sforce-Limit-Info.</param>
    internal ThrottleDecision Decide(SalesforceLimitInfo info, double ownDailyRatio = 0)
    {
        var daily = Math.Max(info.DailyRatio, ownDailyRatio);
        return daily >= _opt.DailyHardRatio
            ? new ThrottleDecision(ThrottleLevel.DailyExhausted, _opt.DailySoftSpeed, _opt.DailyHardPause)
            : daily >= _opt.DailySoftRatio
            ? new ThrottleDecision(ThrottleLevel.DailyLow, _opt.DailySoftSpeed, TimeSpan.Zero)
            : ThrottleDecision.Normal;
    }

    /// <summary>Logs which allocation the budget follows, once per process and again whenever Salesforce's changes.</summary>
    private void LogAllocation(SalesforceLimitInfo info)
    {
        if (info.ApiUsage is not { Max: > 0 } usage)
        {
            return;
        }
        if (Interlocked.Exchange(ref _loggedAllocation, usage.Max) == usage.Max)
        {
            return;
        }
        var configured = _limits.DailyApiAllocation;
        log.LogInformation(
            "Salesforce reports a daily API allocation of {Reported} calls, {Section} allows at most {Configured}: " +
            "the budget is {Budget} calls per {Window} ({Share:P0} of {Allocation})",
            usage.Max, SalesforceLimitsOptions.Section, configured, BudgetFor(usage.Max), _opt.BudgetWindow, _opt.DailyBudgetShare,
            Math.Min(usage.Max, configured));
    }

    private void LogTransition(ThrottleDecision from, ThrottleDecision to, SalesforceLimitInfo info)
    {
        var level = to.Level == ThrottleLevel.Normal ? LogLevel.Information : LogLevel.Warning;
        log.Log(level,
            "Salesforce throttle {From} -> {To} (speed x{Speed}, pause {Pause}). api-usage={Api} api-bursts={Bursts} per-app={App} ({AppName})",
            from.Level, to.Level, to.Speed, to.Pause,
            info.ApiUsage, info.ApiBursts, info.PerAppUsage, info.AppName);
    }

    internal enum ThrottleLevel { Normal, DailyLow, DailyExhausted }

    internal readonly record struct ThrottleDecision(ThrottleLevel Level, double Speed, TimeSpan Pause)
    {
        public static ThrottleDecision Normal => new(ThrottleLevel.Normal, 1.0, TimeSpan.Zero);
    }
}