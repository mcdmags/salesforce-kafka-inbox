using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using SalesforceSync.Data;
using static SalesforceSync.Data.SqlCommands;

namespace SalesforceSync.RateLimiting;

/// <summary>
/// Token bucket + limit guard stored in Postgres, shared by every worker in every process.
/// The bucket's speed is adjusted from the Sforce-Limit-Info header Salesforce returns on each call, and every
/// call is counted against this integration's share of the org's daily allocation over a rolling 24 hours. The
/// allocation is the lower of the configured one (<see cref="SalesforceLimitsOptions.DailyApiAllocation"/>, the
/// maximum) and the limit Salesforce reports in that header (<c>api-usage=25/5000</c>: 5,000).
/// </summary>
public sealed class SalesforceRateLimiter(
    IDbContextFactory<SyncDbContext> contexts,
    IOptions<RateLimitOptions> options,
    IOptions<SalesforceLimitsOptions> limits,
    ILogger<SalesforceRateLimiter> log)
{
    private readonly RateLimitOptions _opt = options.Value;
    private readonly SalesforceLimitsOptions _limits = limits.Value;
    private readonly object _reportLock = new();
    private DateTime _lastReportUtc = DateTime.MinValue;
    private ThrottleDecision _lastDecision = ThrottleDecision.Normal;
    private double _ownDailyRatio;
    private long _loggedAllocation;

    /// <summary>This integration's calls per rolling 24 hours under the configured allocation alone: the maximum.</summary>
    public long DailyBudget => BudgetFor(null);

    /// <summary>
    /// This integration's calls per rolling 24 hours: <see cref="RateLimitOptions.DailyBudgetShare"/> of the lower of
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
        await contexts.RunAsync((db, t) => db.Database.ExecuteAsync(RateLimitSql.ApplyConfiguration, t,
            Param("rate", _opt.MaxRequestsPerSecond), Param("capacity", _opt.BurstCapacity),
            Param("budget", DailyBudget), Param("share", _opt.DailyBudgetShare)), ct);

        log.LogInformation(
            "Salesforce limits: at most {Allocation} calls/24h for the org (lower if Salesforce reports less), budget {Budget} calls/24h " +
            "for this integration ({Share:P0}), {Rate} calls/s, burst {Burst}, max {Concurrent} concurrent long-running requests",
            _limits.DailyApiAllocation, DailyBudget, _opt.DailyBudgetShare,
            _opt.MaxRequestsPerSecond, _opt.BurstCapacity, _limits.ConcurrentLongRunningLimit);
    }

    public async Task AcquireAsync(int permits, CancellationToken ct)
    {
        var deadline = DateTime.UtcNow + _opt.MaxWait;
        while (true)
        {
            // Lock first, in its own statement: the acquire statement's snapshot then includes every usage
            // increment committed by whoever held the lock before (a single statement would read sf_api_usage
            // from a snapshot taken before it waited for the lock). Retried as a whole on transient errors; a
            // retry after an unknown commit can only over-count usage, the safe direction.
            var r = await contexts.RunInTransactionAsync(async (db, t) =>
            {
                await db.Database.ExecuteAsync(RateLimitSql.Lock, t);
                return await db.Database.QuerySingleAsync<AcquireResult>(RateLimitSql.Acquire, t, Param("n", (double)permits));
            }, ct);
            if (r.DailyBudget > 0)
            {
                Volatile.Write(ref _ownDailyRatio, r.Used24h / r.DailyBudget.Value);
            }

            if (r.Granted)
            {
                return;
            }

            var wait = TimeSpan.FromSeconds(Math.Max(r.WaitSeconds, 0.05))
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
        return contexts.RunAsync((db, t) => db.Database.ExecuteAsync(RateLimitSql.Pause, t, Param("duration", duration)), ct);
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

        await contexts.RunAsync((db, t) => db.Database.ExecuteAsync(RateLimitSql.ReportLimits, t,
            Param("apiUsed", info.ApiUsage?.Used), Param("apiMax", info.ApiUsage?.Max),
            Param("burstUsed", info.ApiBursts?.Used), Param("burstMax", info.ApiBursts?.Max),
            Param("appUsed", info.PerAppUsage?.Used), Param("appMax", info.PerAppUsage?.Max),
            Param("appName", info.AppName),
            Param("budget", info.ApiUsage is { Max: > 0 } usage ? BudgetFor(usage.Max) : (long?)null),
            Param("speed", decision.Speed), Param("pauseSeconds", decision.Pause.TotalSeconds)), ct);
    }

    /// <param name="ownDailyRatio">This integration's rolling 24h calls / its daily budget.</param>
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
            "the budget is {Budget} calls/24h ({Share:P0} of {Allocation})",
            usage.Max, SalesforceLimitsOptions.Section, configured, BudgetFor(usage.Max), _opt.DailyBudgetShare,
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

    /// <summary>The row <see cref="RateLimitSql.Acquire"/> returns.</summary>
    private sealed class AcquireResult
    {
        [Column("granted")] public bool Granted { get; set; }
        [Column("wait_seconds")] public double WaitSeconds { get; set; }
        [Column("used_24h")] public double Used24h { get; set; }
        [Column("daily_budget")] public double? DailyBudget { get; set; }
    }

    internal enum ThrottleLevel { Normal, DailyLow, DailyExhausted }

    internal readonly record struct ThrottleDecision(ThrottleLevel Level, double Speed, TimeSpan Pause)
    {
        public static ThrottleDecision Normal => new(ThrottleLevel.Normal, 1.0, TimeSpan.Zero);
    }
}