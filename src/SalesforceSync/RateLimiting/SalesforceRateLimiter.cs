using Dapper;
using Microsoft.Extensions.Options;
using SalesforceSync.Data;

namespace SalesforceSync.RateLimiting;

/// <summary>
/// Token bucket + limit guard stored in Postgres, shared by every worker in every process.
/// The bucket's speed is adjusted from the Sforce-Limit-Info header Salesforce returns on each call, and every
/// call is counted against this integration's share of the org's daily allocation over a rolling 24 hours.
/// </summary>
public sealed class SalesforceRateLimiter(
    PostgresExecutionStrategy pg,
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
    private int _allocationMismatchLogged;

    /// <summary>This integration's calls per rolling 24 hours.</summary>
    public long DailyBudget => (long)Math.Floor(_limits.EffectiveDailyApiAllocation * _opt.DailyBudgetShare);

    /// <summary>
    /// Writes the configured bucket size, rate and daily budget to the shared row (every process applies the same
    /// configuration at startup). The current throttle factor and stored tokens are kept.
    /// </summary>
    public async Task ApplyConfigurationAsync(CancellationToken ct)
    {
        await pg.ExecuteAsync(RateLimitSql.ApplyConfiguration, new
        {
            rate = _opt.MaxRequestsPerSecond,
            capacity = _opt.BurstCapacity,
            budget = DailyBudget
        }, ct);

        log.LogInformation(
            "Salesforce limits: {OrgType} {Edition} org, {Allocation} calls/24h for the org, budget {Budget} calls/24h " +
            "for this integration ({Share:P0}), {Rate} calls/s, burst {Burst}, max {Concurrent} concurrent long-running requests",
            _limits.OrgType, _limits.Edition, _limits.EffectiveDailyApiAllocation, DailyBudget, _opt.DailyBudgetShare,
            _opt.MaxRequestsPerSecond, _opt.BurstCapacity, _limits.EffectiveConcurrentLongRunningLimit);
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
            (bool granted, double waitSeconds, double used24h, double? budget) r = await pg.ExecuteInTransactionAsync(async (conn, tx, t) =>
            {
                await conn.ExecuteAsync(new CommandDefinition(RateLimitSql.Lock, transaction: tx, cancellationToken: t));
                return await conn.QuerySingleAsync<(bool, double, double, double?)>(
                    new CommandDefinition(RateLimitSql.Acquire, new { n = (double)permits }, tx, cancellationToken: t));
            }, ct);
            if (r.budget > 0) Volatile.Write(ref _ownDailyRatio, r.used24h / r.budget.Value);
            if (r.granted) return;

            var wait = TimeSpan.FromSeconds(Math.Max(r.waitSeconds, 0.05))
                       + TimeSpan.FromMilliseconds(Random.Shared.Next(0, 100)); // jitter
            if (DateTime.UtcNow + wait > deadline) throw new RateLimitedException(wait);
            await Task.Delay(wait, ct);
        }
    }

    public Task<bool> IsPausedAsync(CancellationToken ct) => pg.ExecuteScalarAsync<bool>(RateLimitSql.IsPaused, null, ct);

    public Task PauseAsync(TimeSpan duration, CancellationToken ct) => pg.ExecuteAsync(RateLimitSql.Pause, new { duration }, ct);

    /// <summary>
    /// Records the limits Salesforce reported and adjusts the shared bucket: slows down near the daily or per-app
    /// allocation, pauses near exhaustion. api-bursts is recorded but not acted on (see <see cref="SalesforceLimitInfo"/>).
    /// </summary>
    public async Task ReportLimitsAsync(SalesforceLimitInfo info, CancellationToken ct)
    {
        WarnOnAllocationMismatch(info);
        var decision = Decide(info, Volatile.Read(ref _ownDailyRatio));

        // Throttle writes: every response carries the header, but Postgres only needs it occasionally,
        // unless the throttle level just changed.
        lock (_reportLock)
        {
            var now = DateTime.UtcNow;
            if (decision == _lastDecision && now - _lastReportUtc < _opt.ReportInterval) return;
            if (decision != _lastDecision)
                LogTransition(_lastDecision, decision, info);
            _lastDecision = decision;
            _lastReportUtc = now;
        }

        await pg.ExecuteAsync(RateLimitSql.ReportLimits, new
        {
            apiUsed = info.ApiUsage?.Used,
            apiMax = info.ApiUsage?.Max,
            burstUsed = info.ApiBursts?.Used,
            burstMax = info.ApiBursts?.Max,
            appUsed = info.PerAppUsage?.Used,
            appMax = info.PerAppUsage?.Max,
            appName = info.AppName,
            speed = decision.Speed,
            pauseSeconds = decision.Pause.TotalSeconds
        }, ct);
    }

    /// <param name="ownDailyRatio">This integration's rolling 24h calls / its daily budget.</param>
    internal ThrottleDecision Decide(SalesforceLimitInfo info, double ownDailyRatio = 0)
    {
        var daily = Math.Max(info.DailyRatio, ownDailyRatio);
        if (daily >= _opt.DailyHardRatio)
        {
            return new ThrottleDecision(ThrottleLevel.DailyExhausted, _opt.DailySoftSpeed, _opt.DailyHardPause);
        }
        if (daily >= _opt.DailySoftRatio)
        {
            return new ThrottleDecision(ThrottleLevel.DailyLow, _opt.DailySoftSpeed, TimeSpan.Zero);
        }
        return ThrottleDecision.Normal;
    }

    /// <summary>The configured allocation drives the budget; say so once if Salesforce reports a different one.</summary>
    private void WarnOnAllocationMismatch(SalesforceLimitInfo info)
    {
        if (info.ApiUsage is not { Max: > 0 } usage || usage.Max == _limits.EffectiveDailyApiAllocation) return;
        if (Interlocked.Exchange(ref _allocationMismatchLogged, 1) == 1) return;
        log.LogWarning(
            "Salesforce reports a daily API allocation of {Reported}, but {Section} describes {Configured}. " +
            "The budget is based on the configured value; update {Section} (or set DailyApiAllocation).",
            usage.Max, SalesforceLimitsOptions.Section, _limits.EffectiveDailyApiAllocation, SalesforceLimitsOptions.Section);
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
