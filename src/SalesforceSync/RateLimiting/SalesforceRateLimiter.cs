using Dapper;
using Microsoft.Extensions.Options;
using Npgsql;

namespace SalesforceSync.RateLimiting;

/// <summary>
/// Token bucket + limit guard stored in Postgres, shared by every worker in every process.
/// The bucket's speed is adjusted from the Sforce-Limit-Info header Salesforce returns on each call, and every
/// call is counted against this integration's share of the org's daily allocation over a rolling 24 hours.
/// </summary>
public sealed class SalesforceRateLimiter(
    NpgsqlDataSource db,
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
        await using var conn = await db.OpenConnectionAsync(ct);
        await conn.ExecuteAsync(new CommandDefinition(Sql.ApplyConfiguration, new
        {
            rate = _opt.MaxRequestsPerSecond,
            capacity = _opt.BurstCapacity,
            budget = DailyBudget
        }, cancellationToken: ct));

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
            (bool granted, double waitSeconds, double used24h, double? budget) r;
            await using (var conn = await db.OpenConnectionAsync(ct))
            await using (var tx = await conn.BeginTransactionAsync(ct))
            {
                // Lock first, in its own statement: the acquire statement's snapshot then includes every
                // usage increment committed by whoever held the lock before (a single statement would read
                // sf_api_usage from a snapshot taken before it waited for the lock).
                await conn.ExecuteAsync(new CommandDefinition(Sql.Lock, transaction: tx, cancellationToken: ct));
                r = await conn.QuerySingleAsync<(bool, double, double, double?)>(
                    new CommandDefinition(Sql.Acquire, new { n = (double)permits }, tx, cancellationToken: ct));
                await tx.CommitAsync(ct);
            }
            if (r.budget > 0) Volatile.Write(ref _ownDailyRatio, r.used24h / r.budget.Value);
            if (r.granted) return;

            var wait = TimeSpan.FromSeconds(Math.Max(r.waitSeconds, 0.05))
                       + TimeSpan.FromMilliseconds(Random.Shared.Next(0, 100)); // jitter
            if (DateTime.UtcNow + wait > deadline) throw new RateLimitedException(wait);
            await Task.Delay(wait, ct);
        }
    }

    public async Task<bool> IsPausedAsync(CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        return await conn.ExecuteScalarAsync<bool>(new CommandDefinition(Sql.IsPaused, cancellationToken: ct));
    }

    public async Task PauseAsync(TimeSpan duration, CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        await conn.ExecuteAsync(new CommandDefinition(Sql.Pause, new { duration }, cancellationToken: ct));
    }

    /// <summary>
    /// Records the limits Salesforce reported and adjusts the shared bucket:
    /// slows down near the daily / per-app allocation or burst limit, pauses near exhaustion.
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

        await using var conn = await db.OpenConnectionAsync(ct);
        await conn.ExecuteAsync(new CommandDefinition(Sql.ReportLimits, new
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
        }, cancellationToken: ct));
    }

    /// <param name="ownDailyRatio">This integration's rolling 24h calls / its daily budget.</param>
    internal ThrottleDecision Decide(SalesforceLimitInfo info, double ownDailyRatio = 0)
    {
        var daily = Math.Max(info.DailyRatio, ownDailyRatio);
        var burst = info.BurstRatio;

        if (daily >= _opt.DailyHardRatio)
            return new ThrottleDecision(ThrottleLevel.DailyExhausted, _opt.DailySoftSpeed, _opt.DailyHardPause);

        var speed = 1.0;
        var pause = TimeSpan.Zero;
        var level = ThrottleLevel.Normal;

        if (daily >= _opt.DailySoftRatio)
        {
            speed = Math.Min(speed, _opt.DailySoftSpeed);
            level = ThrottleLevel.DailyLow;
        }

        if (burst >= _opt.BurstSoftRatio)
        {
            speed = Math.Min(speed, _opt.BurstSoftSpeed);
            pause = _opt.BurstPause;
            level = level == ThrottleLevel.Normal ? ThrottleLevel.Bursting : level;
        }

        return new ThrottleDecision(level, speed, pause);
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

    internal enum ThrottleLevel { Normal, Bursting, DailyLow, DailyExhausted }

    internal readonly record struct ThrottleDecision(ThrottleLevel Level, double Speed, TimeSpan Pause)
    {
        public static ThrottleDecision Normal => new(ThrottleLevel.Normal, 1.0, TimeSpan.Zero);
    }

    private static class Sql
    {
        public const string Lock = "select 1 from sf_rate_limit where name = 'salesforce' for update";

        // Runs after Lock in the same transaction: the row lock serializes every acquire in every process, so the
        // budget check and the usage increment are exact. The window includes the whole 10-minute bucket that straddles "24 hours ago",
        // so usage is over- rather than under-counted.
        public const string Acquire = """
            with s as (
              select name, refill_per_sec, paused_until, daily_budget,
                     least(capacity, tokens + extract(epoch from clock_timestamp() - updated_at)::float8 * refill_per_sec) as available
              from sf_rate_limit
              where name = 'salesforce'
              for update
            ), w as (
              select coalesce(sum(calls), 0)::float8 as used, min(bucket) as oldest
              from sf_api_usage
              where bucket >= clock_timestamp() - interval '24 hours 10 minutes'
            ), u as (
              update sf_rate_limit r
              set tokens = s.available - @n, updated_at = clock_timestamp()
              from s, w
              where r.name = s.name
                and (s.paused_until is null or s.paused_until <= clock_timestamp())
                and s.available >= @n
                and (s.daily_budget is null or w.used + @n <= s.daily_budget)
              returning 1
            ), counted as (
              insert into sf_api_usage (bucket, calls)
              select to_timestamp(floor(extract(epoch from clock_timestamp()) / 600) * 600), @n
              where exists (select 1 from u)
              on conflict (bucket) do update set calls = sf_api_usage.calls + excluded.calls
            )
            select exists (select 1 from u) as granted,
                   (case when s.paused_until > clock_timestamp()
                         then extract(epoch from s.paused_until - clock_timestamp())
                         when s.daily_budget is not null and w.used + @n > s.daily_budget
                         then greatest(60, extract(epoch from w.oldest + interval '24 hours 10 minutes' - clock_timestamp()))
                         else greatest(0, (@n - s.available) / s.refill_per_sec) end)::float8 as wait_seconds,
                   w.used + (case when exists (select 1 from u) then @n else 0 end) as used_24h,
                   s.daily_budget::float8 as daily_budget
            from s, w
            """;

        // Keeps the current throttle factor (refill_per_sec / base_refill_per_sec) under the new base rate.
        public const string ApplyConfiguration = """
            update sf_rate_limit
            set tokens              = least(@capacity, tokens + extract(epoch from clock_timestamp() - updated_at)::float8 * refill_per_sec),
                updated_at          = clock_timestamp(),
                capacity            = @capacity,
                refill_per_sec      = @rate * coalesce(refill_per_sec / nullif(base_refill_per_sec, 0), 1),
                base_refill_per_sec = @rate,
                daily_budget        = @budget
            where name = 'salesforce';
            delete from sf_api_usage where bucket < clock_timestamp() - interval '25 hours';
            """;

        public const string IsPaused = """
            select coalesce(paused_until > clock_timestamp(), false)
            from sf_rate_limit where name = 'salesforce'
            """;

        public const string Pause = """
            update sf_rate_limit
            set paused_until = greatest(coalesce(paused_until, clock_timestamp()), clock_timestamp() + @duration)
            where name = 'salesforce'
            """;

        // Refill tokens accrued at the old rate before changing the rate, so the switch is exact.
        public const string ReportLimits = """
            update sf_rate_limit
            set tokens          = least(capacity, tokens + extract(epoch from clock_timestamp() - updated_at)::float8 * refill_per_sec),
                updated_at      = clock_timestamp(),
                refill_per_sec  = greatest(base_refill_per_sec * @speed, 0.01),
                paused_until    = case when @pauseSeconds > 0
                                       then greatest(coalesce(paused_until, clock_timestamp()),
                                                     clock_timestamp() + make_interval(secs => @pauseSeconds))
                                       else paused_until end,
                api_used        = coalesce(@apiUsed, api_used),
                api_max         = coalesce(@apiMax, api_max),
                burst_used      = coalesce(@burstUsed, burst_used),
                burst_max       = coalesce(@burstMax, burst_max),
                app_used        = coalesce(@appUsed, app_used),
                app_max         = coalesce(@appMax, app_max),
                app_name        = coalesce(@appName, app_name),
                usage_updated_at = clock_timestamp()
            where name = 'salesforce';
            delete from sf_api_usage where bucket < clock_timestamp() - interval '25 hours';
            """;
    }
}
