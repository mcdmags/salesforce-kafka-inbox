using Dapper;
using Microsoft.Extensions.Options;
using Npgsql;

namespace SalesforceSync.RateLimiting;

/// <summary>
/// Token bucket + limit guard stored in Postgres, shared by every worker in every process.
/// The bucket's speed is adjusted from the Sforce-Limit-Info header Salesforce returns on each call.
/// </summary>
public sealed class SalesforceRateLimiter(
    NpgsqlDataSource db,
    IOptions<RateLimitOptions> options,
    ILogger<SalesforceRateLimiter> log)
{
    private readonly RateLimitOptions _opt = options.Value;
    private readonly object _reportLock = new();
    private DateTime _lastReportUtc = DateTime.MinValue;
    private ThrottleDecision _lastDecision = ThrottleDecision.Normal;

    public async Task AcquireAsync(int permits, CancellationToken ct)
    {
        var deadline = DateTime.UtcNow + _opt.MaxWait;
        while (true)
        {
            (bool granted, double waitSeconds) r;
            await using (var conn = await db.OpenConnectionAsync(ct))
            {
                r = await conn.QuerySingleAsync<(bool, double)>(
                    new CommandDefinition(Sql.Acquire, new { n = (double)permits }, cancellationToken: ct));
            }
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
        var decision = Decide(info);

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

    internal ThrottleDecision Decide(SalesforceLimitInfo info)
    {
        var daily = info.DailyRatio;
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
        public const string Acquire = """
            with s as (
              select name, refill_per_sec, paused_until,
                     least(capacity, tokens + extract(epoch from clock_timestamp() - updated_at)::float8 * refill_per_sec) as available
              from sf_rate_limit
              where name = 'salesforce'
              for update
            ), u as (
              update sf_rate_limit r
              set tokens = s.available - @n, updated_at = clock_timestamp()
              from s
              where r.name = s.name
                and (s.paused_until is null or s.paused_until <= clock_timestamp())
                and s.available >= @n
              returning 1
            )
            select exists (select 1 from u) as granted,
                   (case when s.paused_until > clock_timestamp()
                         then extract(epoch from s.paused_until - clock_timestamp())
                         else greatest(0, (@n - s.available) / s.refill_per_sec) end)::float8 as wait_seconds
            from s
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
            where name = 'salesforce'
            """;
    }
}
