using Microsoft.Extensions.Options;
using SalesforceSync.Data.RateLimiting;
using SalesforceSync.Infrastructure.RateLimiting;

namespace SalesforceSync.RateLimiting;

/// <summary>
/// Token bucket + limit guard stored in Postgres, shared by every worker in every process.
/// The bucket's speed is adjusted from the Sforce-Limit-Info header Salesforce returns on each call: it slows down at
/// <see cref="RateLimitOptions.DailySoftRatio"/> of the org's (or the connected app's) daily allocation and pauses at
/// <see cref="RateLimitOptions.DailyHardRatio"/>. Salesforce counts the allocation over a rolling 24 hours and treats it as
/// a soft limit, so there is no count of our own to keep: the header is the count.
/// <para>
/// Every change has the shared <c>sf_rate_limit</c> row locked, with the database clock, by the
/// <see cref="RateLimitRepository"/>, decides in C# (<see cref="TokenBucket"/>) and has the row written back in the same
/// transaction. Every acquire in every process queues for that lock, so it is held for as few round trips as possible.
/// </para>
/// </summary>
public sealed class SalesforceRateLimiter(
    RateLimitRepository repository,
    IOptions<RateLimitOptions> options,
    IOptions<SalesforceLimitsOptions> limits,
    ILogger<SalesforceRateLimiter> log) : ISalesforceRateLimiter
{
    /// <summary>The least wait between attempts to acquire.</summary>
    private static readonly TimeSpan MinWait = TimeSpan.FromMilliseconds(50);

    private readonly RateLimitOptions _opt = options.Value;
    private readonly SalesforceLimitsOptions _limits = limits.Value;
    private readonly object _reportLock = new();
    private DateTime _lastReportUtc = DateTime.MinValue;
    private ThrottleDecision _lastDecision = ThrottleDecision.Normal;
    private long _loggedAllocation;

    /// <summary>
    /// Writes the configured bucket size and rate to the shared row when they differ from what the row holds: the
    /// deploy's <c>sql/apply-rate-limits.sql</c> (the migrate step runs it; nothing is written when the limits haven't
    /// changed). The current throttle factor and stored tokens are kept.
    /// </summary>
    /// <returns>Whether the row was changed.</returns>
    public async Task<bool> ApplyConfigurationAsync(CancellationToken ct)
    {
        var changed = await repository.ApplyLimitsAsync(_opt.BurstCapacity, _opt.MaxRequestsPerSecond, ct);

        log.LogInformation(
            "Salesforce limits {Outcome}: {Rate} calls/s, burst {Burst}, max {Concurrent} concurrent long-running requests; " +
            "the daily allocation is read from Sforce-Limit-Info",
            changed ? "updated" : "unchanged", _opt.MaxRequestsPerSecond, _opt.BurstCapacity, _limits.ConcurrentLongRunningLimit);
        return changed;
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
            var decision = await repository.DecideAsync(locked =>
            {
                var decision = TokenBucket.Decide(locked.Row, locked.Now, oneCall);
                if (decision.Granted)
                {
                    locked.Row.Tokens = decision.Tokens;
                    locked.Row.UpdatedAt = locked.Now;
                }
                return decision;
            }, ct);

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
        return repository.IsPausedAsync(ct);
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
        var decision = Decide(info);

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
            row.UsageUpdatedAt = now;
        }, ct);
    }

    /// <summary>
    /// With the row locked, refills its tokens at the current rate (so a change of rate or capacity applies from now on)
    /// and applies <paramref name="change"/>.
    /// </summary>
    private Task UpdateAsync(Action<RateLimitState, DateTimeOffset> change, CancellationToken ct)
    {
        return repository.UpdateAsync((row, now) =>
        {
            row.Tokens = TokenBucket.Refill(row, now);
            row.UpdatedAt = now;
            change(row, now);
        }, ct);
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

    /// <param name="info">What Salesforce last reported in Sforce-Limit-Info.</param>
    internal ThrottleDecision Decide(SalesforceLimitInfo info)
    {
        var daily = info.DailyRatio;
        return daily >= _opt.DailyHardRatio
            ? new ThrottleDecision(ThrottleLevel.DailyExhausted, _opt.DailySoftSpeed, _opt.DailyHardPause)
            : daily >= _opt.DailySoftRatio
            ? new ThrottleDecision(ThrottleLevel.DailyLow, _opt.DailySoftSpeed, TimeSpan.Zero)
            : ThrottleDecision.Normal;
    }

    /// <summary>Logs the allocation Salesforce reports, once per process and again whenever it changes.</summary>
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
        log.LogInformation("Salesforce reports a daily API allocation of {Reported} calls: slowing down at {Soft:P0}, pausing at {Hard:P0}",
            usage.Max, _opt.DailySoftRatio, _opt.DailyHardRatio);
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