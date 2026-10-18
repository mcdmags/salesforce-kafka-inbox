using Microsoft.Extensions.Options;
using SalesforceSync.Infrastructure.RateLimiting;

namespace SalesforceSync.RateLimiting;

/// <summary>
/// The rate limiter of this process: a token bucket, a pause, and the throttle that follows the Sforce-Limit-Info header
/// Salesforce returns on each call. The header carries the org's (and the connected app's) daily usage, so it slows the
/// bucket down at <see cref="RateLimitOptions.DailySoftRatio"/> of the daily allocation and pauses at
/// <see cref="RateLimitOptions.DailyHardRatio"/>; Salesforce counts the allocation over a rolling 24 hours and treats it
/// as a soft limit, so there is no count of our own to keep. Salesforce has no per-second limit and caps only the
/// concurrent requests of 20 seconds or longer, so the pods share nothing: each learns the same ratio from its own
/// responses, and a pod that meets a limit error pauses itself.
/// </summary>
public sealed class SalesforceRateLimiter : ISalesforceRateLimiter
{
    /// <summary>The least wait between attempts to acquire.</summary>
    private static readonly TimeSpan MinWait = TimeSpan.FromMilliseconds(50);

    private readonly TimeProvider _clock;
    private readonly RateLimitOptions _opt;
    private readonly ILogger<SalesforceRateLimiter> _log;
    private readonly object _lock = new();
    private readonly TokenBucket _bucket;
    private DateTimeOffset? _pausedUntil;
    private ThrottleDecision _lastDecision = ThrottleDecision.Normal;
    private long _loggedAllocation;

    public SalesforceRateLimiter(TimeProvider clock, IOptions<RateLimitOptions> options, ILogger<SalesforceRateLimiter> log)
    {
        _clock = clock;
        _opt = options.Value;
        _log = log;
        _bucket = new TokenBucket(_opt.BurstCapacity, _opt.MaxRequestsPerSecond, clock.GetUtcNow());
    }

    public bool IsPaused
    {
        get
        {
            lock (_lock)
            {
                return _pausedUntil > _clock.GetUtcNow();
            }
        }
    }

    /// <summary>
    /// Takes a token for one call, waiting up to <see cref="RateLimitOptions.MaxWait"/> for it (and for a pause to end),
    /// else throws <see cref="RateLimitedException"/>.
    /// </summary>
    public async Task AcquireAsync(CancellationToken ct)
    {
        var deadline = _clock.GetUtcNow() + _opt.MaxWait;
        while (true)
        {
            var decision = TryAcquire();
            if (decision.IsGranted)
            {
                return;
            }

            var wait = (decision.Wait > MinWait ? decision.Wait : MinWait)
                       + TimeSpan.FromMilliseconds(Random.Shared.Next(0, 100)); // jitter
            if (_clock.GetUtcNow() + wait > deadline)
            {
                throw new RateLimitedException(wait);
            }

            await Task.Delay(wait, _clock, ct);
        }
    }

    public void Pause(TimeSpan duration)
    {
        lock (_lock)
        {
            PauseLocked(duration);
        }
    }

    /// <summary>Slows the bucket down near the daily or per-app allocation, pauses near exhaustion. api-bursts is not acted on (see <see cref="SalesforceLimitInfo"/>).</summary>
    public void ReportLimits(SalesforceLimitInfo info)
    {
        LogAllocation(info);
        var decision = Decide(info);
        lock (_lock)
        {
            if (decision != _lastDecision)
            {
                LogTransition(_lastDecision, decision, info);
                _lastDecision = decision;
            }

            _bucket.SetRate(_clock.GetUtcNow(), Math.Max(_opt.MaxRequestsPerSecond * decision.Speed, 0.01));
            if (decision.Pause > TimeSpan.Zero)
            {
                PauseLocked(decision.Pause);
            }
        }
    }

    private AcquireDecision TryAcquire()
    {
        lock (_lock)
        {
            var now = _clock.GetUtcNow();
            return _pausedUntil > now ? AcquireDecision.NotYet(_pausedUntil.Value - now) : _bucket.TryTake(now, 1);
        }
    }

    private void PauseLocked(TimeSpan duration)
    {
        var until = _clock.GetUtcNow() + duration;
        if (_pausedUntil is null || _pausedUntil < until)
        {
            _pausedUntil = until;
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
        _log.LogInformation("Salesforce reports a daily API allocation of {Reported} calls: slowing down at {Soft:P0}, pausing at {Hard:P0}",
            usage.Max, _opt.DailySoftRatio, _opt.DailyHardRatio);
    }

    private void LogTransition(ThrottleDecision from, ThrottleDecision to, SalesforceLimitInfo info)
    {
        var level = to.Level == ThrottleLevel.Normal ? LogLevel.Information : LogLevel.Warning;
        _log.Log(level,
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
