using Microsoft.Extensions.Options;
using SalesforceSync.Infrastructure.RateLimiting;

namespace SalesforceSync.RateLimiting;

/// <summary>
/// The rate limiter of this process: a pause, set from the Sforce-Limit-Info header Salesforce returns on each call.
/// The header carries the org's (and the connected app's) daily usage, so the process pauses at
/// <see cref="RateLimitOptions.DailyHardRatio"/> of the daily allocation. Salesforce counts the allocation over a
/// rolling 24 hours and treats it as a soft limit, so we keep no count of our own. Salesforce has no per-second limit
/// and caps only concurrent requests of 20 seconds or longer (bounded by <c>Sync:Workers</c> x pods). So pods share
/// nothing: each learns the same ratio from its own responses, and a pod that meets a limit error pauses itself.
/// </summary>
public sealed class SalesforceRateLimiter : ISalesforceRateLimiter
{
    /// <summary>The least wait between attempts to acquire.</summary>
    private static readonly TimeSpan MinWait = TimeSpan.FromMilliseconds(50);

    private readonly TimeProvider _clock;
    private readonly RateLimitOptions _opt;
    private readonly ILogger<SalesforceRateLimiter> _log;
    private readonly object _lock = new();
    private DateTimeOffset? _pausedUntil;
    private ThrottleDecision _lastDecision = ThrottleDecision.Normal;
    private long _loggedAllocation;

    public SalesforceRateLimiter(TimeProvider clock, IOptions<RateLimitOptions> options, ILogger<SalesforceRateLimiter> log)
    {
        _clock = clock;
        _opt = options.Value;
        _log = log;
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
    /// Lets one call through. Waits up to <see cref="RateLimitOptions.MaxWait"/> for a pause to end, else throws
    /// <see cref="RateLimitedException"/>.
    /// </summary>
    public async Task AcquireAsync(CancellationToken ct)
    {
        var deadline = _clock.GetUtcNow() + _opt.MaxWait;
        while (true)
        {
            var remaining = PauseRemaining();
            if (remaining <= TimeSpan.Zero)
            {
                return;
            }

            var wait = (remaining > MinWait ? remaining : MinWait)
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

    /// <summary>Pauses near exhaustion of the daily or per-app allocation. api-bursts is not acted on (see <see cref="SalesforceLimitInfo"/>).</summary>
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

            if (decision.Pause > TimeSpan.Zero)
            {
                PauseLocked(decision.Pause);
            }
        }
    }

    private TimeSpan PauseRemaining()
    {
        lock (_lock)
        {
            return _pausedUntil is { } until ? until - _clock.GetUtcNow() : TimeSpan.Zero;
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
        return info.DailyRatio >= _opt.DailyHardRatio
            ? new ThrottleDecision(ThrottleLevel.DailyExhausted, _opt.DailyHardPause)
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
        _log.LogInformation("Salesforce reports a daily API allocation of {Reported} calls: pausing at {Hard:P0}",
            usage.Max, _opt.DailyHardRatio);
    }

    private void LogTransition(ThrottleDecision from, ThrottleDecision to, SalesforceLimitInfo info)
    {
        var level = to.Level == ThrottleLevel.Normal ? LogLevel.Information : LogLevel.Warning;
        _log.Log(level,
            "Salesforce throttle {From} -> {To} (pause {Pause}). api-usage={Api} api-bursts={Bursts} per-app={App} ({AppName})",
            from.Level, to.Level, to.Pause,
            info.ApiUsage, info.ApiBursts, info.PerAppUsage, info.AppName);
    }

    internal enum ThrottleLevel { Normal, DailyExhausted }

    internal readonly record struct ThrottleDecision(ThrottleLevel Level, TimeSpan Pause)
    {
        public static ThrottleDecision Normal => new(ThrottleLevel.Normal, TimeSpan.Zero);
    }
}
