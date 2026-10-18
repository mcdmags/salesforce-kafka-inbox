namespace SalesforceSync.RateLimiting;

/// <summary>
/// A token bucket: <see cref="Capacity"/> calls may be made back to back, then <see cref="RatePerSec"/> a second.
/// No I/O and no lock of its own: <see cref="SalesforceRateLimiter"/> uses it under its lock.
/// </summary>
internal sealed class TokenBucket(double capacity, double baseRatePerSec, DateTimeOffset now)
{
    private double _tokens = capacity;
    private DateTimeOffset _updatedAt = now;

    public double Capacity { get; } = capacity;

    /// <summary>The current rate: the normal one, reduced near the daily limit.</summary>
    public double RatePerSec { get; private set; } = baseRatePerSec;

    /// <summary>The tokens at <paramref name="at"/>: those stored plus the refill since, at most the capacity.</summary>
    public double Tokens(DateTimeOffset at)
    {
        return Math.Min(Capacity, _tokens + ((at - _updatedAt).TotalSeconds * RatePerSec));
    }

    /// <summary>Takes <paramref name="permits"/> tokens if there are enough; else says how long until there are.</summary>
    public AcquireDecision TryTake(DateTimeOffset at, int permits)
    {
        var available = Tokens(at);
        if (available < permits)
        {
            return AcquireDecision.NotYet(TimeSpan.FromSeconds((permits - available) / RatePerSec));
        }

        _tokens = available - permits;
        _updatedAt = at;
        return AcquireDecision.Granted;
    }

    /// <summary>Changes the rate from now on (tokens refilled so far at the old rate are kept).</summary>
    public void SetRate(DateTimeOffset at, double ratePerSec)
    {
        _tokens = Tokens(at);
        _updatedAt = at;
        RatePerSec = ratePerSec;
    }
}

/// <param name="IsGranted">Whether the calls may be made now.</param>
/// <param name="Wait">How long until they may be made, when not granted.</param>
internal readonly record struct AcquireDecision(bool IsGranted, TimeSpan Wait)
{
    public static AcquireDecision Granted => new(true, TimeSpan.Zero);

    public static AcquireDecision NotYet(TimeSpan wait)
    {
        return new AcquireDecision(false, wait);
    }
}
