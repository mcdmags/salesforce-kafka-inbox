using SalesforceSync.Data.RateLimiting;

namespace SalesforceSync.RateLimiting;

/// <summary>
/// The arithmetic of the shared <c>sf_rate_limit</c> row, without I/O: <see cref="SalesforceRateLimiter"/> has the
/// row locked (<see cref="RateLimitRepository"/>) and writes back what this decides.
/// </summary>
internal static class TokenBucket
{
    /// <summary>The row's tokens at <paramref name="now"/>: those stored plus the refill since, at most its capacity.</summary>
    public static double Refill(RateLimitState s, DateTimeOffset now)
    {
        return Math.Min(s.Capacity, s.Tokens + ((now - s.UpdatedAt).TotalSeconds * s.RefillPerSec));
    }

    /// <summary>Whether <paramref name="permits"/> calls may be made now, and if not, how long until they may.</summary>
    /// <param name="s">The shared bucket row, locked by the caller.</param>
    /// <param name="now">The database clock, read with the lock.</param>
    /// <param name="permits">Calls to make.</param>
    public static AcquireDecision Decide(RateLimitState s, DateTimeOffset now, int permits)
    {
        if (s.PausedUntil > now)
        {
            return AcquireDecision.NotYet(s.PausedUntil.Value - now);
        }

        var available = Refill(s, now);
        return available < permits
            ? AcquireDecision.NotYet(TimeSpan.FromSeconds((permits - available) / s.RefillPerSec))
            : new AcquireDecision(true, available - permits, TimeSpan.Zero);
    }
}

/// <param name="Granted">Whether the calls may be made now.</param>
/// <param name="Tokens">The row's tokens after spending them, when granted.</param>
/// <param name="Wait">How long until they may be made, when not granted.</param>
internal readonly record struct AcquireDecision(bool Granted, double Tokens, TimeSpan Wait)
{
    public static AcquireDecision NotYet(TimeSpan wait)
    {
        return new AcquireDecision(false, 0, wait);
    }
}