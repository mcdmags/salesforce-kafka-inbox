using SalesforceSync.Infrastructure.RateLimiting;

namespace SalesforceSync.Data.RateLimiting;

/// <summary>
/// The arithmetic of the shared <c>sf_rate_limit</c> row, without I/O: <see cref="SalesforceRateLimiter"/> locks the
/// row, reads the database clock and the usage in the window, and writes back what this decides.
/// </summary>
internal static class TokenBucket
{
    /// <summary>The least wait when the budget is used up: usage frees up a bucket at a time, not continuously.</summary>
    public static readonly TimeSpan MinBudgetWait = TimeSpan.FromSeconds(60);

    /// <summary>The row's tokens at <paramref name="now"/>: those stored plus the refill since, at most its capacity.</summary>
    public static double Refill(RateLimitState s, DateTimeOffset now)
    {
        return Math.Min(s.Capacity, s.Tokens + ((now - s.UpdatedAt).TotalSeconds * s.RefillPerSec));
    }

    /// <summary>The start of the usage bucket <paramref name="now"/> falls in.</summary>
    public static DateTimeOffset BucketOf(DateTimeOffset now, TimeSpan bucketSize)
    {
        return new DateTimeOffset(now.UtcTicks - (now.UtcTicks % bucketSize.Ticks), TimeSpan.Zero);
    }

    /// <summary>Whether <paramref name="permits"/> calls may be made now, and if not, how long until they may.</summary>
    /// <param name="used">This integration's calls in the buckets since <see cref="RateLimitOptions.CountedWindow"/> ago.</param>
    /// <param name="oldestBucket">The oldest of those buckets, null if there are none.</param>
    /// <param name="s">The shared bucket row, locked by the caller.</param>
    /// <param name="now">The database clock, read with the lock.</param>
    /// <param name="permits">Calls to make.</param>
    /// <param name="options">The budget window and usage bucket size.</param>
    public static AcquireDecision Decide(RateLimitState s, DateTimeOffset now, long used, DateTimeOffset? oldestBucket,
        int permits, RateLimitOptions options)
    {
        if (s.PausedUntil > now)
        {
            return AcquireDecision.NotYet(s.PausedUntil.Value - now);
        }

        if (s.DailyBudget is { } budget && used + permits > budget)
        {
            // Calls free up when the oldest bucket leaves the window
            var freed = (oldestBucket + options.CountedWindow - now) ?? TimeSpan.Zero;
            return AcquireDecision.NotYet(freed > MinBudgetWait ? freed : MinBudgetWait);
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