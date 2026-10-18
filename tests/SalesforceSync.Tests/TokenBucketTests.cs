using SalesforceSync.Data.RateLimiting;
using SalesforceSync.RateLimiting;

namespace SalesforceSync.Tests;

/// <summary>The decision <see cref="SalesforceRateLimiter.AcquireAsync"/> makes under the shared row's lock.</summary>
public sealed class TokenBucketTests
{
    private static readonly DateTimeOffset Now = new(2026, 10, 16, 12, 34, 56, TimeSpan.Zero);

    [Fact]
    public void Grants_and_spends_the_tokens_refilled_since_the_last_update()
    {
        // 1 token stored 2 s ago at 2 tokens/s: 5 available
        var decision = TokenBucket.Decide(Row(tokens: 1, updatedAgo: TimeSpan.FromSeconds(2)), Now, permits: 2);

        decision.Should().Be(new AcquireDecision(true, 3, TimeSpan.Zero));
    }

    [Fact]
    public void Refill_is_capped_at_the_capacity()
    {
        var row = Row(tokens: 0, updatedAgo: TimeSpan.FromHours(1));

        TokenBucket.Refill(row, Now).Should().Be(5);
        TokenBucket.Decide(row, Now, permits: 1).Tokens.Should().Be(4);
    }

    [Fact]
    public void Out_of_tokens_waits_until_enough_have_refilled()
    {
        // 0.5 token now, 2 needed at 2 tokens/s: 0.75 s
        var decision = TokenBucket.Decide(Row(tokens: 0.5, updatedAgo: TimeSpan.Zero), Now, permits: 2);

        decision.Granted.Should().BeFalse();
        decision.Wait.Should().Be(TimeSpan.FromSeconds(0.75));
    }

    [Fact]
    public void Paused_waits_until_the_pause_ends_whatever_the_tokens()
    {
        var row = Row(tokens: 5, updatedAgo: TimeSpan.Zero);
        row.PausedUntil = Now.AddSeconds(30);

        TokenBucket.Decide(row, Now, permits: 1)
            .Should().Be(AcquireDecision.NotYet(TimeSpan.FromSeconds(30)));

        row.PausedUntil = Now;
        TokenBucket.Decide(row, Now, permits: 1).Granted.Should().BeTrue("the pause has ended");
    }

    /// <summary>The default bucket: capacity 5, 2 tokens/s.</summary>
    private static RateLimitState Row(double tokens, TimeSpan updatedAgo)
    {
        return new RateLimitState
        {
            Name = "salesforce",
            Tokens = tokens,
            Capacity = 5,
            BaseRefillPerSec = 2,
            RefillPerSec = 2,
            UpdatedAt = Now - updatedAgo
        };
    }
}