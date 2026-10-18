using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using SalesforceSync.Infrastructure.RateLimiting;
using SalesforceSync.RateLimiting;

namespace SalesforceSync.Tests;

/// <summary>The bucket and pause of <see cref="SalesforceRateLimiter"/>, which every Salesforce call takes a token from.</summary>
public sealed class TokenBucketTests
{
    private static readonly DateTimeOffset Now = new(2026, 10, 16, 12, 34, 56, TimeSpan.Zero);

    [Fact]
    public void Grants_and_spends_the_tokens_refilled_since_the_last_take()
    {
        // The default bucket (capacity 5, 2 tokens/s), emptied 2 s ago: 4 available
        var bucket = new TokenBucket(5, 2, Now);
        bucket.TryTake(Now, 5).IsGranted.Should().BeTrue();

        bucket.Tokens(Now.AddSeconds(2)).Should().Be(4);
        bucket.TryTake(Now.AddSeconds(2), 2).Should().Be(AcquireDecision.Granted);
        bucket.Tokens(Now.AddSeconds(2)).Should().Be(2);
    }

    [Fact]
    public void Refill_is_capped_at_the_capacity()
    {
        var bucket = new TokenBucket(5, 2, Now);
        bucket.TryTake(Now, 5);

        bucket.Tokens(Now.AddHours(1)).Should().Be(5);
    }

    [Fact]
    public void Out_of_tokens_waits_until_enough_have_refilled()
    {
        // 1 token left, 2 needed at 2 tokens/s: 0.5 s
        var bucket = new TokenBucket(5, 2, Now);
        bucket.TryTake(Now, 4);

        var decision = bucket.TryTake(Now, 2);

        decision.IsGranted.Should().BeFalse();
        decision.Wait.Should().Be(TimeSpan.FromSeconds(0.5));
        bucket.Tokens(Now).Should().Be(1, "a refused take spends nothing");
    }

    [Fact]
    public void A_new_rate_applies_from_then_on_and_keeps_the_tokens_refilled_before()
    {
        var bucket = new TokenBucket(10, 2, Now);
        bucket.TryTake(Now, 10);

        bucket.SetRate(Now.AddSeconds(2), 0.5); // 4 tokens refilled at 2/s, then 0.5/s

        bucket.Tokens(Now.AddSeconds(2)).Should().Be(4);
        bucket.Tokens(Now.AddSeconds(4)).Should().Be(5);
    }

    [Fact]
    public void Paused_waits_until_the_pause_ends_whatever_the_tokens()
    {
        var clock = new ManualClock(Now);
        var limiter = Limiter(clock);

        limiter.Pause(TimeSpan.FromSeconds(30));

        limiter.IsPaused.Should().BeTrue();
        clock.Advance(TimeSpan.FromSeconds(30));
        limiter.IsPaused.Should().BeFalse("the pause has ended");
    }

    [Fact]
    public void A_shorter_pause_does_not_cut_a_longer_one()
    {
        var clock = new ManualClock(Now);
        var limiter = Limiter(clock);

        limiter.Pause(TimeSpan.FromSeconds(30));
        limiter.Pause(TimeSpan.FromSeconds(5));
        clock.Advance(TimeSpan.FromSeconds(10));

        limiter.IsPaused.Should().BeTrue();
    }

    [Fact]
    public async Task A_call_that_cannot_get_a_token_within_the_max_wait_is_rate_limited()
    {
        var limiter = Limiter(new ManualClock(Now), new RateLimitOptions { BurstCapacity = 1, MaxRequestsPerSecond = 0.001, MaxWait = TimeSpan.FromMilliseconds(200) });
        await limiter.AcquireAsync(CancellationToken.None);

        await FluentActions.Awaiting(() => limiter.AcquireAsync(CancellationToken.None)).Should().ThrowAsync<RateLimitedException>();
    }

    private static SalesforceRateLimiter Limiter(TimeProvider clock, RateLimitOptions? options = null)
    {
        return new SalesforceRateLimiter(clock, Options.Create(options ?? new RateLimitOptions()), NullLogger<SalesforceRateLimiter>.Instance);
    }

    /// <summary>A clock that moves only when told to.</summary>
    private sealed class ManualClock(DateTimeOffset start) : TimeProvider
    {
        private DateTimeOffset _now = start;

        public override DateTimeOffset GetUtcNow()
        {
            return _now;
        }

        public void Advance(TimeSpan by)
        {
            _now += by;
        }
    }
}
