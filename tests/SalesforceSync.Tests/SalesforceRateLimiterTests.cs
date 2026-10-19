using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using SalesforceSync.Infrastructure.RateLimiting;
using SalesforceSync.RateLimiting;

namespace SalesforceSync.Tests;

/// <summary>The pause of <see cref="SalesforceRateLimiter"/>, which every Salesforce call waits for.</summary>
public sealed class SalesforceRateLimiterTests
{
    private static readonly DateTimeOffset Now = new(2026, 10, 16, 12, 34, 56, TimeSpan.Zero);

    [Fact]
    public void Paused_waits_until_the_pause_ends()
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
    public async Task A_call_that_is_not_paused_goes_through_at_once()
    {
        var limiter = Limiter(new ManualClock(Now));

        for (var i = 0; i < 100; i++)
        {
            await limiter.AcquireAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task A_call_that_cannot_get_through_a_pause_within_the_max_wait_is_rate_limited()
    {
        var limiter = Limiter(new ManualClock(Now), new RateLimitOptions { MaxWait = TimeSpan.FromMilliseconds(200) });
        limiter.Pause(TimeSpan.FromMinutes(1));

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
