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
        var decision = TokenBucket.Decide(Row(tokens: 1, updatedAgo: TimeSpan.FromSeconds(2)), Now, used: 0, null, permits: 2, new RateLimitOptions());

        decision.Should().Be(new AcquireDecision(true, 3, TimeSpan.Zero));
    }

    [Fact]
    public void Refill_is_capped_at_the_capacity()
    {
        var row = Row(tokens: 0, updatedAgo: TimeSpan.FromHours(1));

        TokenBucket.Refill(row, Now).Should().Be(5);
        TokenBucket.Decide(row, Now, used: 0, null, permits: 1, new RateLimitOptions()).Tokens.Should().Be(4);
    }

    [Fact]
    public void Out_of_tokens_waits_until_enough_have_refilled()
    {
        // 0.5 token now, 2 needed at 2 tokens/s: 0.75 s
        var decision = TokenBucket.Decide(Row(tokens: 0.5, updatedAgo: TimeSpan.Zero), Now, used: 0, null, permits: 2, new RateLimitOptions());

        decision.Granted.Should().BeFalse();
        decision.Wait.Should().Be(TimeSpan.FromSeconds(0.75));
    }

    [Fact]
    public void Paused_waits_until_the_pause_ends_whatever_the_tokens()
    {
        var row = Row(tokens: 5, updatedAgo: TimeSpan.Zero);
        row.PausedUntil = Now.AddSeconds(30);

        TokenBucket.Decide(row, Now, used: 0, null, permits: 1, new RateLimitOptions())
            .Should().Be(AcquireDecision.NotYet(TimeSpan.FromSeconds(30)));

        row.PausedUntil = Now;
        TokenBucket.Decide(row, Now, used: 0, null, permits: 1, new RateLimitOptions()).Granted.Should().BeTrue("the pause has ended");
    }

    [Fact]
    public void Over_the_budget_waits_until_the_oldest_bucket_leaves_the_window()
    {
        var row = Row(tokens: 5, updatedAgo: TimeSpan.Zero, budget: 100);
        // Counted over 24 h + one 10-minute bucket: the oldest, 24 h ago, leaves in 10 minutes
        var oldest = Now - TimeSpan.FromHours(24);

        TokenBucket.Decide(row, Now, used: 99, oldest, permits: 1, new RateLimitOptions()).Granted.Should().BeTrue("the budget allows exactly 100");
        TokenBucket.Decide(row, Now, used: 100, oldest, permits: 1, new RateLimitOptions())
            .Should().Be(AcquireDecision.NotYet(TimeSpan.FromMinutes(10)));
    }

    [Fact]
    public void Over_the_budget_waits_at_least_a_minute()
    {
        var row = Row(tokens: 5, updatedAgo: TimeSpan.Zero, budget: 0);
        var leavingNow = Now - TimeSpan.FromHours(24) - TimeSpan.FromMinutes(10);

        TokenBucket.Decide(row, Now, used: 10, leavingNow, permits: 1, new RateLimitOptions()).Wait.Should().Be(TokenBucket.MinBudgetWait);
        TokenBucket.Decide(row, Now, used: 0, null, permits: 1, new RateLimitOptions()).Wait.Should().Be(TokenBucket.MinBudgetWait,
            "a budget of 0 allows nothing, even with no usage");
    }

    [Fact]
    public void A_configured_window_and_bucket_set_when_usage_frees_up()
    {
        var options = new RateLimitOptions { BudgetWindow = TimeSpan.FromHours(2), UsageBucket = TimeSpan.FromMinutes(30) };
        var row = Row(tokens: 5, updatedAgo: TimeSpan.Zero, budget: 10);

        // Counted over 2 h 30 min: a bucket 2 h old leaves in 30 minutes
        TokenBucket.Decide(row, Now, used: 10, Now.AddHours(-2), permits: 1, options).Wait.Should().Be(TimeSpan.FromMinutes(30));
        options.CountedWindow.Should().Be(TimeSpan.FromMinutes(150));
    }

    [Fact]
    public void Calls_are_counted_in_the_bucket_their_time_falls_in()
    {
        TokenBucket.BucketOf(Now, TimeSpan.FromMinutes(10)).Should().Be(new DateTimeOffset(2026, 10, 16, 12, 30, 0, TimeSpan.Zero));
        TokenBucket.BucketOf(Now, TimeSpan.FromHours(1)).Should().Be(new DateTimeOffset(2026, 10, 16, 12, 0, 0, TimeSpan.Zero));
        TokenBucket.BucketOf(Now.ToOffset(TimeSpan.FromHours(2)), TimeSpan.FromMinutes(10)).Offset.Should().Be(TimeSpan.Zero);
    }

    /// <summary>The default bucket: capacity 5, 2 tokens/s.</summary>
    private static RateLimitState Row(double tokens, TimeSpan updatedAgo, long? budget = null)
    {
        return new RateLimitState
        {
            Name = "salesforce",
            Tokens = tokens,
            Capacity = 5,
            BaseRefillPerSec = 2,
            RefillPerSec = 2,
            UpdatedAt = Now - updatedAgo,
            DailyBudget = budget
        };
    }
}