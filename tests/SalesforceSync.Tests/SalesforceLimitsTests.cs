using System.Text.Json.Nodes;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using SalesforceSync.Data.Inbox;
using SalesforceSync.Hosting;
using SalesforceSync.Infrastructure.RateLimiting;
using SalesforceSync.RateLimiting;

namespace SalesforceSync.Tests;

/// <summary>Limits from "API Request Limits and Allocations" (Salesforce Developer Limits Quick Reference).</summary>
public sealed class SalesforceLimitsTests
{
    [Fact]
    public void Concurrent_long_running_limit_defaults_to_that_of_a_production_org()
    {
        new SalesforceLimitsOptions().ConcurrentLongRunningLimit.Should().Be(25);
    }

    // ---------- Startup validation ----------

    [Fact]
    public void Defaults_are_valid()
    {
        Validate(Limits()).Succeeded.Should().BeTrue();
    }

    [Fact]
    public void More_workers_than_the_org_allows_concurrent_requests_fails_startup()
    {
        var result = Validate(Limits(concurrentLongRunningLimit: 5), new SyncOptions { Workers = 6 });

        result.Failed.Should().BeTrue();
        result.FailureMessage.Should().Contain("Workers (6) exceeds the org's limit of 5");
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void A_non_positive_concurrent_long_running_limit_fails_startup(int limit)
    {
        var result = Validate(Limits(concurrentLongRunningLimit: limit));

        result.Failed.Should().BeTrue();
        result.FailureMessage.Should().Contain("SalesforceLimits:ConcurrentLongRunningLimit");
    }

    [Fact]
    public void Request_timeout_above_the_10_minute_API_timeout_fails_startup()
    {
        var limits = Limits();
        limits.RequestTimeout = TimeSpan.FromMinutes(11);

        var result = Validate(limits);

        result.Failed.Should().BeTrue();
        result.FailureMessage.Should().Contain("API timeout");
    }

    // ---------- REQUEST_LIMIT_EXCEEDED ----------

    [Fact]
    public void Concurrent_long_running_limit_error_is_recognised()
    {
        SalesforceRateLimitHandler.Classify(
                /*lang=json,strict*/ """[{"message":"ConcurrentPerOrgLongTxn Limit exceeded.","errorCode":"REQUEST_LIMIT_EXCEEDED"}]""")
            .Should().Be(SalesforceRateLimitHandler.LimitExceeded.ConcurrentLongRunning);
    }

    [Fact]
    public void Total_requests_limit_error_is_recognised()
    {
        SalesforceRateLimitHandler.Classify(
                /*lang=json,strict*/ """[{"message":"TotalRequests Limit exceeded.","errorCode":"REQUEST_LIMIT_EXCEEDED"}]""")
            .Should().Be(SalesforceRateLimitHandler.LimitExceeded.TotalRequests);
    }

    [Theory, AutoData]
    public void Unknown_limit_error_is_treated_as_the_daily_limit(string message)
    {
        SalesforceRateLimitHandler.Classify(
                new JsonArray(new JsonObject { ["message"] = message, ["errorCode"] = "REQUEST_LIMIT_EXCEEDED" }).ToJsonString())
            .Should().Be(SalesforceRateLimitHandler.LimitExceeded.TotalRequests, "the safer assumption for an unknown variant");
    }

    [Fact]
    public void Other_403s_are_not_limit_errors()
    {
        SalesforceRateLimitHandler.Classify(/*lang=json,strict*/ """[{"errorCode":"INSUFFICIENT_ACCESS"}]""").Should().BeNull();
    }

    // ---------- Throttle decisions ----------

    [Fact]
    public void The_limit_header_is_read_whatever_its_case()
    {
        using var response = new HttpResponseMessage();
        response.Headers.TryAddWithoutValidation("SForce-Limit-Info", "api-usage=25/5000;");

        response.Headers.TryGetValues(SalesforceRateLimitHandler.LimitInfoHeader, out var values).Should().BeTrue();
        SalesforceLimitInfo.Parse(string.Join("; ", values!))!.ApiUsage.Should().Be(new LimitCounter(25, 5000));
    }

    [Fact]
    public void Org_usage_pauses_near_the_daily_limit()
    {
        var limiter = Limiter(new RateLimitOptions());

        limiter.Decide(new SalesforceLimitInfo(new LimitCounter(10_000, 100_000), null, null, null)).Should().Be(SalesforceRateLimiter.ThrottleDecision.Normal);
        limiter.Decide(new SalesforceLimitInfo(new LimitCounter(85_000, 100_000), null, null, null)).Should().Be(SalesforceRateLimiter.ThrottleDecision.Normal);
        limiter.Decide(new SalesforceLimitInfo(new LimitCounter(96_000, 100_000), null, null, null)).Pause.Should().Be(TimeSpan.FromMinutes(15), "the org is past DailyHardRatio");
    }

    [Fact]
    public void Api_bursts_is_recorded_but_not_throttled_on()
    {
        // Salesforce's REST guide shows api-bursts in an example but doesn't define it
        var info = SalesforceLimitInfo.Parse("api-usage=10018/100000; api-bursts=749/750")!;

        info.ApiBursts.Should().Be(new LimitCounter(749, 750));
        Limiter(new RateLimitOptions()).Decide(info).Should().Be(SalesforceRateLimiter.ThrottleDecision.Normal);
    }

    [Fact]
    public void Lease_must_outlast_the_request_timeout()
    {
        // A call that is merely slow must end (or be given up on) while its entities are still leased
        var limits = Limits();
        limits.RequestTimeout = TimeSpan.FromSeconds(90);

        Validate(limits, new SyncOptions { Lease = TimeSpan.FromSeconds(90) }).FailureMessage.Should().Contain("Sync:Lease");
        Validate(limits, new SyncOptions { Lease = TimeSpan.FromSeconds(91) }).Succeeded.Should().BeTrue();
    }

    [Fact]
    public void Unknown_outcome_hold_defaults_to_the_Salesforce_api_timeout_and_must_be_positive()
    {
        new SyncOptions().UnknownOutcomeHold.Should().Be(SalesforceLimitsOptions.MaxApiTimeout);
        new SyncOptionsValidator().Validate(null, new SyncOptions { UnknownOutcomeHold = TimeSpan.Zero })
            .FailureMessage.Should().Contain("UnknownOutcomeHold");
        new SyncOptionsValidator().Validate(null, new SyncOptions()).Succeeded.Should().BeTrue();
    }

    /// <summary>The worker's default limits (appsettings.json).</summary>
    private static SalesforceLimitsOptions Limits(int concurrentLongRunningLimit = 25)
    {
        return new() { ConcurrentLongRunningLimit = concurrentLongRunningLimit };
    }

    private static ValidateOptionsResult Validate(SalesforceLimitsOptions limits, SyncOptions? sync = null,
        RateLimitOptions? rate = null)
    {
        return new SalesforceLimitsValidator(Options.Create(sync ?? new SyncOptions()), Options.Create(rate ?? new RateLimitOptions()))
                .Validate(null, limits);
    }

    private static SalesforceRateLimiter Limiter(RateLimitOptions rate)
    {
        return new(TimeProvider.System, Options.Create(rate), NullLogger<SalesforceRateLimiter>.Instance);
    }
}