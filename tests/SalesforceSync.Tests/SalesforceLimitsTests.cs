using Microsoft.Extensions.Options;
using SalesforceSync.Data.Inbox;
using SalesforceSync.Hosting;
using SalesforceSync.Infrastructure.Salesforce;

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
        var validator = new FluentOptionsValidator<SyncOptions>(new SyncOptionsValidator());
        validator.Validate(null, new SyncOptions { UnknownOutcomeHold = TimeSpan.Zero }).FailureMessage.Should().Contain("UnknownOutcomeHold");
        validator.Validate(null, new SyncOptions()).Succeeded.Should().BeTrue();
    }

    /// <summary>The worker's default limits (appsettings.json).</summary>
    private static SalesforceLimitsOptions Limits(int concurrentLongRunningLimit = 25)
    {
        return new() { ConcurrentLongRunningLimit = concurrentLongRunningLimit };
    }

    private static ValidateOptionsResult Validate(SalesforceLimitsOptions limits, SyncOptions? sync = null)
    {
        var validator = new SalesforceLimitsValidator(Options.Create(sync ?? new SyncOptions()));
        return new FluentOptionsValidator<SalesforceLimitsOptions>(validator).Validate(null, limits);
    }
}
