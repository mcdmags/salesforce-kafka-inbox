using System.Text.Json.Nodes;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Npgsql;
using SalesforceSync.Data;
using SalesforceSync.Inbox;
using SalesforceSync.RateLimiting;

namespace SalesforceSync.Tests;

/// <summary>Limits from "API Request Limits and Allocations" (Salesforce Developer Limits Quick Reference).</summary>
public sealed class SalesforceLimitsTests
{
    [Fact]
    public void Enterprise_org_with_15_Salesforce_licenses_gets_115000_calls()
    {
        // The worked example from the Salesforce documentation
        var limits = new SalesforceLimitsOptions { Edition = SalesforceEdition.Enterprise, Licenses = { ["Salesforce"] = 15 } };

        limits.EffectiveDailyApiAllocation.Should().Be(115_000);
    }

    [Fact]
    public void Allocation_adds_every_license_type_and_purchased_add_ons()
    {
        var limits = new SalesforceLimitsOptions
        {
            Edition = SalesforceEdition.Unlimited,
            Licenses = { ["Salesforce"] = 10, ["Partner Community"] = 20, ["Customer Community"] = 500 },
            PurchasedApiCalls = 50_000
        };

        limits.EffectiveDailyApiAllocation.Should().Be(204_000, "it is 100,000 + 10 x 5,000 + 20 x 200 + 500 x 0 + 50,000");
    }

    [Theory]
    [InlineData(SalesforceEdition.Enterprise, 1_000)]
    [InlineData(SalesforceEdition.Professional, 1_000)]
    [InlineData(SalesforceEdition.Unlimited, 5_000)]
    [InlineData(SalesforceEdition.Performance, 5_000)]
    public void Calls_per_Salesforce_license_depend_on_the_edition(SalesforceEdition edition, int calls)
        => new SalesforceLimitsOptions { Edition = edition }.CallsPerLicenseFor("salesforce").Should().Be(calls);

    [Theory]
    [InlineData(SalesforceOrgType.DeveloperEdition, 15_000)]
    [InlineData(SalesforceOrgType.FullSandbox, 5_000_000)]
    public void Developer_Edition_and_Full_Sandbox_have_fixed_allocations(SalesforceOrgType type, long allocation)
    {
        var limits = new SalesforceLimitsOptions { OrgType = type, Licenses = { ["Salesforce"] = 100 } };

        limits.EffectiveDailyApiAllocation.Should().Be(allocation, "the licenses don't matter for this org type");
    }

    [Theory]
    [InlineData(SalesforceOrgType.Sandbox)]
    [InlineData(SalesforceOrgType.Trial)]
    public void Orgs_without_a_published_allocation_must_set_it_from_Setup(SalesforceOrgType type)
    {
        Validate(new SalesforceLimitsOptions { OrgType = type, Licenses = { ["Salesforce"] = 15 } })
            .FailureMessage.Should().Contain("DailyApiAllocation", "Salesforce doesn't publish this org type's allocation");
        Validate(new SalesforceLimitsOptions { OrgType = type, DailyApiAllocation = 200_000 }, new SyncOptions { MaxConcurrentRequests = 2 })
            .Succeeded.Should().BeTrue();
    }

    [Theory, AutoData]
    public void Allocation_from_Setup_overrides_the_formula(long fromSetup)
    {
        var limits = new SalesforceLimitsOptions { Licenses = { ["Salesforce"] = 15 }, DailyApiAllocation = fromSetup };

        limits.EffectiveDailyApiAllocation.Should().Be(fromSetup);
    }

    [Theory, AutoData]
    public void Custom_license_types_can_be_added(string customLicense)
    {
        var limits = new SalesforceLimitsOptions
        {
            Licenses = { ["Salesforce"] = 1, [customLicense] = 4 },
            CallsPerLicense = { [customLicense] = 250 }
        };

        limits.EffectiveDailyApiAllocation.Should().Be(102_000, "it is 100,000 + 1 x 1,000 + 4 x 250");
    }

    [Theory]
    [InlineData(SalesforceOrgType.Production, 25)]
    [InlineData(SalesforceOrgType.Sandbox, 25)]
    [InlineData(SalesforceOrgType.FullSandbox, 25)]
    [InlineData(SalesforceOrgType.DeveloperEdition, 5)]
    [InlineData(SalesforceOrgType.Trial, 5)]
    public void Concurrent_long_running_limit_depends_on_the_org_type(SalesforceOrgType type, int limit)
        => new SalesforceLimitsOptions { OrgType = type }.EffectiveConcurrentLongRunningLimit.Should().Be(limit);

    // ---------- Startup validation ----------

    [Fact]
    public void Defaults_are_valid()
        => Validate(new SalesforceLimitsOptions()).Succeeded.Should().BeTrue();

    [Fact]
    public void More_concurrent_requests_than_the_org_allows_fails_startup()
    {
        var result = Validate(new SalesforceLimitsOptions { OrgType = SalesforceOrgType.DeveloperEdition },
            new SyncOptions { MaxConcurrentRequests = 6 });

        result.Failed.Should().BeTrue();
        result.FailureMessage.Should().Contain("MaxConcurrentRequests (6) exceeds the org's limit of 5");
    }

    [Fact]
    public void Request_timeout_above_the_10_minute_API_timeout_fails_startup()
    {
        var result = Validate(new SalesforceLimitsOptions { RequestTimeout = TimeSpan.FromMinutes(11) });

        result.Failed.Should().BeTrue();
        result.FailureMessage.Should().Contain("API timeout");
    }

    [Theory, AutoData]
    public void Unknown_license_type_fails_startup(string unknownLicense)
    {
        var result = Validate(new SalesforceLimitsOptions { Licenses = { [unknownLicense] = 1 } });

        result.Failed.Should().BeTrue();
        result.FailureMessage.Should().Contain($"Unknown Salesforce license type '{unknownLicense}'");
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1.5)]
    public void Daily_budget_share_must_be_a_fraction(double share)
        => Validate(new SalesforceLimitsOptions(), rate: new RateLimitOptions { DailyBudgetShare = share }).Failed.Should().BeTrue();

    // ---------- REQUEST_LIMIT_EXCEEDED ----------

    [Fact]
    public void Concurrent_long_running_limit_error_is_recognised()
        => SalesforceRateLimitHandler.Classify(
                """[{"message":"ConcurrentPerOrgLongTxn Limit exceeded.","errorCode":"REQUEST_LIMIT_EXCEEDED"}]""")
            .Should().Be(SalesforceRateLimitHandler.LimitExceeded.ConcurrentLongRunning);

    [Fact]
    public void Total_requests_limit_error_is_recognised()
        => SalesforceRateLimitHandler.Classify(
                """[{"message":"TotalRequests Limit exceeded.","errorCode":"REQUEST_LIMIT_EXCEEDED"}]""")
            .Should().Be(SalesforceRateLimitHandler.LimitExceeded.TotalRequests);

    [Theory, AutoData]
    public void Unknown_limit_error_is_treated_as_the_daily_limit(string message)
        => SalesforceRateLimitHandler.Classify(
                new JsonArray(new JsonObject { ["message"] = message, ["errorCode"] = "REQUEST_LIMIT_EXCEEDED" }).ToJsonString())
            .Should().Be(SalesforceRateLimitHandler.LimitExceeded.TotalRequests, "the safer assumption for an unknown variant");

    [Fact]
    public void Other_403s_are_not_limit_errors()
        => SalesforceRateLimitHandler.Classify("""[{"errorCode":"INSUFFICIENT_ACCESS"}]""").Should().BeNull();

    // ---------- Throttle decisions ----------

    [Fact]
    public void Daily_budget_is_the_share_of_the_org_allocation()
        => Limiter(new RateLimitOptions { DailyBudgetShare = 0.5 }, new SalesforceLimitsOptions { Licenses = { ["Salesforce"] = 15 } })
            .DailyBudget.Should().Be(57_500, "it is half of 115,000");

    [Fact]
    public void Own_usage_of_the_budget_slows_down_even_when_the_org_has_room()
    {
        var limiter = Limiter(new RateLimitOptions());
        var orgAt10Percent = new SalesforceLimitInfo(new LimitCounter(10_000, 100_000), null, null, null);

        limiter.Decide(orgAt10Percent, ownDailyRatio: 0.5).Speed.Should().Be(1.0);
        limiter.Decide(orgAt10Percent, ownDailyRatio: 0.85).Speed.Should().Be(0.25, "own usage is past DailySoftRatio");
        limiter.Decide(orgAt10Percent, ownDailyRatio: 0.96).Pause.Should().Be(TimeSpan.FromMinutes(15), "own usage is past DailyHardRatio");
    }

    [Fact]
    public void Org_usage_slows_down_even_when_own_budget_has_room()
    {
        var limiter = Limiter(new RateLimitOptions());
        var orgAt85Percent = new SalesforceLimitInfo(new LimitCounter(85_000, 100_000), null, null, null);

        limiter.Decide(orgAt85Percent, ownDailyRatio: 0.1).Speed.Should().Be(0.25, "the org is past DailySoftRatio");
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
        Validate(new SalesforceLimitsOptions { RequestTimeout = TimeSpan.FromSeconds(90) }, new SyncOptions { Lease = TimeSpan.FromSeconds(90) })
            .FailureMessage.Should().Contain("Sync:Lease");
        Validate(new SalesforceLimitsOptions { RequestTimeout = TimeSpan.FromSeconds(90) }, new SyncOptions { Lease = TimeSpan.FromSeconds(91) })
            .Succeeded.Should().BeTrue();
    }

    [Fact]
    public void Unknown_outcome_hold_defaults_to_the_Salesforce_api_timeout_and_must_be_positive()
    {
        new SyncOptions().UnknownOutcomeHold.Should().Be(SalesforceLimitsOptions.MaxApiTimeout);
        new SyncOptionsValidator().Validate(null, new SyncOptions { UnknownOutcomeHold = TimeSpan.Zero })
            .FailureMessage.Should().Contain("UnknownOutcomeHold");
        new SyncOptionsValidator().Validate(null, new SyncOptions { MaxEventsPerEntity = 0 })
            .FailureMessage.Should().Contain("MaxEventsPerEntity");
        new SyncOptionsValidator().Validate(null, new SyncOptions()).Succeeded.Should().BeTrue();
    }

    private static ValidateOptionsResult Validate(SalesforceLimitsOptions limits, SyncOptions? sync = null,
        RateLimitOptions? rate = null)
        => new SalesforceLimitsValidator(Options.Create(sync ?? new SyncOptions()), Options.Create(rate ?? new RateLimitOptions()))
            .Validate(null, limits);

    private static SalesforceRateLimiter Limiter(RateLimitOptions rate, SalesforceLimitsOptions? limits = null)
        => new(new PostgresExecutionStrategy(NpgsqlDataSource.Create("Host=unused"), Options.Create(new PostgresRetryOptions()),
                NullLogger<PostgresExecutionStrategy>.Instance), Options.Create(rate),
            Options.Create(limits ?? new SalesforceLimitsOptions()), NullLogger<SalesforceRateLimiter>.Instance);
}
