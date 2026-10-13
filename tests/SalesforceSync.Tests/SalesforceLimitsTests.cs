using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Npgsql;
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

        Assert.Equal(115_000, limits.EffectiveDailyApiAllocation);
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

        // 100,000 + 10 x 5,000 + 20 x 200 + 500 x 0 + 50,000
        Assert.Equal(204_000, limits.EffectiveDailyApiAllocation);
    }

    [Theory]
    [InlineData(SalesforceEdition.Enterprise, 1_000)]
    [InlineData(SalesforceEdition.Professional, 1_000)]
    [InlineData(SalesforceEdition.Unlimited, 5_000)]
    [InlineData(SalesforceEdition.Performance, 5_000)]
    public void Calls_per_Salesforce_license_depend_on_the_edition(SalesforceEdition edition, int calls)
        => Assert.Equal(calls, new SalesforceLimitsOptions { Edition = edition }.CallsPerLicenseFor("salesforce"));

    [Theory]
    [InlineData(SalesforceOrgType.DeveloperEdition, 15_000)]
    [InlineData(SalesforceOrgType.FullSandbox, 5_000_000)]
    public void Developer_Edition_and_Full_Sandbox_have_fixed_allocations(SalesforceOrgType type, long allocation)
    {
        var limits = new SalesforceLimitsOptions { OrgType = type, Licenses = { ["Salesforce"] = 100 } };

        Assert.Equal(allocation, limits.EffectiveDailyApiAllocation);
    }

    [Fact]
    public void Allocation_from_Setup_overrides_the_formula()
    {
        var limits = new SalesforceLimitsOptions { Licenses = { ["Salesforce"] = 15 }, DailyApiAllocation = 250_000 };

        Assert.Equal(250_000, limits.EffectiveDailyApiAllocation);
    }

    [Fact]
    public void Custom_license_types_can_be_added()
    {
        var limits = new SalesforceLimitsOptions
        {
            Licenses = { ["Salesforce"] = 1, ["My Custom License"] = 4 },
            CallsPerLicense = { ["My Custom License"] = 250 }
        };

        Assert.Equal(102_000, limits.EffectiveDailyApiAllocation);
    }

    [Theory]
    [InlineData(SalesforceOrgType.Production, 25)]
    [InlineData(SalesforceOrgType.Sandbox, 25)]
    [InlineData(SalesforceOrgType.FullSandbox, 25)]
    [InlineData(SalesforceOrgType.DeveloperEdition, 5)]
    [InlineData(SalesforceOrgType.Trial, 5)]
    public void Concurrent_long_running_limit_depends_on_the_org_type(SalesforceOrgType type, int limit)
        => Assert.Equal(limit, new SalesforceLimitsOptions { OrgType = type }.EffectiveConcurrentLongRunningLimit);

    // ---------- Startup validation ----------

    [Fact]
    public void Defaults_are_valid()
        => Assert.True(Validate(new SalesforceLimitsOptions()).Succeeded);

    [Fact]
    public void More_concurrent_requests_than_the_org_allows_fails_startup()
    {
        var result = Validate(new SalesforceLimitsOptions { OrgType = SalesforceOrgType.DeveloperEdition },
            new SyncOptions { MaxConcurrentRequests = 6 });

        Assert.True(result.Failed);
        Assert.Contains("MaxConcurrentRequests (6) exceeds the org's limit of 5", result.FailureMessage);
    }

    [Fact]
    public void Request_timeout_above_the_10_minute_API_timeout_fails_startup()
    {
        var result = Validate(new SalesforceLimitsOptions { RequestTimeout = TimeSpan.FromMinutes(11) });

        Assert.True(result.Failed);
        Assert.Contains("API timeout", result.FailureMessage);
    }

    [Fact]
    public void Unknown_license_type_fails_startup()
    {
        var result = Validate(new SalesforceLimitsOptions { Licenses = { ["Salesforce Platinum"] = 1 } });

        Assert.True(result.Failed);
        Assert.Contains("Unknown Salesforce license type 'Salesforce Platinum'", result.FailureMessage);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1.5)]
    public void Daily_budget_share_must_be_a_fraction(double share)
        => Assert.True(Validate(new SalesforceLimitsOptions(), rate: new RateLimitOptions { DailyBudgetShare = share }).Failed);

    // ---------- REQUEST_LIMIT_EXCEEDED ----------

    [Fact]
    public void Concurrent_long_running_limit_error_is_recognised()
        => Assert.Equal(SalesforceRateLimitHandler.LimitExceeded.ConcurrentLongRunning, SalesforceRateLimitHandler.Classify(
            """[{"message":"ConcurrentPerOrgLongTxn Limit exceeded.","errorCode":"REQUEST_LIMIT_EXCEEDED"}]"""));

    [Fact]
    public void Total_requests_limit_error_is_recognised()
        => Assert.Equal(SalesforceRateLimitHandler.LimitExceeded.TotalRequests, SalesforceRateLimitHandler.Classify(
            """[{"message":"TotalRequests Limit exceeded.","errorCode":"REQUEST_LIMIT_EXCEEDED"}]"""));

    [Fact]
    public void Unknown_limit_error_is_treated_as_the_daily_limit()
        => Assert.Equal(SalesforceRateLimitHandler.LimitExceeded.TotalRequests, SalesforceRateLimitHandler.Classify(
            """[{"message":"Something new","errorCode":"REQUEST_LIMIT_EXCEEDED"}]"""));

    [Fact]
    public void Other_403s_are_not_limit_errors()
        => Assert.Null(SalesforceRateLimitHandler.Classify("""[{"errorCode":"INSUFFICIENT_ACCESS"}]"""));

    // ---------- Throttle decisions ----------

    [Fact]
    public void Daily_budget_is_the_share_of_the_org_allocation()
        => Assert.Equal(57_500, Limiter(new RateLimitOptions { DailyBudgetShare = 0.5 },
            new SalesforceLimitsOptions { Licenses = { ["Salesforce"] = 15 } }).DailyBudget);

    [Fact]
    public void Own_usage_of_the_budget_slows_down_even_when_the_org_has_room()
    {
        var limiter = Limiter(new RateLimitOptions());
        var orgAt10Percent = new SalesforceLimitInfo(new LimitCounter(10_000, 100_000), null, null, null);

        Assert.Equal(1.0, limiter.Decide(orgAt10Percent, ownDailyRatio: 0.5).Speed);
        Assert.Equal(0.25, limiter.Decide(orgAt10Percent, ownDailyRatio: 0.85).Speed);
        Assert.Equal(TimeSpan.FromMinutes(15), limiter.Decide(orgAt10Percent, ownDailyRatio: 0.96).Pause);
    }

    [Fact]
    public void Org_usage_slows_down_even_when_own_budget_has_room()
    {
        var limiter = Limiter(new RateLimitOptions());
        var orgAt85Percent = new SalesforceLimitInfo(new LimitCounter(85_000, 100_000), null, null, null);

        Assert.Equal(0.25, limiter.Decide(orgAt85Percent, ownDailyRatio: 0.1).Speed);
    }

    private static ValidateOptionsResult Validate(SalesforceLimitsOptions limits, SyncOptions? sync = null,
        RateLimitOptions? rate = null)
        => new SalesforceLimitsValidator(Options.Create(sync ?? new SyncOptions()), Options.Create(rate ?? new RateLimitOptions()))
            .Validate(null, limits);

    private static SalesforceRateLimiter Limiter(RateLimitOptions rate, SalesforceLimitsOptions? limits = null)
        => new(NpgsqlDataSource.Create("Host=unused"), Options.Create(rate),
            Options.Create(limits ?? new SalesforceLimitsOptions()), NullLogger<SalesforceRateLimiter>.Instance);
}
