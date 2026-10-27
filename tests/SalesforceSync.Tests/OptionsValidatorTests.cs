using Microsoft.Extensions.Options;
using SalesforceSync.Data;
using SalesforceSync.Data.Inbox;
using SalesforceSync.Hosting;
using SalesforceSync.Infrastructure.RateLimiting;
using SalesforceSync.Infrastructure.Salesforce;

namespace SalesforceSync.Tests;

/// <summary>
/// The settings checked at startup (FluentValidation validators run as options checks, <see cref="FluentOptionsValidator{TOptions}"/>):
/// the defaults pass, and a wrong value fails with a message naming the setting.
/// </summary>
public sealed class OptionsValidatorTests
{
    private static readonly SalesforceOptions ValidSalesforce = new()
    {
        InstanceUrl = "https://acme.my.salesforce.com",
        TokenUrl = "https://acme.my.salesforce.com/services/oauth2/token",
        BaseApiPath = "/services/data",
        ApiVersion = "v62.0"
    };

    [Fact]
    public void The_defaults_pass()
    {
        Validate(new RateLimitOptionsValidator(), new RateLimitOptions()).Succeeded.Should().BeTrue();
        Validate(new PostgresRetryOptionsValidator(), new PostgresRetryOptions()).Succeeded.Should().BeTrue();
        Validate(new SalesforceOptionsValidator(), ValidSalesforce).Succeeded.Should().BeTrue();
    }

    [Fact]
    public void The_storage_full_hold_must_be_positive()
    {
        var sync = new FluentOptionsValidator<SyncOptions>(new SyncOptionsValidator());

        sync.Validate(null, new SyncOptions()).Succeeded.Should().BeTrue();
        sync.Validate(null, new SyncOptions { StorageFullHold = TimeSpan.Zero }).FailureMessage.Should().Contain("Sync:StorageFullHold");
    }

    [Fact]
    public void Every_wrong_Salesforce_setting_is_listed_at_once()
    {
        var result = Validate(new SalesforceOptionsValidator(), new SalesforceOptions { InstanceUrl = "acme", BaseApiPath = "services/data", ApiVersion = "62" });

        result.Failures.Should().HaveCount(4).And.Contain(f => f.Contains("Salesforce:InstanceUrl"))
            .And.Contain(f => f.Contains("Salesforce:TokenUrl")).And.Contain(f => f.Contains("Salesforce:BaseApiPath"))
            .And.Contain(f => f.Contains("Salesforce:ApiVersion"));
    }

    [Theory]
    [InlineData(0.0, "DailyHardRatio")]
    [InlineData(-1.0, "DailyHardRatio")]
    public void A_daily_ratio_of_zero_or_less_is_refused(double ratio, string setting)
    {
        Validate(new RateLimitOptionsValidator(), new RateLimitOptions { DailyHardRatio = ratio }).FailureMessage.Should().Contain($"RateLimit:{setting}");
    }

    [Fact]
    public void Pauses_must_be_positive()
    {
        var result = Validate(new RateLimitOptionsValidator(), new RateLimitOptions { ConcurrentLimitPause = TimeSpan.Zero, DailyHardPause = TimeSpan.Zero });

        result.Failures.Should().HaveCount(2);
    }

    [Fact]
    public void Postgres_retry_delays_must_be_in_order()
    {
        Validate(new PostgresRetryOptionsValidator(), new PostgresRetryOptions { BaseDelay = TimeSpan.FromSeconds(10), MaxRetryDelay = TimeSpan.FromSeconds(5) })
            .FailureMessage.Should().Contain("Postgres:Retry:MaxRetryDelay");
    }

    private static ValidateOptionsResult Validate<TOptions>(FluentValidation.IValidator<TOptions> validator, TOptions options)
        where TOptions : class
    {
        return new FluentOptionsValidator<TOptions>(validator).Validate(null, options);
    }
}
