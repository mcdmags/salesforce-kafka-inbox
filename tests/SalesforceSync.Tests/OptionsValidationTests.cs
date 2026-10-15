using Microsoft.Extensions.Options;
using SalesforceSync.Inbox;
using SalesforceSync.Salesforce;

namespace SalesforceSync.Tests;

/// <summary>Startup validation of the Salesforce client and sync options.</summary>
public sealed class OptionsValidationTests
{
    [Fact]
    public void Defaults_with_an_instance_url_are_valid()
    {
        Validate(Salesforce()).Succeeded.Should().BeTrue();
    }

    [Fact]
    public void A_missing_instance_url_fails_startup()
    {
        Validate(new SalesforceOptions()).FailureMessage.Should().Contain("Salesforce:InstanceUrl");
    }

    [Theory]
    [InlineData(0)]
    [InlineData(11)]
    public void Request_timeout_must_be_positive_and_within_the_10_minute_API_timeout(int minutes)
    {
        var options = Salesforce();
        options.RequestTimeout = TimeSpan.FromMinutes(minutes);

        var result = Validate(options, new SyncOptions { Lease = TimeSpan.FromMinutes(20) });

        result.Failed.Should().BeTrue();
        result.FailureMessage.Should().Contain("API timeout");
    }

    [Fact]
    public void Lease_must_outlast_the_request_timeout()
    {
        // A call that is merely slow must end (or be given up on) while its entities are still leased
        var options = Salesforce();
        options.RequestTimeout = TimeSpan.FromSeconds(90);

        Validate(options, new SyncOptions { Lease = TimeSpan.FromSeconds(90) }).FailureMessage.Should().Contain("Sync:Lease");
        Validate(options, new SyncOptions { Lease = TimeSpan.FromSeconds(91) }).Succeeded.Should().BeTrue();
    }

    [Fact]
    public void Unknown_outcome_hold_defaults_to_the_Salesforce_api_timeout_and_must_be_positive()
    {
        new SyncOptions().UnknownOutcomeHold.Should().Be(SalesforceOptions.MaxApiTimeout);
        new SyncOptionsValidator().Validate(null, new SyncOptions { UnknownOutcomeHold = TimeSpan.Zero })
            .FailureMessage.Should().Contain("UnknownOutcomeHold");
        new SyncOptionsValidator().Validate(null, new SyncOptions { MaxEventsPerEntity = 0 })
            .FailureMessage.Should().Contain("MaxEventsPerEntity");
        new SyncOptionsValidator().Validate(null, new SyncOptions()).Succeeded.Should().BeTrue();
    }

    [Fact]
    public void Coalesce_delay_must_be_at_least_zero_and_less_than_the_lease()
    {
        var validator = new SyncOptionsValidator();
        validator.Validate(null, new SyncOptions { CoalesceDelay = TimeSpan.FromMilliseconds(-1) })
            .FailureMessage.Should().Contain("CoalesceDelay");
        validator.Validate(null, new SyncOptions { CoalesceDelay = new SyncOptions().Lease })
            .FailureMessage.Should().Contain("CoalesceDelay");
        validator.Validate(null, new SyncOptions { CoalesceDelay = TimeSpan.Zero }).Succeeded.Should().BeTrue();
    }

    private static SalesforceOptions Salesforce()
    {
        return new() { InstanceUrl = "https://example.my.salesforce.com" };
    }

    private static ValidateOptionsResult Validate(SalesforceOptions options, SyncOptions? sync = null)
    {
        return new SalesforceOptionsValidator(Options.Create(sync ?? new SyncOptions())).Validate(null, options);
    }
}