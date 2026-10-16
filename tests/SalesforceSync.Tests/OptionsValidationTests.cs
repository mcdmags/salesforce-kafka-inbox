using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using SalesforceSync.Inbox;
using SalesforceSync.Salesforce;

namespace SalesforceSync.Tests;

/// <summary>Startup validation of the Salesforce client and sync options, and of every row type's limits.</summary>
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

    // ---------- Row type limits ----------

    [Fact]
    public void Every_registered_row_type_queries_and_writes_within_Salesforces_limits()
    {
        // Registering a sync validates its row type's limits
        var register = () => new ServiceCollection().AddInboxSync();

        register.Should().NotThrow();
    }

    [Fact]
    public void A_row_type_may_query_and_write_up_to_Salesforces_limits()
    {
        var validate = SyncServiceCollectionExtensions.ValidateLimits<AtTheLimitsRow>;

        validate.Should().NotThrow();
    }

    [Fact]
    public void A_row_type_querying_more_values_than_a_result_page_fails_startup()
    {
        var validate = SyncServiceCollectionExtensions.ValidateLimits<TooManyValuesRow>;

        validate.Should().Throw<InvalidOperationException>().WithMessage("TooManyValuesRow.MaxValuesPerQuery (2001) must be 1 to 2000*");
    }

    [Fact]
    public void A_row_type_writing_no_records_per_upsert_fails_startup()
    {
        var validate = SyncServiceCollectionExtensions.ValidateLimits<NoRecordsRow>;

        validate.Should().Throw<InvalidOperationException>().WithMessage("NoRecordsRow.MaxRecordsPerWrite (0) must be 1 to 200*");
    }

    private sealed class AtTheLimitsRow : IInboxRow
    {
        public static int MaxValuesPerQuery => 2_000;
        public static int MaxRecordsPerWrite => 200;
    }

    private sealed class TooManyValuesRow : IInboxRow
    {
        public static int MaxValuesPerQuery => 2_001;
        public static int MaxRecordsPerWrite => 200;
    }

    private sealed class NoRecordsRow : IInboxRow
    {
        public static int MaxValuesPerQuery => 100;
        public static int MaxRecordsPerWrite => 0;
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