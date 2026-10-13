using System.Text.Json;
using SalesforceSync.Salesforce;
using SalesforceSync.Sync;

namespace SalesforceSync.Tests;

public sealed class FieldValuesTests
{
    private static SalesforceField F(string type, int length = 0, int precision = 0, int scale = 0, bool nillable = true)
        => new("F__c", type, length, precision, scale, true, true, nillable);

    private static JsonElement J(string json) => JsonDocument.Parse(json).RootElement.Clone();

    private static object? N(SalesforceField field, object? value)
    {
        FieldValues.TryNormalize(field, value, out var normalized, out var error)
            .Should().BeTrue("{0} should be a valid {1} value, but got {2}", value, field.Type, error);
        return normalized;
    }

    [Fact]
    public void Number_is_rounded_to_the_field_scale()
    {
        var currency = F("currency", precision: 18, scale: 2);
        N(currency, "10.005").Should().Be(10.01m);
        FieldValues.AreEqual(currency, N(currency, 10.005m), J("10.01")).Should().BeTrue();
    }

    [Theory, AutoData]
    public void Numeric_string_equals_the_stored_number(int number)
    {
        var field = F("double", precision: 18, scale: 2);

        FieldValues.AreEqual(field, N(field, number.ToString()), J(number.ToString())).Should().BeTrue();
    }

    [Fact]
    public void Number_too_large_for_the_field_is_rejected()
    {
        FieldValues.TryNormalize(F("currency", precision: 5, scale: 2), 1000m, out _, out var error).Should().BeFalse();
        error.Code.Should().Be("NUMBER_OUTSIDE_VALID_RANGE");
    }

    [Fact]
    public void Checkbox_null_is_false()
    {
        var checkbox = F("boolean", nillable: false);
        N(checkbox, null).Should().Be(false);
        FieldValues.AreEqual(checkbox, false, J("false")).Should().BeTrue();
    }

    [Fact]
    public void Empty_text_is_null()
        => FieldValues.AreEqual(F("string", 80), N(F("string", 80), ""), J("null")).Should().BeTrue();

    [Theory, AutoData]
    public void Text_longer_than_the_field_is_rejected(string text)
    {
        FieldValues.TryNormalize(F("string", text.Length - 1), text, out _, out var error).Should().BeFalse();
        error.Code.Should().Be("STRING_TOO_LONG");
    }

    [Fact]
    public void Required_field_can_not_be_emptied()
    {
        FieldValues.TryNormalize(F("string", 80, nillable: false), null, out _, out var error).Should().BeFalse();
        error.Code.Should().Be("REQUIRED_FIELD_MISSING");
    }

    [Fact]
    public void Multi_select_compares_as_a_set()
        => FieldValues.AreEqual(F("multipicklist", 4099), N(F("multipicklist", 4099), "B; A;A"), J("\"A;B\"")).Should().BeTrue();

    [Fact]
    public void Fifteen_character_id_is_compared_in_its_18_character_form()
    {
        // The example from the Salesforce documentation
        FieldValues.To18CharId("001D000000IqhSL").Should().Be("001D000000IqhSLIAZ");
        var lookup = F("reference", 18);
        FieldValues.AreEqual(lookup, N(lookup, "001D000000IqhSL"), J("\"001D000000IqhSLIAZ\"")).Should().BeTrue();
    }

    [Fact]
    public void Date_time_compares_as_an_instant_to_the_millisecond()
    {
        var stamp = F("datetime");
        FieldValues.AreEqual(stamp, N(stamp, "2026-10-13T03:02:03.4567+02:00"), J("\"2026-10-13T01:02:03.456+0000\"")).Should().BeTrue();
        FieldValues.ToWire(N(stamp, "2026-10-13T03:02:03.4567+02:00")).Should().Be("2026-10-13T01:02:03.456Z");
    }

    [Fact]
    public void Date_is_sent_as_yyyy_MM_dd()
        => FieldValues.ToWire(N(F("date"), "2026-10-13")).Should().Be("2026-10-13");

    [Theory, AutoData]
    public void Wrong_type_is_rejected(string notABoolean)
    {
        FieldValues.TryNormalize(F("boolean"), notABoolean, out _, out var error).Should().BeFalse();
        error.Code.Should().Be("INVALID_TYPE_ON_FIELD_IN_RECORD");
    }

    [Theory]
    [InlineData(new[] { "PROCESSING_HALTED" }, SyncOutcome.NotAttempted)]
    [InlineData(new[] { "ALL_OR_NONE_OPERATION_ROLLED_BACK" }, SyncOutcome.NotAttempted)]
    [InlineData(new[] { "UNABLE_TO_LOCK_ROW", "PROCESSING_HALTED" }, SyncOutcome.TransientFailure)]
    [InlineData(new[] { "REQUIRED_FIELD_MISSING", "PROCESSING_HALTED" }, SyncOutcome.PermanentFailure)]
    [InlineData(new[] { "SOMETHING_NEW" }, SyncOutcome.TransientFailure)]
    public void Graph_errors_are_classified(string[] codes, SyncOutcome expected)
    {
        var errors = new SalesforceErrors(Microsoft.Extensions.Options.Options.Create(new SalesforceOptions()));
        errors.Classify(codes.Select(c => new SalesforceError("acct", c, null)).ToList()).Should().Be(expected);
    }

    [Fact]
    public void Permanent_codes_are_configurable()
    {
        var errors = new SalesforceErrors(Microsoft.Extensions.Options.Options.Create(
            new SalesforceOptions { PermanentErrorCodes = ["UNABLE_TO_LOCK_ROW"] }));
        errors.Classify([new SalesforceError(null, "UNABLE_TO_LOCK_ROW", null)]).Should().Be(SyncOutcome.PermanentFailure);
        errors.Classify([new SalesforceError(null, "REQUIRED_FIELD_MISSING", null)]).Should().Be(SyncOutcome.TransientFailure,
            "a code is permanent only when listed, and setting the list replaces the defaults");
    }
}
