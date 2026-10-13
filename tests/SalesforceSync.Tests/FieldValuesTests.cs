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
        Assert.True(FieldValues.TryNormalize(field, value, out var normalized, out var error), error.ToString());
        return normalized;
    }

    [Fact]
    public void Number_is_rounded_to_the_field_scale()
    {
        var currency = F("currency", precision: 18, scale: 2);
        Assert.Equal(10.01m, N(currency, "10.005"));
        Assert.True(FieldValues.AreEqual(currency, N(currency, 10.005m), J("10.01")));
    }

    [Fact]
    public void Numeric_string_equals_the_stored_number()
        => Assert.True(FieldValues.AreEqual(F("double", precision: 18, scale: 2), N(F("double", precision: 18, scale: 2), "42"), J("42")));

    [Fact]
    public void Number_too_large_for_the_field_is_rejected()
    {
        Assert.False(FieldValues.TryNormalize(F("currency", precision: 5, scale: 2), 1000m, out _, out var error));
        Assert.Equal("NUMBER_OUTSIDE_VALID_RANGE", error.Code);
    }

    [Fact]
    public void Checkbox_null_is_false()
    {
        var checkbox = F("boolean", nillable: false);
        Assert.Equal(false, N(checkbox, null));
        Assert.True(FieldValues.AreEqual(checkbox, false, J("false")));
    }

    [Fact]
    public void Empty_text_is_null()
        => Assert.True(FieldValues.AreEqual(F("string", 80), N(F("string", 80), ""), J("null")));

    [Fact]
    public void Text_longer_than_the_field_is_rejected()
    {
        Assert.False(FieldValues.TryNormalize(F("string", 5), "toolong", out _, out var error));
        Assert.Equal("STRING_TOO_LONG", error.Code);
    }

    [Fact]
    public void Required_field_can_not_be_emptied()
    {
        Assert.False(FieldValues.TryNormalize(F("string", 80, nillable: false), null, out _, out var error));
        Assert.Equal("REQUIRED_FIELD_MISSING", error.Code);
    }

    [Fact]
    public void Multi_select_compares_as_a_set()
        => Assert.True(FieldValues.AreEqual(F("multipicklist", 4099), N(F("multipicklist", 4099), "B; A;A"), J("\"A;B\"")));

    [Fact]
    public void Fifteen_character_id_is_compared_in_its_18_character_form()
    {
        // The example from the Salesforce documentation
        Assert.Equal("001D000000IqhSLIAZ", FieldValues.To18CharId("001D000000IqhSL"));
        var lookup = F("reference", 18);
        Assert.True(FieldValues.AreEqual(lookup, N(lookup, "001D000000IqhSL"), J("\"001D000000IqhSLIAZ\"")));
    }

    [Fact]
    public void Date_time_compares_as_an_instant_to_the_millisecond()
    {
        var stamp = F("datetime");
        Assert.True(FieldValues.AreEqual(stamp, N(stamp, "2026-10-13T03:02:03.4567+02:00"), J("\"2026-10-13T01:02:03.456+0000\"")));
        Assert.Equal("2026-10-13T01:02:03.456Z", FieldValues.ToWire(N(stamp, "2026-10-13T03:02:03.4567+02:00")));
    }

    [Fact]
    public void Date_is_sent_as_yyyy_MM_dd()
        => Assert.Equal("2026-10-13", FieldValues.ToWire(N(F("date"), "2026-10-13")));

    [Fact]
    public void Wrong_type_is_rejected()
    {
        Assert.False(FieldValues.TryNormalize(F("boolean"), "maybe", out _, out var error));
        Assert.Equal("INVALID_TYPE_ON_FIELD_IN_RECORD", error.Code);
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
        Assert.Equal(expected, errors.Classify(codes.Select(c => new SalesforceError("acct", c, null)).ToList()));
    }

    [Fact]
    public void Permanent_codes_are_configurable()
    {
        var errors = new SalesforceErrors(Microsoft.Extensions.Options.Options.Create(
            new SalesforceOptions { PermanentErrorCodes = ["UNABLE_TO_LOCK_ROW"] }));
        Assert.Equal(SyncOutcome.PermanentFailure, errors.Classify([new SalesforceError(null, "UNABLE_TO_LOCK_ROW", null)]));
        Assert.Equal(SyncOutcome.TransientFailure, errors.Classify([new SalesforceError(null, "REQUIRED_FIELD_MISSING", null)]));
    }
}
