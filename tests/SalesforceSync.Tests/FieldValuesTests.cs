using System.Text.Json;
using SalesforceSync.Infrastructure.Salesforce;

namespace SalesforceSync.Tests;

public sealed class FieldValuesTests
{
    private static JsonElement J(string json)
    {
        return JsonDocument.Parse(json).RootElement.Clone();
    }

    [Theory, AutoData]
    public void Any_number_equals_the_same_stored_number(int number)
    {
        FieldValues.AreEqual(FieldValues.Normalize(number), J(number.ToString())).Should().BeTrue();
        FieldValues.AreEqual(FieldValues.Normalize((double)number), J(number.ToString())).Should().BeTrue();
    }

    [Fact]
    public void Numbers_are_compared_as_decimals()
    {
        FieldValues.Normalize(10.01).Should().Be(10.01m);
        FieldValues.AreEqual(FieldValues.Normalize(10.01m), J("10.01")).Should().BeTrue();
        FieldValues.AreEqual(FieldValues.Normalize(10.005m), J("10.01")).Should().BeFalse("the scale of the field is not known: it is written again");
    }

    [Fact]
    public void A_number_sent_as_text_equals_the_stored_number()
    {
        FieldValues.AreEqual(FieldValues.Normalize("7801234"), J("7801234.0")).Should().BeTrue("Salesforce stores \"7801234\" as 7801234.0");
        FieldValues.AreEqual(FieldValues.Normalize("7801235"), J("7801234.0")).Should().BeFalse();
        FieldValues.AreEqual(FieldValues.Normalize("78O1234"), J("7801234.0")).Should().BeFalse("not a number");
        FieldValues.AreEqual(FieldValues.Normalize("7801234"), J("\"7801234\"")).Should().BeTrue("a Text field");
    }

    [Fact]
    public void A_value_passed_through_as_JSON_is_compared_by_its_kind_not_its_text()
    {
        FieldValues.AreEqual(FieldValues.Normalize(J("7801234")), J("7801234.0")).Should().BeTrue();
        FieldValues.AreEqual(FieldValues.Normalize(J("true")), J("true")).Should().BeTrue();
        FieldValues.AreEqual(FieldValues.Normalize(J("\"Wellington\"")), J("\"Wellington\"")).Should().BeTrue();
        FieldValues.Normalize(J("null")).Should().BeNull("clears the field");
        FieldValues.Normalize(J("\"\"")).Should().BeNull("empty text, as Salesforce stores it");
        FieldValues.ToWire(FieldValues.Normalize(J("7801234"))).Should().Be(7801234m, "sent as a number");
    }

    [Fact]
    public void Checkbox_null_is_false()
    {
        FieldValues.AreEqual(null, J("false")).Should().BeTrue();
        FieldValues.AreEqual(false, J("false")).Should().BeTrue();
        FieldValues.AreEqual(true, J("false")).Should().BeFalse();
    }

    [Fact]
    public void Empty_text_is_null()
    {
        FieldValues.Normalize("").Should().BeNull();
        FieldValues.AreEqual(FieldValues.Normalize(""), J("null")).Should().BeTrue();
        FieldValues.AreEqual("a", J("null")).Should().BeFalse();
    }

    [Fact]
    public void Text_is_compared_as_it_is()
    {
        FieldValues.AreEqual("Acme", J("\"Acme\"")).Should().BeTrue();
        FieldValues.AreEqual("Acme", J("\"acme\"")).Should().BeFalse();
    }

    [Fact]
    public void Date_time_compares_as_an_instant_to_the_second()
    {
        // Salesforce keeps date/time fields to the second, so an event's milliseconds must not count as a change
        var stamp = FieldValues.Normalize(new DateTimeOffset(2026, 10, 13, 3, 2, 3, 456, TimeSpan.FromHours(2)));
        FieldValues.AreEqual(stamp, J("\"2026-10-13T01:02:03.000+0000\"")).Should().BeTrue();
        FieldValues.ToWire(stamp).Should().Be("2026-10-13T01:02:03.000Z");
    }

    [Fact]
    public void Date_time_of_unspecified_kind_is_taken_as_UTC()
    {
        FieldValues.Normalize(new DateTime(2026, 10, 13, 1, 2, 3, DateTimeKind.Unspecified))
            .Should().Be(new DateTimeOffset(2026, 10, 13, 1, 2, 3, TimeSpan.Zero));
    }

    [Fact]
    public void Time_keeps_milliseconds()
    {
        FieldValues.ToWire(FieldValues.Normalize(new TimeOnly(13, 14, 15, 678))).Should().Be("13:14:15.678Z");
        FieldValues.AreEqual(FieldValues.Normalize(new TimeOnly(13, 14, 15, 678)), J("\"13:14:15.678Z\"")).Should().BeTrue();
    }

    [Fact]
    public void Date_is_sent_as_yyyy_MM_dd()
    {
        FieldValues.ToWire(FieldValues.Normalize(new DateOnly(2026, 10, 13))).Should().Be("2026-10-13");
        FieldValues.AreEqual(FieldValues.Normalize(new DateOnly(2026, 10, 13)), J("\"2026-10-13\"")).Should().BeTrue();
    }
}
