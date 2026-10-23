using System.Globalization;
using System.Text.Json;

namespace SalesforceSync.Infrastructure.Salesforce;

/// <summary>
/// Field values handled by .NET type; no org metadata is read. A desired value is first put in canonical form (numbers
/// as decimal, date/time to the second, empty text as null, ...); the diff compares that with what Salesforce returned,
/// and the canonical form is what gets sent. What the org stores differently from what is sent (a number rounded to
/// the field's scale, text cut by a field's length) can't be known here: such a field looks changed until the values
/// agree, which costs a write, never a wrong one. A value Salesforce refuses is that record's own error.
/// </summary>
public static class FieldValues
{
    /// <summary>The canonical form of <paramref name="value"/>: what <see cref="AreEqual"/> compares and <see cref="ToWire"/> sends.</summary>
    public static object? Normalize(object? value)
    {
        return value switch
        {
            null or string { Length: 0 } => null, // Salesforce stores "" as null
            string or bool or decimal or DateOnly => value,
            int or long or short or byte or double or float => Convert.ToDecimal(value, CultureInfo.InvariantCulture),
            // Salesforce keeps date/time values to the second ("full timestamps with a precision of one second",
            // Object Reference, Primitive Data Types); time values keep milliseconds
            DateTimeOffset t => Truncate(t.ToUniversalTime(), TimeSpan.TicksPerSecond),
            DateTime t => Truncate(new DateTimeOffset(t.Kind == DateTimeKind.Unspecified ? DateTime.SpecifyKind(t, DateTimeKind.Utc) : t.ToUniversalTime()),
                TimeSpan.TicksPerSecond),
            TimeOnly t => new TimeOnly(t.Ticks - (t.Ticks % TimeSpan.TicksPerMillisecond)),
            Enum or Guid => value.ToString(),
            // A value passed through from the event's JSON: compared by its JSON kind, not as raw text ("7801234" is
            // Salesforce's 7801234.0)
            JsonElement e => e.ValueKind switch
            {
                JsonValueKind.Null or JsonValueKind.Undefined => null,
                JsonValueKind.String => Normalize(e.GetString()),
                JsonValueKind.Number => e.GetDecimal(),
                JsonValueKind.True => true,
                JsonValueKind.False => false,
                _ => value
            },
            _ => value
        };
    }

    /// <summary>Whether Salesforce's current value equals a value produced by <see cref="Normalize"/>.</summary>
    public static bool AreEqual(object? normalized, JsonElement current)
    {
        if (current.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined)
        {
            return normalized is null;
        }

        if (normalized is null)
        {
            return current.ValueKind == JsonValueKind.False; // a checkbox is never null: Salesforce stores false
        }

        var currentText = current.ValueKind == JsonValueKind.String ? current.GetString() : null;
        return normalized switch
        {
            bool b => current.ValueKind is JsonValueKind.True or JsonValueKind.False && current.GetBoolean() == b,
            decimal d => current.ValueKind == JsonValueKind.Number && current.GetDecimal() == d,
            DateOnly d => currentText is not null && TryDate(currentText, out var cd) && cd == d,
            DateTimeOffset t => currentText is not null && TryDateTime(currentText, out var ct) && ct == t,
            TimeOnly t => currentText is not null && TryTime(currentText, out var ct) && ct == t,
            // Text, or a number sent as text: Salesforce takes "7801234" for a Number field and stores 7801234
            string s => currentText == s || (current.ValueKind == JsonValueKind.Number && IsNumber(s, out var n) && current.GetDecimal() == n),
            _ => JsonSerializer.Serialize(normalized) == current.GetRawText()
        };
    }

    /// <summary>The JSON value to send for a normalized value.</summary>
    public static object? ToWire(object? normalized)
    {
        return normalized switch
        {
            DateOnly d => d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
            DateTimeOffset t => t.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'", CultureInfo.InvariantCulture),
            TimeOnly t => t.ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture) + "Z",
            _ => normalized
        };
    }

    private static bool IsNumber(string text, out decimal number)
    {
        return decimal.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out number);
    }

    private static DateTimeOffset Truncate(DateTimeOffset utc, long ticks)
    {
        return new DateTimeOffset(utc.UtcTicks - (utc.UtcTicks % ticks), TimeSpan.Zero);
    }

    private static bool TryDate(string text, out DateOnly date)
    {
        return DateOnly.TryParseExact(text, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out date);
    }

    private static bool TryDateTime(string text, out DateTimeOffset instant)
    {
        // Salesforce returns "+0000"; .NET wants "+00:00"
        if (text.Length > 5 && text[^5] is '+' or '-' && char.IsAsciiDigit(text[^1]))
        {
            text = text.Insert(text.Length - 2, ":");
        }

        if (!DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var parsed))
        {
            instant = default;
            return false;
        }

        instant = Truncate(parsed.ToUniversalTime(), TimeSpan.TicksPerSecond);
        return true;
    }

    private static bool TryTime(string text, out TimeOnly time)
    {
        if (!TimeOnly.TryParse(text.TrimEnd('Z', 'z'), CultureInfo.InvariantCulture, DateTimeStyles.None, out time))
        {
            return false;
        }

        time = new TimeOnly(time.Ticks - (time.Ticks % TimeSpan.TicksPerMillisecond));
        return true;
    }
}
