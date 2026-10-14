using System.Globalization;
using System.Text.Json;

namespace SalesforceSync.Salesforce;

/// <summary>
/// Type-aware handling of field values, driven by sObject Describe. A desired value is first normalized to exactly
/// what Salesforce stores for that field type (number rounded to the field's scale, checkbox null -> false,
/// 15-character id -> 18, date/time to the second, ...). Types and limits: Salesforce Object Reference, "Primitive
/// Data Types" and "Field Types". The diff compares normalized values, and the normalized value is what gets sent, so
/// a value Salesforce would store differently can't make the same field look changed on every event.
/// </summary>
public static class FieldValues
{
    private static readonly HashSet<string> TextTypes = new(StringComparer.OrdinalIgnoreCase)
    {
        "string", "textarea", "email", "phone", "url", "picklist", "combobox", "encryptedstring"
    };

    private static readonly HashSet<string> NumberTypes = new(StringComparer.OrdinalIgnoreCase)
    {
        "int", "long", "double", "currency", "percent"
    };

    /// <summary>Normalizes <paramref name="value"/> to what Salesforce stores in <paramref name="field"/>.</summary>
    /// <returns>False with <paramref name="error"/> when Salesforce would reject the value.</returns>
    public static bool TryNormalize(SalesforceField field, object? value, out object? normalized, out FieldError error)
    {
        normalized = null;
        error = default;
        var type = field.Type;
        var isBoolean = type.Equals("boolean", StringComparison.OrdinalIgnoreCase);

        if (value is null or string { Length: 0 })
        {
            if (isBoolean)
            {
                normalized = false; // a checkbox is never null
            }
            else if (!field.Nillable)
            {
                return Fail(field, "REQUIRED_FIELD_MISSING", "Required field can't be empty", out error);
            }

            return true; // Salesforce stores "" as null
        }

        if (TextTypes.Contains(type))
        {
            return TryText(field, AsString(value), out normalized, out error);
        }

        if (type.Equals("multipicklist", StringComparison.OrdinalIgnoreCase))
        {
            return TryText(field, string.Join(';', SplitMultiPicklist(AsString(value))), out normalized, out error);
        }

        if (type is "id" or "reference")
        {
            var id = To18CharId(AsString(value));
            if (id is null)
            {
                return Fail(field, "MALFORMED_ID", $"'{value}' is not a Salesforce id", out error);
            }

            normalized = id;
            return true;
        }

        if (isBoolean)
        {
            if (value is bool b)
            {
                normalized = b;
            }
            else if (value is string s && bool.TryParse(s, out var parsed))
            {
                normalized = parsed;
            }
            else
            {
                return WrongType(field, value, "a boolean", out error);
            }

            return true;
        }

        if (NumberTypes.Contains(type))
        {
            return TryNumber(field, value, out normalized, out error);
        }

        if (type.Equals("date", StringComparison.OrdinalIgnoreCase))
        {
            if (!TryDate(value, out var date))
            {
                return WrongType(field, value, "a date", out error);
            }

            normalized = date;
            return true;
        }

        if (type.Equals("datetime", StringComparison.OrdinalIgnoreCase))
        {
            if (!TryDateTime(value, out var instant))
            {
                return WrongType(field, value, "a date/time", out error);
            }

            normalized = instant;
            return true;
        }

        if (type.Equals("time", StringComparison.OrdinalIgnoreCase))
        {
            if (!TryTime(value, out var time))
            {
                return WrongType(field, value, "a time", out error);
            }

            normalized = time;
            return true;
        }

        normalized = value; // other types (base64, anyType, ...): compared as JSON
        return true;
    }

    private static bool TryText(SalesforceField field, string text, out object? normalized, out FieldError error)
    {
        normalized = null;
        error = default;
        if (field.Length > 0 && text.Length > field.Length)
        {
            return Fail(field, "STRING_TOO_LONG", $"{text.Length} characters, max {field.Length}", out error);
        }

        normalized = text;
        return true;
    }

    /// <summary>
    /// Rounded to the field's scale (integers: 0), and within its size: precision - scale digits left of the point for
    /// numbers with decimals, and digits for int and long, which Describe gives no precision.
    /// </summary>
    private static bool TryNumber(SalesforceField field, object value, out object? normalized, out FieldError error)
    {
        normalized = null;
        error = default;
        if (!TryDecimal(value, out var number))
        {
            return WrongType(field, value, "a number", out error);
        }

        var isInteger = field.Type is "int" or "long";
        var scale = isInteger ? 0 : field.Scale;
        number = Math.Round(number, scale, MidpointRounding.AwayFromZero);
        var integerDigits = isInteger ? field.Digits : field.Precision - scale;
        if ((isInteger ? field.Digits : field.Precision) > 0 && Math.Abs(number) >= Pow10(integerDigits))
        {
            return Fail(field, "NUMBER_OUTSIDE_VALID_RANGE",
                $"{number} doesn't fit {integerDigits} digits left of the decimal point", out error);
        }
        normalized = number;
        return true;
    }

    private static bool WrongType(SalesforceField field, object value, string expected, out FieldError error)
    {
        return Fail(field, "INVALID_TYPE_ON_FIELD_IN_RECORD", $"'{value}' is not {expected}", out error);
    }

    /// <summary>Whether Salesforce's current value equals a value produced by <see cref="TryNormalize"/>.</summary>
    public static bool AreEqual(SalesforceField field, object? normalized, JsonElement current)
    {
        if (current.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined)
        {
            return normalized is null;
        }

        if (normalized is null)
        {
            return false;
        }

        var currentText = current.ValueKind == JsonValueKind.String ? current.GetString() : null;
        return normalized switch
        {
            bool b => current.ValueKind is JsonValueKind.True or JsonValueKind.False && current.GetBoolean() == b,
            decimal d => current.ValueKind == JsonValueKind.Number && current.GetDecimal() == d,
            DateOnly d => currentText is not null && TryDate(currentText, out var cd) && cd == d,
            DateTimeOffset t => currentText is not null && TryDateTime(currentText, out var ct) && ct == t,
            TimeOnly t => currentText is not null && TryTime(currentText, out var ct) && ct == t,
            string s when field.Type.Equals("multipicklist", StringComparison.OrdinalIgnoreCase) =>
                currentText is not null && SplitMultiPicklist(currentText).ToHashSet().SetEquals(SplitMultiPicklist(s)),
            string s when field.Type is "id" or "reference" =>
                currentText is not null && To18CharId(currentText) == s,
            string s => currentText == s,
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

    /// <summary>
    /// Salesforce ids are case-sensitive 15-character values; the API returns the 18-character form, whose last three
    /// characters encode the case of the first 15. Returns null if <paramref name="id"/> isn't a valid id, including an
    /// 18-character id whose case was changed: Salesforce rejects those too, because the suffix no longer matches.
    /// </summary>
    public static string? To18CharId(string id)
    {
        if (id.Length == 18)
        {
            var expected = To18CharId(id[..15]);
            return expected is not null && id.EndsWith(expected[15..], StringComparison.Ordinal) ? id : null;
        }
        if (id.Length != 15 || !id.All(char.IsAsciiLetterOrDigit))
        {
            return null;
        }

        const string alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ012345";
        Span<char> suffix = stackalloc char[3];
        for (var chunk = 0; chunk < 3; chunk++)
        {
            var bits = 0;
            for (var i = 0; i < 5; i++)
            {
                if (char.IsAsciiLetterUpper(id[(chunk * 5) + i]))
                {
                    bits |= 1 << i;
                }
            }

            suffix[chunk] = alphabet[bits];
        }
        return id + new string(suffix);
    }

    // ---------- Parsing ----------

    private static IEnumerable<string> SplitMultiPicklist(string value)
    {
        return value.Split(';', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries).Distinct(StringComparer.Ordinal);
    }

    private static string AsString(object value)
    {
        return value switch
        {
            string s => s,
            bool b => b ? "true" : "false",
            IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
            _ => value.ToString() ?? ""
        };
    }

    private static bool TryDecimal(object value, out decimal number)
    {
        switch (value)
        {
            case decimal d: number = d; return true;
            case int or long or short or byte or double or float:
                number = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
                return true;
            case string s:
                return decimal.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out number);
            default:
                number = 0;
                return false;
        }
    }

    private static decimal Pow10(int exponent)
    {
        var result = 1m;
        for (var i = 0; i < Math.Clamp(exponent, 0, 28); i++)
        {
            result *= 10;
        }

        return result;
    }

    private static bool TryDate(object value, out DateOnly date)
    {
        switch (value)
        {
            case DateOnly d: date = d; return true;
            case DateTime dt: date = DateOnly.FromDateTime(dt); return true;
            case string s when DateOnly.TryParseExact(s, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out date):
                return true;
            default:
                date = default;
                return false;
        }
    }

    private static bool TryDateTime(object value, out DateTimeOffset instant)
    {
        DateTimeOffset parsed;
        switch (value)
        {
            case DateTimeOffset dto: parsed = dto; break;
            case DateTime dt:
                parsed = new DateTimeOffset(dt.Kind == DateTimeKind.Unspecified
                ? DateTime.SpecifyKind(dt, DateTimeKind.Utc) : dt.ToUniversalTime()); break;
            case string s:
                // Salesforce returns "+0000"; .NET wants "+00:00"
                if (s.Length > 5 && s[^5] is '+' or '-' && char.IsAsciiDigit(s[^1]))
                {
                    s = s.Insert(s.Length - 2, ":");
                }

                if (!DateTimeOffset.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out parsed))
                {
                    instant = default;
                    return false;
                }
                break;
            default:
                instant = default;
                return false;
        }

        // Salesforce keeps date/time values to the second ("full timestamps with a precision of one second",
        // Object Reference, Primitive Data Types); time values keep milliseconds
        var utc = parsed.ToUniversalTime();
        instant = new DateTimeOffset(utc.UtcTicks - (utc.UtcTicks % TimeSpan.TicksPerSecond), TimeSpan.Zero);
        return true;
    }

    private static bool TryTime(object value, out TimeOnly time)
    {
        if (value is TimeOnly t)
        {
            time = t;
        }
        else if (value is not string s
                 || !TimeOnly.TryParse(s.TrimEnd('Z', 'z'), CultureInfo.InvariantCulture, DateTimeStyles.None, out time))
        {
            time = default;
            return false;
        }

        time = new TimeOnly(time.Ticks - (time.Ticks % TimeSpan.TicksPerMillisecond));
        return true;
    }

    private static bool Fail(SalesforceField field, string code, string message, out FieldError error)
    {
        error = new FieldError(field.Name, code, message);
        return false;
    }
}