using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace SalesforceSync.FakeSalesforce;

public sealed record FakeField(
    string Name,
    string Type,
    int Length = 0,
    int Precision = 0,
    int Scale = 0,
    bool Createable = true,
    bool Updateable = true,
    bool Nillable = true,
    string? ReferenceTo = null,
    string? RelationshipName = null,
    int Digits = 0);

/// <summary>
/// The mock org's metadata (served by sObject Describe) and how it stores values, the way Salesforce does:
/// unknown and read-only fields are rejected, numbers are rounded to the field's scale, date/time is kept to the second, a checkbox is never null,
/// multi-select values are kept in picklist order, text is length-checked. Deliberately independent of the client's
/// normalization, so tests catch a client that would make fields flap.
/// </summary>
public static class FakeSchema
{
    public static readonly IReadOnlyDictionary<string, IReadOnlyList<FakeField>> Objects =
        new Dictionary<string, IReadOnlyList<FakeField>>(StringComparer.OrdinalIgnoreCase)
        {
            ["Account"] =
            [
                new("Id", "id", 18, Createable: false, Updateable: false, Nillable: false),
                new("External_Id__c", "string", 255),
                new("Name", "string", 255, Nillable: false),
                new("Rating", "picklist", 255),
                new("Industry", "picklist", 255),
                new("Description", "textarea", 32000),
                new("AnnualRevenue", "currency", Precision: 18, Scale: 2),
                new("NumberOfEmployees", "int", Digits: 8),
                new("Phone", "phone", 40),
                new("Website", "url", 255),
                new("Active__c", "boolean", Nillable: false),
                new("Tags__c", "multipicklist", 4099),
                new("Customer_Since__c", "date"),
                new("Last_Contacted__c", "datetime"),
                new("CreatedDate", "datetime", Createable: false, Updateable: false, Nillable: false),
                new("LastModifiedDate", "datetime", Createable: false, Updateable: false, Nillable: false)
            ],
            ["Profile__c"] =
            [
                new("Id", "id", 18, Createable: false, Updateable: false, Nillable: false),
                new("Name", "string", 80),
                new("External_Id__c", "string", 255),
                new("Account__c", "reference", 18, ReferenceTo: "Account", RelationshipName: "Account__r"),
                new("Tier__c", "picklist", 255),
                new("Score__c", "double", Precision: 18, Scale: 2),
                new("Discount__c", "percent", Precision: 5, Scale: 2),
                new("Notes__c", "textarea", 255),
                new("Opt_In__c", "boolean", Nillable: false),
                new("Start_Date__c", "date"),
                new("CreatedDate", "datetime", Createable: false, Updateable: false, Nillable: false),
                new("LastModifiedDate", "datetime", Createable: false, Updateable: false, Nillable: false)
            ],
            ["Account_Relationship__c"] =
            [
                new("Id", "id", 18, Createable: false, Updateable: false, Nillable: false),
                new("Name", "string", 80),
                new("External_Id__c", "string", 255),
                new("Account__c", "reference", 18, ReferenceTo: "Account", RelationshipName: "Account__r"),
                new("Related_Account__c", "reference", 18, ReferenceTo: "Account", RelationshipName: "Related_Account__r"),
                new("Type__c", "picklist", 255),
                new("Start_Date__c", "date"),
                new("Active__c", "boolean", Nillable: false),
                new("CreatedDate", "datetime", Createable: false, Updateable: false, Nillable: false),
                new("LastModifiedDate", "datetime", Createable: false, Updateable: false, Nillable: false)
            ]
        };

    /// <summary>The first three characters of an object's record Ids.</summary>
    private static readonly Dictionary<string, string> KeyPrefixes = new(StringComparer.OrdinalIgnoreCase)
    {
        ["Account"] = "001",
        ["Profile__c"] = "a00"
    };

    /// <summary>Custom objects without a prefix of their own share one.</summary>
    public static string KeyPrefixOf(string sobject)
    {
        return KeyPrefixes.GetValueOrDefault(sobject, "a01");
    }

    public static FakeField? Find(string sobject, string field)
    {
        return Objects.TryGetValue(sobject, out var fields)
                ? fields.FirstOrDefault(f => f.Name.Equals(field, StringComparison.OrdinalIgnoreCase))
                : null;
    }

    public static JsonObject Describe(string sobject)
    {
        return new()
        {
            ["name"] = Objects.Keys.First(k => k.Equals(sobject, StringComparison.OrdinalIgnoreCase)),
            ["fields"] = new JsonArray([.. Objects[sobject].Select(f => (JsonNode)new JsonObject
            {
                ["name"] = f.Name,
                ["type"] = f.Type,
                ["length"] = f.Length,
                ["precision"] = f.Precision,
                ["scale"] = f.Scale,
                ["digits"] = f.Digits,
                ["createable"] = f.Createable,
                ["updateable"] = f.Updateable,
                ["nillable"] = f.Nillable,
                ["referenceTo"] = f.ReferenceTo is null ? [] : new JsonArray(f.ReferenceTo),
                ["relationshipName"] = f.RelationshipName
            })])
        };
    }

    /// <summary>The value Salesforce would store, or an error (code, message) it would return.</summary>
    public static (JsonNode? Stored, string? Code, string? Message) Store(FakeField field, JsonNode? value)
    {
        var kind = value?.GetValueKind() ?? JsonValueKind.Null;
        var text = kind == JsonValueKind.String ? value!.GetValue<string>() : null;
        if (kind == JsonValueKind.Null || text == "")
        {
            if (field.Type == "boolean")
            {
                return (false, null, null);
            }

            return field.Nillable ? (null, null, null) : (null, "REQUIRED_FIELD_MISSING", $"Required fields are missing: [{field.Name}]");
        }

        switch (field.Type)
        {
            case "int" or "double" or "currency" or "percent":
                {
                    decimal number;
                    if (kind == JsonValueKind.Number)
                    {
                        number = decimal.Parse(value!.ToJsonString(), NumberStyles.Float, CultureInfo.InvariantCulture);
                    }
                    else if (text is not null && decimal.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed))
                    {
                        number = parsed;
                    }
                    else
                    {
                        return Invalid(field, value);
                    }

                    number = Math.Round(number, field.Type == "int" ? 0 : field.Scale, MidpointRounding.AwayFromZero);
                    return (JsonValue.Create(number), null, null);
                }
            case "boolean":
                if (kind is JsonValueKind.True or JsonValueKind.False)
                {
                    return (value!.DeepClone(), null, null);
                }

                return text is not null && bool.TryParse(text, out var b) ? (b, null, null) : Invalid(field, value);
            case "multipicklist":
                if (text is null)
                {
                    return Invalid(field, value);
                }
                // Salesforce returns the selected values in the picklist's defined order; here: alphabetical
                var ordered = string.Join(';', text.Split(';', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
                    .Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal));
                return TooLong(field, ordered) ?? (ordered, null, null);
            case "date":
                return text is not null && DateOnly.TryParseExact(text, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var d)
                    ? (d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture), null, null)
                    : Invalid(field, value);
            case "datetime":
                // Stored to the second, like Salesforce's date/time fields
                return text is not null && DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var t)
                    ? (FormatDateTime(new DateTimeOffset(t.UtcTicks - (t.UtcTicks % TimeSpan.TicksPerSecond), TimeSpan.Zero)), null, null)
                    : Invalid(field, value);
            case "id" or "reference":
                return text is { Length: 15 or 18 } ? (text, null, null) : (null, "MALFORMED_ID", $"{field.Name}: id value of incorrect type: {text}");
            default:
                {
                    var s = text ?? value!.ToJsonString();
                    return TooLong(field, s) ?? (s, null, null);
                }
        }
    }

    /// <summary>Salesforce's date/time format, e.g. 2026-10-13T01:02:03.000+0000.</summary>
    public static string FormatDateTime(DateTimeOffset t)
    {
        return t.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss.fff'+0000'", CultureInfo.InvariantCulture);
    }

    private static (JsonNode?, string?, string?)? TooLong(FakeField field, string text)
    {
        return field.Length > 0 && text.Length > field.Length
                ? (null, "STRING_TOO_LONG", $"{field.Name}: data value too large: {text[..Math.Min(20, text.Length)]}... (max length={field.Length})")
                : null;
    }

    private static (JsonNode?, string?, string?) Invalid(FakeField field, JsonNode? value)
    {
        return (null, "INVALID_TYPE_ON_FIELD_IN_RECORD", $"{field.Name}: value not of required type: {value?.ToJsonString()}");
    }
}