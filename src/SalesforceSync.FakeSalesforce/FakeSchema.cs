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
    int Digits = 0,
    string? KeyOf = null)
{
    /// <summary>
    /// A reference field's relationship name, by Salesforce's convention: <c>AccountId</c> -> <c>Account</c>,
    /// <c>Parent__c</c> -> <c>Parent__r</c>.
    /// </summary>
    public string? RelationshipName => Type != "reference" ? null
        : Name.EndsWith("__c", StringComparison.Ordinal) ? $"{Name[..^3]}__r"
        : Name.EndsWith("Id", StringComparison.Ordinal) ? Name[..^2]
        : null;
}

/// <summary>A record type of the mock org, as Describe's <c>recordTypeInfos</c> lists it.</summary>
public sealed record FakeRecordType(
    string DeveloperName,
    string Name,
    string Id,
    bool Active = true,
    bool Available = true,
    bool Master = false,
    bool Default = false);

/// <summary>
/// The mock org's metadata (served by sObject Describe) and how it stores values, the way Salesforce does:
/// unknown and read-only fields are rejected, numbers are rounded to the field's scale, date/time is kept to the second, a checkbox is never null,
/// multi-select values are kept in picklist order, text is length-checked, a record type must be one of the object's
/// that is active and available. Deliberately independent of the client's
/// normalization, so tests catch a client that would make fields flap.
/// <para>
/// A Text field with <see cref="FakeField.KeyOf"/> is a foreign key: it holds the external id of a record of that
/// object, stored as plain text. Like the org's automation, a create or update that sets it to an external id no such
/// record has is rejected (<see cref="ParentMissingCode"/>, <see cref="ParentMissingMessage"/>).
/// </para>
/// </summary>
public static class FakeSchema
{
    /// <summary>
    /// What the mock org answers for a foreign key whose parent doesn't exist. A real org's automation decides its
    /// own (most likely this code, a validation rule's or trigger's, with a message of its own): verify in a sandbox.
    /// </summary>
    public const string ParentMissingCode = "FIELD_CUSTOM_VALIDATION_EXCEPTION";

    public const string ParentMissingMessage = "Parent record not found";

    public const string RecordTypeIdField = "RecordTypeId";

    /// <summary>The Master record type (its Id is the same in every org).</summary>
    private static readonly FakeRecordType Master = new("Master", "Master", "012000000000000AAA", Available: false, Master: true);

    public static readonly IReadOnlyDictionary<string, IReadOnlyList<FakeField>> Objects =
        new Dictionary<string, IReadOnlyList<FakeField>>(StringComparer.OrdinalIgnoreCase)
        {
            ["Account"] =
            [
                new("Id", "id", 18, Createable: false, Updateable: false, Nillable: false),
                new(RecordTypeIdField, "reference", 18, ReferenceTo: "RecordType"),
                new("ExternalId__c", "string", 255),
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
                new(RecordTypeIdField, "reference", 18, ReferenceTo: "RecordType"),
                new("Name", "string", 80),
                new("ExternalId__c", "string", 255),
                new("AccountId__c", "string", 255, KeyOf: "Account"),
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
                new(RecordTypeIdField, "reference", 18, ReferenceTo: "RecordType"),
                new("Name", "string", 80),
                new("ExternalId__c", "string", 255),
                new("AccountId__c", "string", 255, KeyOf: "Account"),
                new("RelatedAccountId__c", "string", 255, KeyOf: "Account"),
                new("Type__c", "picklist", 255),
                new("Start_Date__c", "date"),
                new("Active__c", "boolean", Nillable: false),
                new("CreatedDate", "datetime", Createable: false, Updateable: false, Nillable: false),
                new("LastModifiedDate", "datetime", Createable: false, Updateable: false, Nillable: false)
            ],
            ["Contact"] =
            [
                new("Id", "id", 18, Createable: false, Updateable: false, Nillable: false),
                new(RecordTypeIdField, "reference", 18, ReferenceTo: "RecordType"),
                new("ExternalId__c", "string", 255),
                // The standard lookup every org's Contact has; this org's sync leaves it alone and uses AccountId__c
                new("AccountId", "reference", 18, ReferenceTo: "Account"),
                new("AccountId__c", "string", 255, KeyOf: "Account"),
                new("FirstName", "string", 40),
                new("LastName", "string", 80, Nillable: false),
                new("Email", "email", 80),
                new("Phone", "phone", 40),
                new("CreatedDate", "datetime", Createable: false, Updateable: false, Nillable: false),
                new("LastModifiedDate", "datetime", Createable: false, Updateable: false, Nillable: false)
            ]
        };

    /// <summary>
    /// Every object's record types: one per synced object, with the developer name its sObject class declares, plus
    /// Master (which every object has).
    /// </summary>
    public static readonly IReadOnlyDictionary<string, IReadOnlyList<FakeRecordType>> RecordTypes =
        new Dictionary<string, IReadOnlyList<FakeRecordType>>(StringComparer.OrdinalIgnoreCase)
        {
            ["Account"] = [Master, new("Business", "Business", FakeRecords.Id18("012FAKE00000001"), Default: true)],
            ["Profile__c"] = [Master, new("Standard", "Standard", FakeRecords.Id18("012FAKE00000002"), Default: true)],
            ["Account_Relationship__c"] = [Master, new("Standard", "Standard", FakeRecords.Id18("012FAKE00000003"), Default: true)],
            ["Contact"] = [Master, new("Standard", "Standard", FakeRecords.Id18("012FAKE00000004"), Default: true)]
        };

    /// <summary>The first three characters of an object's record Ids.</summary>
    private static readonly Dictionary<string, string> KeyPrefixes = new(StringComparer.OrdinalIgnoreCase)
    {
        ["Account"] = "001",
        ["Contact"] = "003",
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
            })]),
            ["recordTypeInfos"] = new JsonArray([.. RecordTypes[sobject].Select(r => (JsonNode)new JsonObject
            {
                ["developerName"] = r.DeveloperName,
                ["name"] = r.Name,
                ["recordTypeId"] = r.Id,
                ["active"] = r.Active,
                ["available"] = r.Available,
                ["master"] = r.Master,
                ["defaultRecordTypeMapping"] = r.Default
            })])
        };
    }

    /// <summary>The 18-character Id of the object's record type with this developer name.</summary>
    public static string RecordTypeIdOf(string sobject, string developerName)
    {
        return RecordTypes[sobject].Single(r => r.DeveloperName == developerName).Id;
    }

    /// <summary>Whether records of the object can be given the record type with this (15- or 18-character) Id.</summary>
    public static bool IsAssignableRecordType(string sobject, string id)
    {
        return RecordTypes.TryGetValue(sobject, out var recordTypes)
               && recordTypes.Any(r => r.Active && r.Available && id.Length >= 15 && r.Id.StartsWith(id[..15], StringComparison.Ordinal));
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