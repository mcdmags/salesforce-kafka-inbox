using System.Text.Json;

namespace SalesforceSync.Infrastructure.Salesforce;

/// <summary>
/// Reads Salesforce's JSON answers into <see cref="RecordResult"/> and <see cref="SalesforceError"/>. Kept apart from
/// <see cref="SalesforceRestClient"/> so the client stays about sending: parsing here is pure, with no HTTP or state.
/// </summary>
internal static class SalesforceResponseParser
{
    internal static List<SalesforceError> ParseErrors(string? referenceId, JsonElement body)
    {
        return body.ValueKind != JsonValueKind.Array
            ? [new SalesforceError(referenceId, "UNKNOWN_ERROR", body.ValueKind == JsonValueKind.Undefined ? null : body.GetRawText())]
            : [.. body.EnumerateArray().Select(e => ParseError(referenceId, e, "errorCode"))];
    }

    internal static RecordResult ParseRecordResult(JsonElement record)
    {
        return new(
            record.TryGetProperty("success", out var ok) && ok.ValueKind == JsonValueKind.True,
            record.TryGetProperty("id", out var id) && id.ValueKind == JsonValueKind.String ? id.GetString() : null,
            record.TryGetProperty("errors", out var errors) && errors.ValueKind == JsonValueKind.Array
                ? errors.EnumerateArray().Select(e => ParseError(null, e, "statusCode")).ToList()
                : [],
            record.TryGetProperty("created", out var created) && created.ValueKind is JsonValueKind.True or JsonValueKind.False
                ? created.GetBoolean()
                : null);
    }

    /// <param name="codeProperty">"errorCode" in an error response, "statusCode" in a collection's per-record errors.</param>
    /// <param name="referenceId">The subrequest the error belongs to, null for a call of its own.</param>
    /// <param name="e">One error object of the response.</param>
    internal static SalesforceError ParseError(string? referenceId, JsonElement e, string codeProperty)
    {
        return new(
            referenceId,
            StringProperty(e, codeProperty) ?? "UNKNOWN_ERROR",
            StringProperty(e, "message"),
            e.ValueKind == JsonValueKind.Object && e.TryGetProperty("fields", out var f) && f.ValueKind == JsonValueKind.Array
                ? f.EnumerateArray().Where(x => x.ValueKind == JsonValueKind.String).Select(x => x.GetString()!).ToList()
                : null);
    }

    internal static string? StringProperty(JsonElement e, string name)
    {
        return e.ValueKind == JsonValueKind.Object && e.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.String
                ? value.GetString()
                : null;
    }
}
