using System.Text.Json.Nodes;

namespace SalesforceSync.FakeSalesforce;

/// <summary>The JSON shapes Salesforce answers with.</summary>
internal static class FakeJson
{
    /// <summary>An error response body: <c>[{ "errorCode": ..., "message": ..., "fields": [...] }]</c>.</summary>
    public static JsonArray Errors(string code, string message, params string[] fields)
    {
        return new JsonArray(new JsonObject
        {
            ["errorCode"] = code,
            ["message"] = message,
            ["fields"] = new JsonArray([.. fields.Select(f => (JsonNode)f)])
        });
    }

    /// <summary>One subrequest's entry in a Composite response.</summary>
    public static JsonObject SubResponse(string referenceId, int status, JsonNode? body)
    {
        return new()
        {
            ["body"] = body,
            ["httpHeaders"] = new JsonObject(),
            ["httpStatusCode"] = status,
            ["referenceId"] = referenceId
        };
    }

    /// <summary>
    /// The node's value if it is a JSON string, else null. TryGetValue, not GetValueKind: on a value the org stored
    /// (not parsed from JSON), .NET 8's GetValueKind serializes it to find its kind, and record lookups call this for
    /// every record they scan.
    /// </summary>
    public static string? Str(JsonNode? node)
    {
        return node is JsonValue v && v.TryGetValue<string>(out var s) ? s : null;
    }
}