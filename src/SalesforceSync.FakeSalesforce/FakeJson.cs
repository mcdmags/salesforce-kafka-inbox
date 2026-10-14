using System.Text.Json;
using System.Text.Json.Nodes;

namespace SalesforceSync.FakeSalesforce;

/// <summary>The JSON shapes Salesforce answers with.</summary>
internal static class FakeJson
{
    /// <summary>An error response body: <c>[{ "errorCode": ..., "message": ..., "fields": [...] }]</c>.</summary>
    public static JsonArray Errors(string code, string message, params string[] fields) =>
        new JsonArray(new JsonObject
        {
            ["errorCode"] = code, ["message"] = message, ["fields"] = new JsonArray(fields.Select(f => (JsonNode)f).ToArray())
        });

    /// <summary>One subrequest's entry in a Composite response.</summary>
    public static JsonObject SubResponse(string referenceId, int status, JsonNode? body) => new()
    {
        ["body"] = body,
        ["httpHeaders"] = new JsonObject(),
        ["httpStatusCode"] = status,
        ["referenceId"] = referenceId
    };

    /// <summary>The node's value if it is a JSON string, else null.</summary>
    public static string? Str(JsonNode? node) =>
        node is JsonValue v && v.GetValueKind() == JsonValueKind.String ? v.GetValue<string>() : null;
}
