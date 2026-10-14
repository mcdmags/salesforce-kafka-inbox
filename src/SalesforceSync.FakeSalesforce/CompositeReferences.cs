using System.Text.Json.Nodes;
using System.Text.RegularExpressions;

namespace SalesforceSync.FakeSalesforce;

/// <summary>
/// References between Composite subrequests: a later one can use an earlier one's result as
/// <c>@{referenceId.field}</c> or <c>@{referenceId[i].field}</c> (case-sensitive).
/// </summary>
internal static partial class CompositeReferences
{
    [GeneratedRegex(@"^@\{(?<path>[^}]+)\}$")]
    private static partial Regex Reference();

    [GeneratedRegex(@"(?<name>[A-Za-z0-9_]+)(?:\[(?<index>\d+)\])*")]
    private static partial Regex ReferencePart();

    /// <summary>Replaces every "@{ref.path}" string in the body; returns the first reference that has no value, or null.</summary>
    /// <param name="results">Reference id -> the response body of that earlier subrequest.</param>
    public static string? Resolve(JsonNode node, IReadOnlyDictionary<string, JsonNode?> results)
    {
        switch (node)
        {
            case JsonObject obj:
                foreach (var key in obj.Select(kv => kv.Key).ToList())
                {
                    if (FakeJson.Str(obj[key]) is { } text && Reference().Match(text) is { Success: true } m)
                    {
                        if (ValueAt(m.Groups["path"].Value, results) is not { } value) return m.Groups["path"].Value;
                        obj[key] = value.DeepClone();
                    }
                    else if (obj[key] is JsonObject or JsonArray && Resolve(obj[key]!, results) is { } unresolved)
                    {
                        return unresolved;
                    }
                }
                return null;
            case JsonArray array:
                foreach (var item in array)
                    if (item is not null && Resolve(item, results) is { } unresolved) return unresolved;
                return null;
            default:
                return null;
        }
    }

    /// <summary>ref.field, ref[0].field, ref.items[2].Id ...: a failed record has no id, so a reference to it has no value.</summary>
    private static JsonNode? ValueAt(string path, IReadOnlyDictionary<string, JsonNode?> results)
    {
        var parts = ReferencePart().Matches(path);
        if (parts.Count == 0 || !results.TryGetValue(parts[0].Groups["name"].Value, out var current)) return null;
        for (var i = 0; i < parts.Count && current is not null; i++)
        {
            if (i > 0) current = current is JsonObject o && o.TryGetPropertyValue(parts[i].Groups["name"].Value, out var next) ? next : null;
            foreach (Capture index in parts[i].Groups["index"].Captures)
                current = current is JsonArray a && int.Parse(index.Value) < a.Count ? a[int.Parse(index.Value)] : null;
        }
        return current;
    }
}
