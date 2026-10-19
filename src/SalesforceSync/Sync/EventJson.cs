using System.Text.Json;

namespace SalesforceSync.Sync;

/// <summary>
/// How inbox payloads (whole event envelopes) are serialized and deserialized: camelCase JSON, matched to the
/// PascalCase CLR properties without regard to case.
/// </summary>
public static class EventJson
{
    public static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);
}
