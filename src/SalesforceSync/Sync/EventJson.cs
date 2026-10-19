using System.Text.Json;

namespace SalesforceSync.Sync;

/// <summary>
/// Inbox payload (whole event envelope) serialization: camelCase JSON, case-insensitive match to PascalCase CLR
/// properties.
/// </summary>
public static class EventJson
{
    public static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);
}
