using System.Globalization;
using System.Text.Json.Nodes;

namespace SalesforceSync.Tests;

/// <summary>Inbox events as the sources send them, for the tests that sync them.</summary>
internal static class Events
{
    public static DateTimeOffset TimeOf(string json)
    {
        return DateTimeOffset.Parse((string)JsonNode.Parse(json)!["meta"]!["time"]!, CultureInfo.InvariantCulture);
    }

    /// <summary>An Account event at second <paramref name="second"/> of a fixed minute; fields of <paramref name="account"/> override the defaults.</summary>
    public static string Account(string key, int second, string type = "updated", Action<JsonObject>? account = null)
    {
        var body = new JsonObject { ["externalId"] = key, ["name"] = "Acme" };
        account?.Invoke(body);
        return Event("account", body, second, type);
    }

    public static string Relationship(string key, string account, string related, int second, string type = "updated", Action<JsonObject>? with = null)
    {
        var body = new JsonObject { ["externalId"] = key, ["name"] = "Partner", ["accountExternalId"] = account, ["relatedAccountExternalId"] = related };
        with?.Invoke(body);
        return Event("accountRelationship", body, second, type);
    }

    public static string Contact(string key, string account, int second, string type = "updated", Action<JsonObject>? with = null)
    {
        var body = new JsonObject { ["externalId"] = key, ["accountExternalId"] = account, ["lastName"] = "Lovelace" };
        with?.Invoke(body);
        return Event("contact", body, second, type);
    }

    /// <summary>A delete: the key alone.</summary>
    public static string Delete(string entityType, string key, int second)
    {
        return Event(entityType, new JsonObject { ["externalId"] = key }, second, "deleted");
    }

    private static string Event(string entityType, JsonObject body, int second, string type)
    {
        return new JsonObject
        {
            ["meta"] = new JsonObject { ["time"] = $"2026-10-14T01:02:{second:D2}Z", ["type"] = type },
            [entityType] = body
        }.ToJsonString();
    }
}