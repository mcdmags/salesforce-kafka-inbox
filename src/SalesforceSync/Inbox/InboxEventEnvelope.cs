using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;

namespace SalesforceSync.Inbox;

/// <summary>The entity type, key and time of a valid event, as the inbox's SQL functions will read them.</summary>
public sealed record InboxEventHeader(string EntityType, string Key, DateTimeOffset Time);

/// <summary>
/// The shape every inbox event must have, checked before it is stored:
/// <code>{ "meta": { "time": "2026-10-14T01:02:03.456Z" }, "&lt;entity type&gt;": { "externalId": "...", ... } }</code>
/// sf_event_type / sf_event_key / sf_event_version (sql/schema.sql) read the same three things in Postgres, and rely on
/// this check: exactly one entity object, and a time with an explicit offset (what makes sf_event_version immutable).
/// </summary>
public static partial class InboxEventEnvelope
{
    public const string Meta = "meta";
    public const string Time = "time";
    public const string ExternalId = "externalId";

    /// <summary>
    /// ISO-8601 date and time with an offset or Z. No other formats: Postgres would parse e.g. "now" or a time without an
    /// offset too, but depending on the session, so it could order the same event differently later.
    /// </summary>
    [GeneratedRegex(@"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,9})?(Z|[+-]\d{2}:\d{2})$", RegexOptions.CultureInvariant)]
    private static partial Regex IsoTimeWithOffset();

    /// <summary>The event's header, or why the event can never be synced (to skip it rather than store it).</summary>
    /// <param name="entityTypes">The types a registered <see cref="Sync.ISalesforceEntitySync"/> handles.</param>
    public static bool TryRead(string value, IReadOnlySet<string> entityTypes, out InboxEventHeader? header, out string? problem)
    {
        header = null;
        try
        {
            if (JsonNode.Parse(value) is not JsonObject json)
                return Fail("Event is not a JSON object", out problem);

            var entities = json.Where(p => p.Key != Meta).ToList();
            if (entities.Count != 1)
                return Fail($"Event must have exactly one entity object besides \"{Meta}\", found {entities.Count}", out problem);

            var (type, body) = entities[0];
            if (!entityTypes.Contains(type))
                return Fail($"No sync handles entity type \"{type}\"", out problem);
            if (body is not JsonObject entity)
                return Fail($"\"{type}\" is not an object", out problem);
            if (entity[ExternalId] is not JsonValue id || id.GetValueKind() != JsonValueKind.String ||
                string.IsNullOrWhiteSpace(id.GetValue<string>()))
                return Fail($"{type}.{ExternalId} must be a non-empty string", out problem);

            if (json[Meta]?[Time] is not JsonValue t || t.GetValueKind() != JsonValueKind.String ||
                !IsoTimeWithOffset().IsMatch(t.GetValue<string>()) ||
                !DateTimeOffset.TryParse(t.GetValue<string>(), CultureInfo.InvariantCulture, DateTimeStyles.None, out var time))
                return Fail($"{Meta}.{Time} must be an ISO-8601 time with an offset or Z, e.g. 2026-10-14T01:02:03.456Z", out problem);

            header = new InboxEventHeader(type, id.GetValue<string>(), time);
            problem = null;
            return true;
        }
        catch (Exception ex) when (ex is JsonException or ArgumentException)
        {
            // Not JSON, or a duplicate property name
            return Fail(ex.Message, out problem);
        }
    }

    private static bool Fail(string message, out string? problem)
    {
        problem = message;
        return false;
    }
}
