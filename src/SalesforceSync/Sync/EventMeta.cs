using System.Globalization;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;

namespace SalesforceSync.Sync;

/// <summary>
/// <c>meta</c> object every event carries next to its entity:
/// <c>{ "time": "2026-10-14T01:02:03.456Z", "type": "inserted" }</c>.
/// Time = event's place in entity history: newest wins whatever arrival order, deletes too. Type is inserted or updated
/// (same to the sync: Salesforce state decides create/update), or deleted.
/// </summary>
public sealed partial class EventMeta
{
    /// <summary>
    /// As sent. Text on purpose, read through <see cref="Timestamp"/>:
    /// <list type="bullet">
    /// <item>As a DateTimeOffset, the JSON reader would accept a time without an offset and apply the reading machine's
    /// time zone, so pods in different zones could order the same entity's events differently.</item>
    /// <item>A bad time fails only this check, with a clear reason ("event time"), not the whole JSON read.</item>
    /// <item>Child events copy the text unchanged (<see cref="AggregateChange"/>): meta.time may have 9 fraction digits,
    /// DateTimeOffset keeps 7, so a parse and rewrite would change it and could make two times equal.</item>
    /// </list>
    /// </summary>
    public string? Time { get; set; }

    /// <summary>
    /// ISO-8601 date-time with offset or Z, nothing else: offset-less time would be read in worker's zone, so pods
    /// could order the same event differently.
    /// </summary>
    [GeneratedRegex(@"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,9})?(Z|[+-]\d{2}:\d{2})$", RegexOptions.CultureInvariant)]
    private static partial Regex IsoTimeWithOffset();

    /// <summary>
    /// As sent: "inserted", "updated" or "deleted", any case. Text on the wire, so anything else is rejected by name,
    /// not as bad JSON; read it through <see cref="Kind"/>.
    /// </summary>
    public string? Type { get; set; }

    /// <summary><see cref="Type"/> as an enum; null when it isn't one of the three (any case).</summary>
    [JsonIgnore]
    public EventType? Kind => Enum.GetValues<EventType>().Cast<EventType?>()
        .FirstOrDefault(k => k.ToString()!.Equals(Type, StringComparison.OrdinalIgnoreCase));

    /// <summary>A meta of this kind at this time, its type written as the source writes it ("deleted").</summary>
    public static EventMeta Of(EventType kind, string time)
    {
        return new EventMeta { Type = kind.ToString().ToLowerInvariant(), Time = time };
    }

    /// <summary>
    /// <see cref="Type"/> is inserted or updated. Which one decides nothing: Salesforce state decides create/update.
    /// </summary>
    public bool IsInsertOrUpdate()
    {
        return Kind is EventType.Inserted or EventType.Updated;
    }

    public bool IsDelete()
    {
        return Kind == EventType.Deleted;
    }

    /// <summary><see cref="Time"/> as instant; null if missing or not ISO-8601 with offset.</summary>
    public DateTimeOffset? Timestamp()
    {
        return Time is not null && IsoTimeWithOffset().IsMatch(Time) &&
               DateTimeOffset.TryParse(Time, CultureInfo.InvariantCulture, DateTimeStyles.None, out var time)
                ? time
                : null;
    }
}

/// <summary>What happened to the entity: <see cref="EventMeta.Type"/> as an enum.</summary>
public enum EventType
{
    Inserted,
    Updated,
    Deleted
}
