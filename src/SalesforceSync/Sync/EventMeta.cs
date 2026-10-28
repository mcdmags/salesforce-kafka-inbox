using System.Text.Json.Serialization;

namespace SalesforceSync.Sync;

/// <summary>
/// <c>meta</c> object every event carries next to its entity:
/// <c>{ "time": "2026-10-14T01:02:03.456Z", "type": "inserted" }</c>.
/// Time = event's place in entity history: newest wins whatever arrival order, deletes too. Type is inserted or updated
/// (same to the sync: Salesforce state decides create/update), or deleted.
/// </summary>
public sealed class EventMeta
{
    /// <summary>
    /// When it happened, with its offset (the source always sends one, e.g. <c>2026-10-14T01:02:03.456Z</c>). Null when
    /// missing; a value that isn't a date-time fails the JSON read (path <c>$.meta.time</c>).
    /// </summary>
    public DateTimeOffset? Time { get; set; }

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
    public static EventMeta Of(EventType kind, DateTimeOffset time)
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
}

/// <summary>What happened to the entity: <see cref="EventMeta.Type"/> as an enum.</summary>
public enum EventType
{
    Inserted,
    Updated,
    Deleted
}
