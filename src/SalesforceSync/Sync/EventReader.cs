using System.Diagnostics.CodeAnalysis;
using System.Text.Json;

namespace SalesforceSync.Sync;

/// <summary>
/// How to read an event of one type: whose it is (the entity id) and when and what happened (its meta). Shared by the two
/// halves of the sync, so they agree on what a valid event is: ingest (<see cref="Persistence.IAggregateIngest"/>)
/// stores only events that read, and the engine reads claimed rows the same way. Knows nothing of Salesforce or aggregates.
/// </summary>
/// <typeparam name="TEvent">The inbox payload (the whole event envelope).</typeparam>
/// <param name="EntityType">The type's name, e.g. "account": the object name of its events, and how logs name it.</param>
/// <param name="EntityId">The id of the entity an event is about (e.g. account.externalId), or null if it has none.</param>
/// <param name="Meta">The event's meta, or null if it has none: when the event happened (meta.time, which must have an
/// explicit offset) and what happened (meta.type: inserted, updated or deleted).</param>
public sealed record EventReader<TEvent>(
    string EntityType,
    Func<TEvent, string?> EntityId,
    Func<TEvent, EventMeta?> Meta)
{
    /// <summary>
    /// Reads and validates a raw event: valid JSON for <typeparamref name="TEvent"/>, an entity with a non-empty id,
    /// a time with an explicit offset, and a type of inserted, updated or deleted. The Kafka consumer stores only events
    /// that pass (anything else can never be synced). The engine reads claimed rows the same way.
    /// </summary>
    /// <returns>False with <paramref name="problem"/> saying why the event can never be synced.</returns>
    public bool TryRead(string json, [NotNullWhen(true)] out ReadEvent<TEvent>? read, [NotNullWhen(false)] out string? problem)
    {
        read = null;
        TEvent? e;
        try
        {
            e = JsonSerializer.Deserialize<TEvent>(json, EventJson.Options);
        }
        catch (JsonException ex)
        {
            problem = $"Not a valid {EntityType} event: {ex.Message}";
            return false;
        }

        if (e is null)
        {
            problem = "Event is null";
        }
        else if (EntityId(e) is not { } id || string.IsNullOrWhiteSpace(id))
        {
            problem = $"A {EntityType} event needs its entity object with a non-empty id";
        }
        else if (Meta(e)?.Timestamp() is not { } time)
        {
            problem = "The event time must be an ISO-8601 time with an offset or Z, e.g. 2026-10-14T01:02:03.456Z";
        }
        else if (Meta(e) is { } meta && !meta.IsInsertOrUpdate() && !meta.IsDelete())
        {
            problem = "The event type must be \"inserted\", \"updated\" or \"deleted\"";
        }
        else
        {
            read = new ReadEvent<TEvent>(e, id, time);
            problem = null;
            return true;
        }
        return false;
    }
}

/// <summary>An inbox event that passed <see cref="EventReader{TEvent}.TryRead"/>: the typed event, whose it is and when.</summary>
public sealed record ReadEvent<TEvent>(TEvent Event, string EntityId, DateTimeOffset Time);
