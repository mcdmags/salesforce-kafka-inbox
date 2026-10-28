using System.Diagnostics.CodeAnalysis;
using System.Text.Json;

namespace SalesforceSync.Sync;

/// <summary>
/// How to read one type's event: whose (entity id), when + what (meta). Shared by both sync halves so they agree on
/// validity: ingest (<see cref="Persistence.IAggregateIngest"/>) stores only events that read, and
/// <see cref="ToSalesforce.SalesforceBatchSyncService"/> reads claimed rows the same way. Knows no Salesforce or aggregates.
/// </summary>
/// <typeparam name="TEvent">Inbox payload (whole event envelope).</typeparam>
/// <param name="EntityType">Type name, e.g. "account": object name of its events; used in logs.</param>
/// <param name="EntityId">Id of entity event is about (e.g. account.externalId), null if none.</param>
/// <param name="Meta">Event meta, null if none: when (meta.time) + what (meta.type:
/// inserted, updated, deleted).</param>
public sealed record EventReader<TEvent>(
    string EntityType,
    Func<TEvent, string?> EntityId,
    Func<TEvent, EventMeta?> Meta)
{
    /// <returns>False with <paramref name="problem"/> = why event can never sync.</returns>
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
        else if (Meta(e)?.Time is not { } time)
        {
            problem = "The event needs its time (meta.time), e.g. 2026-10-14T01:02:03.456Z";
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

/// <summary>Inbox event that passed <see cref="EventReader{TEvent}.TryRead"/>: typed event, whose, when.</summary>
public sealed record ReadEvent<TEvent>(TEvent Event, string EntityId, DateTimeOffset Time);
