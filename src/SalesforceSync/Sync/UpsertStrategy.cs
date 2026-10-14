using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using SalesforceSync.Inbox.Payloads;

namespace SalesforceSync.Sync;

/// <summary>
/// What varies per synced object: how to read an event's entity id and meta (time and type), how an
/// event becomes Salesforce records, and what to read from Salesforce to diff them and resolve their lookups.
/// Everything else (claiming, ordering, coalescing, rate limits, batching, retries, dead-lettering) is
/// <see cref="SalesforceBatchUpsertService"/>'s. The engine, the Kafka consumer and the SQL never name an event
/// field: they go through <paramref name="EntityId"/> and <paramref name="Meta"/>.
/// </summary>
/// <typeparam name="TEvent">The inbox payload (the whole event envelope).</typeparam>
/// <param name="EntityType">The strategy's name, e.g. "account": the key of its watermarks and of its topic setting.</param>
/// <param name="EntityId">The id of the entity an event is about (e.g. account.externalId), or null if it has none.</param>
/// <param name="Meta">The event's meta, or null if it has none: when the event happened (meta.time, which must have an
/// explicit offset) and what the source says happened (meta.type: inserted or updated). The type is only checked at
/// ingest, so a delete is never synced; whether a record is created or updated is decided by what Salesforce has.</param>
/// <param name="Map">Inbox event (with its entity id) -> the records to upsert together.</param>
/// <param name="Queries">The batch's units -> the reads (at most 5 after chunking) the diff and lookups need.</param>
public sealed record UpsertStrategy<TEvent>(
    string EntityType,
    Func<TEvent, string?> EntityId,
    Func<TEvent, EventMeta?> Meta,
    Func<string, TEvent, UpsertUnit> Map,
    Func<IReadOnlyCollection<UpsertUnit>, IEnumerable<SoqlQuery>> Queries)
{
    /// <summary>
    /// Reads and validates a raw event: valid JSON for <typeparamref name="TEvent"/>, an entity with a non-empty id,
    /// a time with an explicit offset, and a type of inserted or updated. The Kafka consumer admits only events that pass (anything else can never
    /// be synced), and the engine reads claimed rows the same way.
    /// </summary>
    /// <returns>False with <paramref name="problem"/> saying why the event can never be synced.</returns>
    public bool TryRead(string json, [NotNullWhen(true)] out ReadEvent<TEvent>? read, [NotNullWhen(false)] out string? problem)
    {
        read = null;
        TEvent? e;
        try
        {
            e = JsonSerializer.Deserialize<TEvent>(json, SalesforceBatchUpsertService.EventJson);
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
        else if (Meta(e)?.Kind() is null)
        {
            problem = "The event type must be \"inserted\" or \"updated\" (deletes are not synced)";
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