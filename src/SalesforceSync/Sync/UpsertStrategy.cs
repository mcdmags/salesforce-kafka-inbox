using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;
using SalesforceSync.Salesforce;

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
/// <param name="Queries">The batch's units -> the reads (at most 5 after chunking) the diff and lookups need. Not called
/// with <see cref="WriteMode.DirectUpsert"/>, which reads nothing.</param>
/// <param name="Mode">How the records reach Salesforce: compared with what it has first (the default), or upserted
/// by external id without a read (<see cref="WriteMode"/>).</param>
public sealed record UpsertStrategy<TEvent>(
    string EntityType,
    Func<TEvent, string?> EntityId,
    Func<TEvent, EventMeta?> Meta,
    Func<string, TEvent, UpsertUnit> Map,
    Func<IReadOnlyCollection<UpsertUnit>, IEnumerable<SoqlQuery>> Queries,
    WriteMode Mode = WriteMode.ReadAndDiff)
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

/// <summary>How a strategy's records are written. Everything before (describe, normalize, record type) and after
/// (per-record results -> outcome per entity, holds, retries) is the same in both modes.</summary>
public enum WriteMode
{
    /// <summary>
    /// Read the records from Salesforce, write only what differs (create or update by what the read found), link
    /// children to parents created in the same request by Composite reference. Two calls per batch; an unchanged
    /// record isn't written at all.
    /// </summary>
    ReadAndDiff,

    /// <summary>
    /// sObject Collections upsert by external id (<c>PATCH /composite/sobjects/{SObject}/{ExternalIdField}</c>): no
    /// read, no diff, every record of the batch sent whole, lookups set by the parent's external id. One call per
    /// object and 200 records. For objects where a write costs little (no meaningful automation, field history or
    /// drift to repair) and API calls matter more.
    /// </summary>
    DirectUpsert
}

/// <summary>An inbox event that passed <see cref="UpsertStrategy{TEvent}.TryRead"/>: the typed event, whose it is and when.</summary>
public sealed record ReadEvent<TEvent>(TEvent Event, string EntityId, DateTimeOffset Time);

/// <summary>Records of one sObject to read from Salesforce, by a field (normally a unique external id).</summary>
public sealed record SoqlQuery(string SObject, string WhereField, IReadOnlyCollection<string> Values)
{
    /// <summary>Reads the units' records of <typeparamref name="TSObject"/> by external id (to diff them).</summary>
    public static SoqlQuery RecordsOf<TSObject>(IEnumerable<UpsertUnit> units) where TSObject : class
    {
        var meta = SObjectMetadata<TSObject>.Instance;
        return new SoqlQuery(meta.SObject, meta.ExternalIdField, ExternalIdsOf(units, meta.SObject));
    }

    /// <summary>Reads the <typeparamref name="TSObject"/> records the units' lookups point at (for their Ids).</summary>
    public static SoqlQuery LookupTargetsOf<TSObject>(IEnumerable<UpsertUnit> units) where TSObject : class
    {
        var meta = SObjectMetadata<TSObject>.Instance;
        return new SoqlQuery(meta.SObject, meta.ExternalIdField, LookupIdsOf(units, meta.SObject));
    }

    /// <summary>The external ids of every record of <paramref name="sobject"/> in the units.</summary>
    public static IReadOnlyCollection<string> ExternalIdsOf(IEnumerable<UpsertUnit> units, string sobject)
    {
        return [.. units.SelectMany(u => u.Records)
                .Where(r => r.SObject.Equals(sobject, StringComparison.OrdinalIgnoreCase))
                .Select(r => r.ExternalId)
                .Distinct(StringComparer.Ordinal)];
    }

    /// <summary>The external ids every lookup of the units points at in <paramref name="sobject"/>.</summary>
    public static IReadOnlyCollection<string> LookupIdsOf(IEnumerable<UpsertUnit> units, string sobject)
    {
        return [.. units.SelectMany(u => u.Records)
                .SelectMany(r => r.Lookups ?? [])
                .Where(l => l.SObject.Equals(sobject, StringComparison.OrdinalIgnoreCase))
                .Select(l => l.ExternalId)
                .Distinct(StringComparer.Ordinal)];
    }
}

/// <summary>What the source says happened to the entity. Deletes are not synced.</summary>
public enum EventType
{
    Inserted,
    Updated
}

/// <summary>
/// The <c>meta</c> object every event carries next to its entity:
/// <c>{ "time": "2026-10-14T01:02:03.456Z", "type": "inserted" }</c>.
/// The time is the event's place in its entity's history: the newest time wins, whatever order events arrive in.
/// The type must be inserted or updated (deletes are not synced); whether a record is created or updated is decided
/// by what Salesforce has.
/// </summary>
public sealed partial class EventMeta
{
    /// <summary>As sent. Kept as text so a time without an offset can be told apart and rejected.</summary>
    public string? Time { get; set; }

    /// <summary>
    /// ISO-8601 date and time with an offset or Z. Nothing else: a time without an offset would be read in whatever
    /// zone the worker runs in, so the same event could be ordered differently on another pod.
    /// </summary>
    [GeneratedRegex(@"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,9})?(Z|[+-]\d{2}:\d{2})$", RegexOptions.CultureInvariant)]
    private static partial Regex IsoTimeWithOffset();

    /// <summary>"inserted" or "updated", in any case. Kept as text so anything else is rejected by name rather than as bad JSON.</summary>
    public string? Type { get; set; }

    /// <summary><see cref="Type"/> as an <see cref="EventType"/>, or null if it is missing or anything else (e.g. "deleted").</summary>
    public EventType? Kind()
    {
        return "inserted".Equals(Type, StringComparison.OrdinalIgnoreCase) ? EventType.Inserted
                : "updated".Equals(Type, StringComparison.OrdinalIgnoreCase) ? EventType.Updated
                : null;
    }

    /// <summary><see cref="Time"/> as an instant, or null if it is missing or not an ISO-8601 time with an offset.</summary>
    public DateTimeOffset? Timestamp()
    {
        return Time is not null && IsoTimeWithOffset().IsMatch(Time) &&
               DateTimeOffset.TryParse(Time, CultureInfo.InvariantCulture, DateTimeStyles.None, out var time)
                ? time
                : null;
    }
}