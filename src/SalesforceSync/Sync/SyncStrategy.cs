using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;
using SalesforceSync.Infrastructure.Salesforce;

namespace SalesforceSync.Sync;

/// <summary>
/// What varies per synced object: how to read an event's entity id and meta (time and type), what an event asks of
/// Salesforce (the records to upsert, or to delete), and what to read from Salesforce to diff them and find
/// their parents.
/// <see cref="SalesforceBatchSyncService"/> does everything else (claiming, ordering, coalescing, rate limits,
/// batching, the read, the writes and deletes, retries, dead-lettering). The engine, the Kafka consumer and the SQL
/// never name an event field: they go through <paramref name="EntityId"/> and <paramref name="Meta"/>.
/// </summary>
/// <typeparam name="TEvent">The inbox payload (the whole event envelope).</typeparam>
/// <param name="EntityType">The strategy's name, e.g. "account": the object name of its events, and how logs name it.</param>
/// <param name="EntityId">The id of the entity an event is about (e.g. account.externalId), or null if it has none.</param>
/// <param name="Meta">The event's meta, or null if it has none: when the event happened (meta.time, which must have an
/// explicit offset) and what happened (meta.type: inserted, updated or deleted).</param>
/// <param name="Map">
/// The action delegate: inbox event (with its entity id) -> what to do with the entity's records. An inserted or
/// updated event upserts them (whether a record is created or updated is decided by what Salesforce has), a deleted
/// one deletes them (<see cref="SObjectRecord.Deleting{TSObject}"/>, by the entity's id alone). The engine runs
/// only what it is given.
/// </param>
/// <param name="Queries">The batch's units -> the reads (at most 5 after chunking) the diff and the foreign keys need.</param>
public sealed record SyncStrategy<TEvent>(
    string EntityType,
    Func<TEvent, string?> EntityId,
    Func<TEvent, EventMeta?> Meta,
    Func<string, TEvent, SyncUnit> Map,
    Func<IReadOnlyCollection<SyncUnit>, IEnumerable<SoqlQuery>> Queries)
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
            e = JsonSerializer.Deserialize<TEvent>(json, SalesforceBatchSyncService.EventJson);
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

/// <summary>An inbox event that passed <see cref="SyncStrategy{TEvent}.TryRead"/>: the typed event, whose it is and when.</summary>
public sealed record ReadEvent<TEvent>(TEvent Event, string EntityId, DateTimeOffset Time);

/// <summary>Records of one sObject to read from Salesforce, by a field (normally a unique external id).</summary>
public sealed record SoqlQuery(string SObject, string WhereField, IReadOnlyCollection<string> Values)
{
    /// <summary>Reads the units' records of <typeparamref name="TSObject"/> by external id (to diff them).</summary>
    public static SoqlQuery RecordsOf<TSObject>(IEnumerable<SyncUnit> units) where TSObject : class
    {
        var meta = SObjectMetadata<TSObject>.Instance;
        return new SoqlQuery(meta.SObject, meta.ExternalIdField, ExternalIdsOf(units, meta.SObject));
    }

    /// <summary>
    /// Reads the <typeparamref name="TSObject"/> records the units' foreign keys point at, to know that a parent exists
    /// before its child is sent.
    /// </summary>
    public static SoqlQuery ParentsOf<TSObject>(IEnumerable<SyncUnit> units) where TSObject : class
    {
        var meta = SObjectMetadata<TSObject>.Instance;
        return new SoqlQuery(meta.SObject, meta.ExternalIdField, ParentIdsOf(units, meta.SObject));
    }

    /// <summary>The external ids of every record of <paramref name="sobject"/> in the units.</summary>
    public static IReadOnlyCollection<string> ExternalIdsOf(IEnumerable<SyncUnit> units, string sobject)
    {
        return [.. units.SelectMany(u => u.Records)
                .Where(r => r.SObject.Equals(sobject, StringComparison.OrdinalIgnoreCase))
                .Select(r => r.ExternalId)
                .Distinct(StringComparer.Ordinal)];
    }

    /// <summary>The external ids every foreign key of the units points at in <paramref name="sobject"/>.</summary>
    public static IReadOnlyCollection<string> ParentIdsOf(IEnumerable<SyncUnit> units, string sobject)
    {
        return [.. units.SelectMany(u => u.Records)
                .SelectMany(r => r.Parents)
                .Where(l => l.SObject.Equals(sobject, StringComparison.OrdinalIgnoreCase))
                .Select(l => l.ExternalId)
                .Distinct(StringComparer.Ordinal)];
    }
}

/// <summary>
/// The <c>meta</c> object every event carries next to its entity:
/// <c>{ "time": "2026-10-14T01:02:03.456Z", "type": "inserted" }</c>.
/// The time is the event's place in its entity's history: the newest time wins, whatever order events arrive in, a
/// delete like any other. The type is inserted or updated (the same to the sync: what Salesforce has decides
/// create or update), or deleted.
/// </summary>
public sealed partial class EventMeta
{
    /// <summary>As sent. Kept as text so a time without an offset can be told apart and rejected.</summary>
    public string? Time { get; set; }

    /// <summary>
    /// ISO-8601 date and time with an offset or Z. Nothing else: a time without an offset would be read in the
    /// worker's zone, so another pod could order the same event differently.
    /// </summary>
    [GeneratedRegex(@"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,9})?(Z|[+-]\d{2}:\d{2})$", RegexOptions.CultureInvariant)]
    private static partial Regex IsoTimeWithOffset();

    /// <summary>
    /// "inserted", "updated" or "deleted", in any case. Kept as text so anything else is rejected by name, not
    /// as bad JSON.
    /// </summary>
    public string? Type { get; set; }

    /// <summary>
    /// Whether <see cref="Type"/> is inserted or updated. Which of the two decides nothing: Salesforce's state
    /// decides create or update.
    /// </summary>
    public bool IsInsertOrUpdate()
    {
        return "inserted".Equals(Type, StringComparison.OrdinalIgnoreCase)
               || "updated".Equals(Type, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Whether <see cref="Type"/> is deleted: the event's entity is to be deleted.</summary>
    public bool IsDelete()
    {
        return "deleted".Equals(Type, StringComparison.OrdinalIgnoreCase);
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