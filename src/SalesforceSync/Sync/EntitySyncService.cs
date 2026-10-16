using SalesforceSync.Inbox;

namespace SalesforceSync.Sync;

/// <summary>
/// Syncs one entity type from its inbox table to Salesforce; the worker loops run every registered one in turn.
/// Also what the Kafka consumer needs to store that type's events, without knowing their shape.
/// </summary>
public interface ISalesforceEntitySync
{
    /// <summary>The type's name, e.g. "account".</summary>
    string EntityType { get; }

    /// <summary>The Kafka topic this type's events arrive on, unless configured otherwise (<c>Kafka:Topics:&lt;EntityType&gt;</c>).</summary>
    string Topic { get; }

    /// <summary>
    /// Checks a raw Kafka message value for this type and reads the id of the entity it is about and the event's time.
    /// </summary>
    /// <returns>False with <paramref name="problem"/> if the message can never be synced (to skip it rather than store it).</returns>
    bool TryReadEvent(string json, out string? entityId, out DateTimeOffset eventTime, out string? problem);

    /// <summary>
    /// Stores an event that passed <see cref="TryReadEvent"/> in its entity's row of this type's inbox, unchanged, with
    /// the time it read, unless the row holds a newer event.
    /// </summary>
    Task StoreEventAsync(string entityId, DateTimeOffset eventTime, string json, CancellationToken ct);

    /// <returns>Inbox rows processed (0 = nothing to do).</returns>
    Task<int> SyncBatchAsync(CancellationToken ct);
}

/// <summary>
/// One entity type's sync: its strategy, its topic and its inbox, run by the engine. A synced object supplies only its
/// strategy (names, how to read an event, the mapping and the queries; e.g. <see cref="AccountSync"/>) and its inbox row
/// type, which selects the inbox table; it is registered in <c>AddInboxSync</c>.
/// </summary>
public sealed class EntitySyncService<TEvent, TRow>(
    SalesforceBatchUpsertService engine,
    IRepository<TRow> inbox,
    UpsertStrategy<TEvent> strategy,
    string defaultTopic) : ISalesforceEntitySync
    where TRow : InboxRow, IInboxRow, new()
{
    public string EntityType => strategy.EntityType;

    public string Topic => defaultTopic;

    public bool TryReadEvent(string json, out string? entityId, out DateTimeOffset eventTime, out string? problem)
    {
        var ok = strategy.TryRead(json, out var read, out problem);
        entityId = read?.EntityId;
        eventTime = read?.Time ?? default;
        return ok;
    }

    public Task StoreEventAsync(string entityId, DateTimeOffset eventTime, string json, CancellationToken ct)
    {
        return inbox.AddAsync(entityId, eventTime, json, ct);
    }

    public Task<int> SyncBatchAsync(CancellationToken ct)
    {
        return engine.UpsertBatchAsync(strategy, inbox, ct);
    }
}