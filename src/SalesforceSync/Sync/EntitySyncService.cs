using SalesforceSync.Inbox;

namespace SalesforceSync.Sync;

/// <summary>
/// What every entity type's sync has in common: it is a strategy, a topic and an inbox, run by the engine. A synced
/// object derives from this and supplies only its strategy (names, how to read an event, the mapping and the queries)
/// and its inbox row type, which selects the inbox table.
/// </summary>
public abstract class EntitySyncService<TEvent, TRow>(
    SalesforceBatchUpsertService engine,
    IRepository<TRow> inbox,
    UpsertStrategy<TEvent> strategy,
    string defaultTopic) : ISalesforceEntitySync
    where TRow : InboxRow, new()
{
    public string EntityType => strategy.EntityType;

    public string Topic => defaultTopic;

    public bool TryReadEvent(string json, out string? entityId, out string? problem)
    {
        var ok = strategy.TryRead(json, out var read, out problem);
        entityId = read?.EntityId;
        return ok;
    }

    public Task StoreEventAsync(string entityId, string json, CancellationToken ct)
    {
        return inbox.AddAsync(entityId, json, ct);
    }

    public Task<int> SyncBatchAsync(CancellationToken ct)
    {
        return engine.UpsertBatchAsync(strategy, inbox, ct);
    }
}