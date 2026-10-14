namespace SalesforceSync.Sync;

/// <summary>
/// What every entity type's sync has in common: it is a strategy and a topic, run by the engine. A synced object
/// derives from this and supplies only its strategy (names, how to read an event, the mapping and the queries).
/// </summary>
public abstract class EntitySyncService<TEvent>(
    SalesforceBatchUpsertService engine,
    UpsertStrategy<TEvent> strategy,
    string defaultTopic) : ISalesforceEntitySync
{
    public string EntityType => strategy.EntityType;

    // Explicit: a derived class names its table as a constant called InboxTable
    string ISalesforceEntitySync.InboxTable => strategy.InboxTable;

    public string Topic => defaultTopic;

    public bool TryReadEvent(string json, out string? entityId, out string? problem)
    {
        var ok = strategy.TryRead(json, out var read, out problem);
        entityId = read?.EntityId;
        return ok;
    }

    public Task<int> SyncBatchAsync(CancellationToken ct)
    {
        return engine.UpsertBatchAsync(strategy, ct);
    }
}