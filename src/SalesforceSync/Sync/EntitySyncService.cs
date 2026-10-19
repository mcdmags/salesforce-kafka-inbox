using SalesforceSync.Data.Inbox;

namespace SalesforceSync.Sync;

/// <summary>
/// Syncs one entity type from its inbox table to Salesforce; the worker loops run every registered one in turn. The
/// send half: it knows the inbox and Salesforce, nothing of aggregates or Kafka (that is <see cref="IAggregateIngest"/>).
/// </summary>
public interface IEntitySync
{
    /// <summary>The type's name, e.g. "account".</summary>
    string EntityType { get; }

    /// <returns>Inbox rows processed (0 = nothing to do).</returns>
    Task<int> SyncBatchAsync(CancellationToken ct);
}

/// <summary>
/// One entity type's sync: its strategy and its inbox, run by the engine. A synced object supplies only its
/// strategy (how to read an event, the mapping and the queries; e.g. <see cref="AccountSync"/>) and its inbox row
/// type, which selects the inbox table. Registered in <c>AddInboxSync</c>.
/// </summary>
public sealed class EntitySyncService<TEvent, TRow>(
    SalesforceBatchSyncService engine,
    IRepository<TRow> inbox,
    SyncStrategy<TEvent> strategy) : IEntitySync
    where TRow : InboxRow, IInboxRow, new()
{
    public string EntityType => strategy.EntityType;

    public Task<int> SyncBatchAsync(CancellationToken ct)
    {
        return engine.SyncBatchAsync(strategy, inbox, ct);
    }
}
