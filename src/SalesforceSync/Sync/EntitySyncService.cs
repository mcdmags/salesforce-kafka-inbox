using SalesforceSync.Data.Inbox;

namespace SalesforceSync.Sync;

/// <summary>
/// Syncs one entity type from its inbox table to Salesforce; the worker loops run every registered one in turn.
/// Also takes from each Account aggregate the events of its type and stores them in its inbox: the Kafka consumer
/// needs to know no type.
/// </summary>
public interface ISalesforceEntitySync
{
    /// <summary>The type's name, e.g. "account".</summary>
    string EntityType { get; }

    /// <summary>
    /// Checks an event of this type and reads the id of the entity it is about and the event's time.
    /// </summary>
    /// <returns>False with <paramref name="problem"/> if the message can never be synced (to skip it rather than store it).</returns>
    bool TryReadEvent(string json, out string? entityId, out DateTimeOffset eventTime, out string? problem);

    /// <summary>
    /// Takes the events of this type from an aggregate (<see cref="IAggregateProjection"/>) and stores each valid one in
    /// its entity's row of this type's inbox, unless the row holds a newer event.
    /// </summary>
    /// <returns>One problem per event that can never be synced (and was skipped); empty if all were stored.</returns>
    Task<IReadOnlyList<string>> IngestAsync(AggregateChange change, CancellationToken ct);

    /// <summary>
    /// Stores an event that passed <see cref="TryReadEvent"/> in its entity's row of this type's inbox, unchanged, with
    /// the time it read, unless the row holds a newer event.
    /// </summary>
    Task StoreEventAsync(string entityId, DateTimeOffset eventTime, string json, CancellationToken ct);

    /// <returns>Inbox rows processed (0 = nothing to do).</returns>
    Task<int> SyncBatchAsync(CancellationToken ct);
}

/// <summary>
/// One entity type's sync: its strategy and its inbox, run by the engine. A synced object supplies only its
/// strategy (names, how to read an event, the mapping and the queries; e.g. <see cref="AccountSync"/>) and its inbox row
/// type, which selects the inbox table. Registered in <c>AddInboxSync</c>.
/// </summary>
public sealed class EntitySyncService<TEvent, TRow>(
    SalesforceBatchSyncService engine,
    IRepository<TRow> inbox,
    SyncStrategy<TEvent> strategy) : ISalesforceEntitySync
    where TRow : InboxRow, IInboxRow, new()
{
    public string EntityType => strategy.EntityType;

    public bool TryReadEvent(string json, out string? entityId, out DateTimeOffset eventTime, out string? problem)
    {
        var ok = strategy.TryRead(json, out var read, out problem);
        entityId = read?.EntityId;
        eventTime = read?.Time ?? default;
        return ok;
    }

    public async Task<IReadOnlyList<string>> IngestAsync(AggregateChange change, CancellationToken ct)
    {
        List<string> problems = [];
        foreach (var json in strategy.ProjectJson(change))
        {
            if (strategy.TryRead(json, out var read, out var problem))
            {
                await inbox.AddAsync(read.EntityId, read.Time, json, ct);
            }
            else
            {
                problems.Add(problem);
            }
        }

        return problems;
    }

    public Task StoreEventAsync(string entityId, DateTimeOffset eventTime, string json, CancellationToken ct)
    {
        return inbox.AddAsync(entityId, eventTime, json, ct);
    }

    public Task<int> SyncBatchAsync(CancellationToken ct)
    {
        return engine.SyncBatchAsync(strategy, inbox, ct);
    }
}