namespace SalesforceSync.Sync;

/// <summary>
/// Syncs one entity type from its inbox table to Salesforce; the worker loop runs every registered one in turn.
/// Also what the Kafka consumer needs to store that type's events, without knowing their shape.
/// </summary>
public interface ISalesforceEntitySync
{
    /// <summary>The type's name, e.g. "account".</summary>
    string EntityType { get; }

    /// <summary>The Kafka topic this type's events arrive on, unless configured otherwise (<c>Kafka:Topics:&lt;EntityType&gt;</c>).</summary>
    string Topic { get; }

    /// <summary>
    /// Checks a raw Kafka message value for this type and reads the id of the entity it is about.
    /// </summary>
    /// <returns>False with <paramref name="problem"/> if the message can never be synced (to skip it rather than store it).</returns>
    bool TryReadEvent(string json, out string? entityId, out string? problem);

    /// <summary>Stores an event that passed <see cref="TryReadEvent"/> as a new row of this type's inbox, unchanged.</summary>
    Task StoreEventAsync(string entityId, string json, CancellationToken ct);

    /// <returns>Inbox rows processed (0 = nothing to do).</returns>
    Task<int> SyncBatchAsync(CancellationToken ct);
}