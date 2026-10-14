namespace SalesforceSync.Data;

/// <summary>A row of <c>sf_sync_state</c>: the newest event time synced to Salesforce for an entity.</summary>
public sealed class SyncStateRow
{
    /// <summary>The strategy's entity type, e.g. 'account'.</summary>
    public string EntityType { get; set; } = "";

    public string EntityId { get; set; } = "";

    /// <summary>The newest synced event's time, microseconds since the epoch.</summary>
    public long SyncedTime { get; set; }

    public DateTimeOffset SyncedAt { get; set; }
}
