namespace SalesforceSync.Inbox;

/// <summary>
/// A row of sf_sync_state: one synced entity of any type, whether and when it has events to process, the lease of the
/// worker processing it, and its watermark.
/// </summary>
public sealed class SyncState
{
    /// <summary>The strategy's type name, e.g. "account".</summary>
    public string EntityType { get; set; } = "";

    public string EntityId { get; set; } = "";

    /// <summary>Null: no pending events. Else claimable from then on (backoff, deferral or hold included).</summary>
    public DateTimeOffset? NextAttemptAt { get; set; }

    /// <summary>Lease while a worker processes the entity.</summary>
    public DateTimeOffset? LockedUntil { get; set; }

    /// <summary>Which worker holds the lease.</summary>
    public Guid? LockToken { get; set; }

    /// <summary>The time of the newest event written to Salesforce, microseconds since the epoch: the watermark.</summary>
    public long? SyncedTime { get; set; }
}