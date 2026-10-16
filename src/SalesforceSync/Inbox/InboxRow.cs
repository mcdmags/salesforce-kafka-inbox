using SalesforceSync.Salesforce;

namespace SalesforceSync.Inbox;

/// <summary>
/// One row of an inbox table: an entity's newest Kafka event, what became of it, and the entity's sync state (when it is
/// due, the lease of the worker processing it, its watermark). One row per entity: a newer event overwrites it. Every
/// inbox table has exactly these columns (the inbox configuration in SyncDbContext), so they all live here; a synced
/// object only adds a subclass with its limits (<see cref="IInboxRow"/>) and gets its own table (<see cref="Data.SyncDbContext"/> maps each subclass to one; the base is not an entity, there is no
/// shared table).
/// </summary>
public abstract class InboxRow
{
    public long Id { get; set; }

    /// <summary>The entity's id, read from the event by the consumer; unique in the table.</summary>
    public string EntityId { get; set; } = "";

    /// <summary>The Kafka message value, unchanged (jsonb); never read by SQL.</summary>
    public string Payload { get; set; } = "";

    /// <summary>
    /// The event's time (its meta.time), to the microsecond as the watermark keeps it; set by the consumer. What
    /// InboxSql.Done moves <see cref="SyncedTime"/> to.
    /// </summary>
    public DateTimeOffset EventTime { get; set; }

    /// <summary>pending | done | superseded | dead. Null on a new row: the database default ('pending') applies.</summary>
    public string Status { get; set; } = null!;

    /// <summary>
    /// Times this event was claimed (a newer event starts again at 0); it is dead-lettered after
    /// <see cref="SyncOptions.MaxRetries"/>.
    /// </summary>
    public int AttemptCount { get; set; }

    public string? LastError { get; set; }

    /// <summary>E.g. FIELD_CUSTOM_VALIDATION_EXCEPTION, to query dead letters by cause.</summary>
    public string? LastErrorCode { get; set; }

    public DateTimeOffset LastUpdated { get; set; }

    /// <summary>
    /// Null: nothing to do (the row is not pending). Else claimable from then on (backoff, deferral or hold included);
    /// a claim leaves it as it is.
    /// </summary>
    public DateTimeOffset? NextAttemptAt { get; set; }

    /// <summary>Lease while a worker processes the entity.</summary>
    public DateTimeOffset? LockedUntil { get; set; }

    /// <summary>Which claim holds the lease.</summary>
    public Guid? LockToken { get; set; }

    /// <summary>
    /// The time of the newest event written to Salesforce, microseconds since the epoch: the watermark. A newer event
    /// overwriting the row keeps it.
    /// </summary>
    public long? SyncedTime { get; set; }
}

/// <summary>
/// An inbox row type: how many records its batches read per query and write per upsert (checked when its sync is
/// registered).
/// </summary>
public interface IInboxRow
{
    /// <summary>
    /// External ids per SOQL query of this type's batch read, at most (1 to
    /// <see cref="SalesforceRestClient.MaxQueryValues"/>: one result page). The URL still wins: a query takes ids only
    /// while its URI and headers fit 16,384 bytes, so long ids give shorter queries.
    /// </summary>
    static abstract int MaxValuesPerQuery { get; }

    /// <summary>
    /// Records of one object per sObject Collections upsert, at most (1 to
    /// <see cref="SalesforceRestClient.MaxCollectionRecords"/>, Salesforce's limit).
    /// </summary>
    static abstract int MaxRecordsPerWrite { get; }
}