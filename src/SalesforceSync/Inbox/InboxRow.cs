namespace SalesforceSync.Inbox;

/// <summary>
/// One row of an inbox table: a Kafka event and what became of it. Every inbox table has exactly these columns (the
/// template in sql/schema.sql), so they all live here; a synced object only adds a subclass that names its table
/// (<see cref="SyncDbContext"/> maps each subclass to its own table; the base is not an entity, there is no shared table).
/// </summary>
public abstract class InboxRow
{
    public long Id { get; set; }

    /// <summary>The Kafka message key (the entity's id); set by the consumer.</summary>
    public string EntityId { get; set; } = "";

    /// <summary>The Kafka message value, unchanged (jsonb); never read by SQL.</summary>
    public string Payload { get; set; } = "";

    /// <summary>pending | done | superseded | dead. Null on a new row: the database default ('pending') applies.</summary>
    public string Status { get; set; } = null!;

    /// <summary>Times this event was claimed; it is dead-lettered after <see cref="SyncOptions.MaxRetries"/>.</summary>
    public int AttemptCount { get; set; }

    public string? LastError { get; set; }

    /// <summary>E.g. FIELD_CUSTOM_VALIDATION_EXCEPTION, to query dead letters by cause.</summary>
    public string? LastErrorCode { get; set; }

    public DateTimeOffset ReceivedAt { get; set; }

    public DateTimeOffset LastUpdated { get; set; }
}