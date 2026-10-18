namespace SalesforceSync.Data.Inbox;

/// <summary>
/// One row of an inbox table: an entity's newest Kafka event, what became of it, and the entity's sync state (when it is
/// due, the lease of the worker processing it). One row per entity: a newer event overwrites it. Every inbox table has
/// exactly these columns (the inbox configuration in SyncDbContext), so they all live here; a synced object only adds
/// a subclass with its limits (<see cref="IInboxRow"/>) and gets its own table (<see cref="Data.SyncDbContext"/> maps
/// each subclass to one; the base is not an entity, there is no shared table).
/// </summary>
public abstract class InboxRow
{
    /// <summary>
    /// The entity's id, read from the event by the consumer: the row's primary key. There is no surrogate id: the table
    /// has one row per entity, so the entity id already names the row, and a separate id would only add an index.
    /// </summary>
    public string EntityId { get; set; } = "";

    /// <summary>The entity's event as the consumer split it from its Kafka message (jsonb); never read by SQL.</summary>
    public string Payload { get; set; } = "";

    /// <summary>The event's time (its meta.time), cut to the microsecond (<see cref="ToWholeMicroseconds"/>); set by the consumer.</summary>
    public DateTimeOffset EventTime { get; set; }

    /// <summary>
    /// Times this event failed transiently (a newer event starts again at 0); it is dead-lettered at
    /// <see cref="SyncOptions.MaxRetries"/>. A claim, a release, a deferral, a hold or a lost lease spends none.
    /// </summary>
    public int AttemptCount { get; set; }

    /// <summary>
    /// The latest failure of the event, naming the Salesforce (or sync) error code, e.g. <c>FIELD_CUSTOM_VALIDATION_EXCEPTION</c>
    /// (query dead letters by cause with <c>LIKE</c>). Null once the event is done. An idle row (no <see cref="NextAttemptAt"/>)
    /// with an error is dead.
    /// </summary>
    public string? LastError { get; set; }

    public DateTimeOffset LastUpdated { get; set; }

    /// <summary>
    /// Null: nothing to do (the row is not pending). Else claimable from then on (backoff, deferral or hold included).
    /// A claim moves it to the end of its lease, so a leased row is not due until the lease ends or expires.
    /// </summary>
    public DateTimeOffset? NextAttemptAt { get; set; }

    /// <summary>Which claim holds the lease (until <see cref="NextAttemptAt"/>).</summary>
    public Guid? LockToken { get; set; }

    /// <summary>
    /// An event time as <see cref="EventTime"/> stores it: cut to whole microseconds, in UTC. Postgres keeps microseconds,
    /// and meta.time may have up to 9 fraction digits: cut here, which two events count as equally new (the store
    /// compares their times) never depends on how a finer time would be rounded on its way in.
    /// </summary>
    public static DateTimeOffset ToWholeMicroseconds(DateTimeOffset time)
    {
        var micros = (time - DateTimeOffset.UnixEpoch).Ticks / TimeSpan.TicksPerMicrosecond;
        return DateTimeOffset.UnixEpoch.AddTicks(micros * TimeSpan.TicksPerMicrosecond);
    }
}

/// <summary>
/// An inbox row type: how many records its batches read per query and write per upsert (checked when its sync is
/// registered).
/// </summary>
public interface IInboxRow
{
    /// <summary>
    /// External ids per SOQL query of this type's batch read, at most (1 to
    /// <c>SalesforceRestClient.MaxQueryValues</c>: one result page). The URL still wins: a query takes ids only
    /// while its URI and headers fit 16,384 bytes, so long ids give shorter queries.
    /// </summary>
    static abstract int MaxValuesPerQuery { get; }

    /// <summary>
    /// Records of one object per sObject Collections upsert, at most (1 to
    /// <c>SalesforceRestClient.MaxCollectionRecords</c>, Salesforce's limit).
    /// </summary>
    static abstract int MaxRecordsPerWrite { get; }
}