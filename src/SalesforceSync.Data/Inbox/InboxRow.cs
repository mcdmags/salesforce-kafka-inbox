namespace SalesforceSync.Data.Inbox;

/// <summary>
/// Columns of every inbox table. One row per entity holding its newest event: a newer event overwrites it, because an
/// event is the full record and only the newest matters to Salesforce. Each subclass is its own table; the base is not
/// mapped, so there is no shared hot table.
/// <para>
/// State without a status column: pending = <see cref="NextAttemptAt"/> set; idle without <see cref="LastError"/> =
/// done; idle with one = dead. A claim's lease is <see cref="NextAttemptAt"/> moved to the lease end, so an expired
/// lease (dead worker) is just due again and nothing has to reset it.
/// </para>
/// </summary>
public abstract class InboxRow
{
    /// <summary>Primary key. No surrogate id: one row per entity, so another id would only add an index.</summary>
    public string EntityId { get; set; } = "";

    /// <summary>jsonb; SQL never reads inside it.</summary>
    public string Payload { get; set; } = "";

    /// <summary>The event's meta.time, cut to the microsecond (<see cref="ToWholeMicroseconds"/>).</summary>
    public DateTimeOffset EventTime { get; set; }

    /// <summary>Transient failures of this event; a newer event resets it. Claims, releases and holds spend none.</summary>
    public int AttemptCount { get; set; }

    public string? LastError { get; set; }

    /// <summary>For operators only; no statement reads it.</summary>
    public DateTimeOffset LastUpdated { get; set; }

    public DateTimeOffset? NextAttemptAt { get; set; }

    /// <summary>
    /// Whole microseconds, UTC. Postgres keeps microseconds but meta.time may have 9 fraction digits; cutting here,
    /// not rounding in Postgres, keeps "same event time" (Kafka redelivery) exact.
    /// </summary>
    public static DateTimeOffset ToWholeMicroseconds(DateTimeOffset time)
    {
        var micros = (time - DateTimeOffset.UnixEpoch).Ticks / TimeSpan.TicksPerMicrosecond;
        return DateTimeOffset.UnixEpoch.AddTicks(micros * TimeSpan.TicksPerMicrosecond);
    }
}
