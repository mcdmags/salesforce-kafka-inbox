using SalesforceSync.Sync.Upsert;

namespace SalesforceSync.Inbox;

public sealed class SyncOptions
{
    public const string Section = "Sync";

    /// <summary>Parallel worker loops in this process.</summary>
    public int Workers { get; set; } = 4;

    /// <summary>Max in-flight Salesforce batches across ALL processes.</summary>
    public int MaxConcurrentRequests { get; set; } = 6;

    /// <summary>
    /// Entities per batch, 1 to <see cref="ReadQueryBuilder.MaxEntitiesPerBatch"/> (100): a batch is read with one
    /// Composite request, which carries 5 queries of 100 external ids.
    /// </summary>
    public int BatchSize { get; set; } = 75;

    /// <summary>
    /// Pending events of one entity taken per claim (the oldest first). An event is the full record, so only the
    /// newest of them is written; the bound keeps a deep backlog for one entity from being leased and loaded whole.
    /// What is left is taken by the following claims.
    /// </summary>
    public int MaxEventsPerEntity { get; set; } = 20;

    public TimeSpan Lease { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// How long an entity is left alone after a write to Salesforce whose outcome never arrived (timeout, lost
    /// connection, gateway error): Salesforce may still apply that write, and syncing a newer event before it lands
    /// would let the late write overwrite newer data. The default is Salesforce's API request timeout (10 minutes),
    /// after which the lost request has completed or been stopped.
    /// </summary>
    public TimeSpan UnknownOutcomeHold { get; set; } = TimeSpan.FromMinutes(10);

    /// <summary>Attempts before a transiently failing entity is dead-lettered (permanent errors: immediately).</summary>
    public int MaxRetries { get; set; } = 8;

    /// <summary>Backoff after the first transient failure; doubles per attempt up to <see cref="MaxRetryDelay"/>.</summary>
    public TimeSpan RetryBaseDelay { get; set; } = TimeSpan.FromSeconds(30);

    public TimeSpan MaxRetryDelay { get; set; } = TimeSpan.FromHours(1);
    public TimeSpan IdleDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// What to do when an event's meta.type disagrees with Salesforce: "inserted" but the record exists, or "updated"
    /// but it doesn't. Judged on the entity's main record (the first of its unit).
    /// </summary>
    public EventTypeMismatch EventTypeMismatch { get; set; } = EventTypeMismatch.Reconcile;
}

public enum EventTypeMismatch
{
    /// <summary>Salesforce decides: an existing record is updated with what differs, a missing one is created. Logged.</summary>
    Reconcile,

    /// <summary>The event is dead-lettered with <c>last_error_code = 'EVENT_TYPE_MISMATCH'</c>.</summary>
    DeadLetter
}
