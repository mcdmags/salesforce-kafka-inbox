namespace SalesforceSync.Data.Inbox;

public sealed class SyncOptions
{
    public const string Section = "Sync";

    /// <summary>
    /// Parallel worker loops in this process. Each has one Salesforce call in flight, so the org sees at most this
    /// many times the pod count: keep that under the org's concurrent long-running request limit
    /// (<c>SalesforceLimits:ConcurrentLongRunningLimit</c>, 25); nothing caps it across pods. A pod that meets the
    /// limit pauses itself and defers its batch without spending an attempt.
    /// </summary>
    public int Workers { get; set; } = 4;

    /// <summary>
    /// Entities per batch, 1 to <see cref="MaxEntitiesPerBatch"/>. A batch is read with one query per object (up to
    /// 200 external ids each; more or very long ids split the read over more queries) and written with one upsert and
    /// one delete call per object.
    /// </summary>
    public int BatchSize { get; set; } = 180;

    /// <summary>
    /// Most entities one batch may hold; the ceiling of <see cref="BatchSize"/>. Not a Salesforce limit: at 200 ids per
    /// query and 200 records per write call, 200 entities take one call per object when each needs one record of it (a
    /// relationship reads up to 3 ids: itself and two Accounts). Raising it is safe: reads and writes split into more
    /// calls of 200.
    /// </summary>
    public int MaxEntitiesPerBatch { get; set; } = 200;

    /// <summary>
    /// How long a claim owns its entities (until the inbox row's next_attempt_at). Between claim transaction and
    /// outcome transaction nothing else holds them: no transaction or row lock spans the Salesforce calls. The worker
    /// stops calling Salesforce at 80% of it. A crashed worker needs no cleanup: the lease expires and the entities
    /// are claimable again. Must be longer than <c>SalesforceLimits:RequestTimeout</c>.
    /// </summary>
    public TimeSpan Lease { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// How long an entity is left alone after a write to Salesforce whose outcome never arrived (timeout, lost
    /// connection, gateway error). Salesforce may still apply that write, and syncing a newer event first would let
    /// the late write overwrite newer data. Default is Salesforce's API request timeout (10 minutes), after which the
    /// lost request has completed or been stopped.
    /// </summary>
    public TimeSpan UnknownOutcomeHold { get; set; } = TimeSpan.FromMinutes(10);

    /// <summary>
    /// How long an inbox stops after a mapping error (a field the code sends that Salesforce doesn't have or won't
    /// take): time to deploy the fix or add the field. Due rows wait this long, then a claim tries again and stops the
    /// inbox again if the error persists. Nothing is dead-lettered meanwhile.
    /// </summary>
    public TimeSpan MappingErrorHold { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// How long an inbox stops when Salesforce's data storage is full (STORAGE_LIMIT_EXCEEDED): time for an admin to
    /// free space. Due rows wait this long, then a claim tries again and stops the inbox again if storage is still
    /// full. No attempt is spent and nothing is dead-lettered meanwhile.
    /// </summary>
    public TimeSpan StorageFullHold { get; set; } = TimeSpan.FromMinutes(15);

    /// <summary>
    /// Transient failures of an entity's event (its row's attempt_count, reset by each newer event) at which it is
    /// dead-lettered (permanent errors: immediately).
    /// </summary>
    public int MaxRetries { get; set; } = 8;

    /// <summary>
    /// The entity's backoff after the first transient failure; doubles per failure (its row's attempt_count) up to
    /// <see cref="MaxRetryDelay"/>.
    /// </summary>
    public TimeSpan RetryBaseDelay { get; set; } = TimeSpan.FromSeconds(30);

    public TimeSpan MaxRetryDelay { get; set; } = TimeSpan.FromHours(1);

    public TimeSpan IdleDelay { get; set; } = TimeSpan.FromSeconds(1);
}
