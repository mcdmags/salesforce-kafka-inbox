namespace SalesforceSync.Sync;

public enum SyncOutcome
{
    /// <summary>Salesforce already matches the inbox state; nothing was written.</summary>
    Unchanged,

    /// <summary>The differing fields were written.</summary>
    Saved,

    /// <summary>Not saved because of another entity's record (skipped with it): retry now, no retry spent.</summary>
    NotAttempted,

    /// <summary>May succeed later (lock contention, outage, a lookup target not synced yet): retry with backoff.</summary>
    TransientFailure,

    /// <summary>Retrying the same data fails the same way (validation, unknown or read-only field): dead-letter now.</summary>
    PermanentFailure
}