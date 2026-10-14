namespace SalesforceSync.Inbox;

/// <summary>An entity whose newest event (row <paramref name="EventId"/>) is in Salesforce; its watermark moves to <paramref name="EventTime"/>.</summary>
public sealed record SyncedEntity(string EntityId, long EventTime, long EventId);

/// <summary>An inbox row with the error to store on it.</summary>
public sealed record FailedEvent(long EventId, string Error, string? Code);

/// <summary>
/// What to record for the rows of a batch, by kind of outcome. A row in none of them (an entity not attempted because
/// of another entity's record) stays pending as it is and is read again by the next batch, no retry spent.
/// </summary>
/// <param name="Synced">Done; the entity's watermark advances.</param>
/// <param name="Superseded">Not written: an event of the same entity is newer, or as new was already synced.</param>
/// <param name="Dead">Dead-lettered at once.</param>
/// <param name="Failed">Retried with backoff, dead-lettered after <see cref="SyncOptions.MaxRetries"/>.</param>
public sealed record BatchOutcomes(
    IReadOnlyList<SyncedEntity> Synced,
    IReadOnlyList<long> Superseded,
    IReadOnlyList<FailedEvent> Dead,
    IReadOnlyList<FailedEvent> Failed);