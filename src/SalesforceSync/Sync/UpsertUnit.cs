namespace SalesforceSync.Sync;

/// <summary>
/// The records of one entity, written in the same request. Each record is saved or rejected on its own (they are not
/// atomic); the entity counts as synced only when all of them are. A record must come after the records it looks up.
/// </summary>
public sealed record UpsertUnit(IReadOnlyList<SObjectRecord> Records);