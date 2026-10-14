using SalesforceSync.Sync;

namespace SalesforceSync.Inbox;

/// <summary>A batch of entities leased to this worker: their claimed rows, and what was already synced for them.</summary>
/// <param name="Token">Identifies the lease; every outcome statement is conditional on it.</param>
/// <param name="SyncedTimes">Entity id -> the time (microseconds since the epoch) of its newest synced event.</param>
public sealed record InboxClaim(
    string EntityType,
    Guid Token,
    IReadOnlyList<InboxEvent> Events,
    IReadOnlyDictionary<string, long> SyncedTimes)
{
    public long[] EventIds => Events.Select(e => e.Id).ToArray();
}
