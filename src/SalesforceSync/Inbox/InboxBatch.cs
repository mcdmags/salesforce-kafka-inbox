using SalesforceSync.Sync;

namespace SalesforceSync.Inbox;

/// <summary>The next batch of an inbox: its rows, and what was already synced for their entities.</summary>
/// <param name="SyncedTimes">Entity id -> the time (microseconds since the epoch) of its newest synced event.</param>
public sealed record InboxBatch(
    string EntityType,
    IReadOnlyList<InboxEvent> Events,
    IReadOnlyDictionary<string, long> SyncedTimes)
{
    public long[] EventIds => [.. Events.Select(e => e.Id)];
}