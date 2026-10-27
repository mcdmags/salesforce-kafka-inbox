using SalesforceSync.Data.Inbox;
using SalesforceSync.Sync;

namespace SalesforceSync.Persistence;

/// <summary>Shared by ingests: validate an aggregate's events, store in type's inbox.</summary>
internal static class InboxIngest
{
    /// <summary>
    /// Stores each reader-accepted event in its entity's inbox row, unless row holds a newer one.
    /// </summary>
    /// <returns>One problem per never-syncable (skipped) event; empty if all stored.</returns>
    public static async Task<IReadOnlyList<string>> StoreAsync<TEvent, TRow>(IEnumerable<TEvent> events, EventReader<TEvent> reader,
        IRepository<TRow> inbox, CancellationToken ct)
        where TRow : InboxRow
    {
        List<string> problems = [];
        foreach (var json in events.Select(AggregateProjection.Serialize))
        {
            if (reader.TryRead(json, out var read, out var problem))
            {
                await inbox.AddAsync(read.EntityId, read.Time, json, ct);
            }
            else
            {
                problems.Add(problem);
            }
        }

        return problems;
    }
}
