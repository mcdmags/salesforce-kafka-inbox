using SalesforceSync.Data.Inbox;
using SalesforceSync.Sync;

namespace SalesforceSync.Persistence;

/// <summary>What the types' ingests share: checking the events of an aggregate and storing them in the type's inbox.</summary>
internal static class InboxIngest
{
    /// <summary>
    /// Stores each event the type's reader accepts in its entity's row of the inbox, unless the row holds a newer event.
    /// </summary>
    /// <returns>One problem per event that can never be synced (and was skipped); empty if all were stored.</returns>
    public static async Task<IReadOnlyList<string>> StoreAsync<TEvent, TRow>(IEnumerable<TEvent> events, EventReader<TEvent> reader,
        IRepository<TRow> inbox, CancellationToken ct)
        where TRow : InboxRow, IInboxRow, new()
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
