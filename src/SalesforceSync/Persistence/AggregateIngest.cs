using SalesforceSync.Data.Inbox;
using SalesforceSync.Sync;

namespace SalesforceSync.Persistence;

/// <summary>
/// The ingest of one entity type: what it takes from an aggregate (<see cref="Project"/>), checked with the type's
/// reader and stored in its inbox. A type supplies only those two (e.g. <see cref="AccountIngest"/>) and its inbox row
/// type, which selects the inbox table. Registered in <c>AddInboxSync</c>.
/// </summary>
public abstract class AggregateIngest<TEvent, TRow>(IRepository<TRow> inbox) : IAggregateIngest
    where TRow : InboxRow, IInboxRow, new()
{
    public string EntityType => Reader.EntityType;

    /// <summary>How an event of this type is read: whose it is and when.</summary>
    protected abstract EventReader<TEvent> Reader { get; }

    /// <summary>The events of this type that the change calls for; none if it changes nothing for the type.</summary>
    internal abstract IEnumerable<TEvent> Project(AggregateChange change);

    public async Task<IReadOnlyList<string>> IngestAsync(AggregateChange change, CancellationToken ct)
    {
        List<string> problems = [];
        foreach (var json in Project(change).Select(AggregateProjection.Serialize))
        {
            if (Reader.TryRead(json, out var read, out var problem))
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
