using SalesforceSync.Data.Inbox;

namespace SalesforceSync.Sync;

/// <summary>
/// Takes from each Account aggregate the events of one entity type and stores them in that type's inbox: the Kafka
/// consumer needs to know no type. The persist half: it knows aggregates and the inbox, nothing of Salesforce (that is
/// <see cref="IEntitySync"/>).
/// </summary>
public interface IAggregateIngest
{
    /// <summary>The type's name, e.g. "account".</summary>
    string EntityType { get; }

    /// <summary>
    /// Takes the events of this type from an aggregate (<see cref="IAggregateProjection"/>) and stores each valid one in
    /// its entity's row of this type's inbox, unless the row holds a newer event.
    /// </summary>
    /// <returns>One problem per event that can never be synced (and was skipped); empty if all were stored.</returns>
    Task<IReadOnlyList<string>> IngestAsync(AggregateChange change, CancellationToken ct);
}

/// <summary>One entity type's ingest: its projection of an aggregate, the reader that checks what it projected, and its inbox.</summary>
public sealed class EntityIngestService<TEvent, TRow>(
    IAggregateProjection projection,
    EventReader<TEvent> reader,
    IRepository<TRow> inbox) : IAggregateIngest
    where TRow : InboxRow, IInboxRow, new()
{
    public string EntityType => projection.EntityType;

    public async Task<IReadOnlyList<string>> IngestAsync(AggregateChange change, CancellationToken ct)
    {
        List<string> problems = [];
        foreach (var json in projection.ProjectJson(change))
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
