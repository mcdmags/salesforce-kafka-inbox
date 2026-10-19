using SalesforceSync.Sync;

namespace SalesforceSync.Persistence;

/// <summary>
/// Takes from each Account aggregate the events of one entity type and stores them in that type's inbox: the Kafka
/// consumer needs to know no type. The persist half of the sync: it knows aggregates and the inbox, nothing of
/// Salesforce (that is <see cref="ToSalesforce.IEntitySync"/>).
/// </summary>
public interface IAggregateIngest
{
    /// <summary>The type's name, e.g. "account".</summary>
    string EntityType { get; }

    /// <summary>
    /// Takes the events of this type from an aggregate and stores each valid one in its entity's row of this type's
    /// inbox, unless the row holds a newer event.
    /// </summary>
    /// <returns>One problem per event that can never be synced (and was skipped); empty if all were stored.</returns>
    Task<IReadOnlyList<string>> IngestAsync(AggregateChange change, CancellationToken ct);
}
