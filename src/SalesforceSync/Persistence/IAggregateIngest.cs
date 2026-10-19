using SalesforceSync.Sync;

namespace SalesforceSync.Persistence;

/// <summary>
/// Takes one entity type's events from each Account aggregate, stores in that type's inbox; Kafka consumer knows no
/// type. Persist half: knows aggregates + inbox, not Salesforce (<see cref="ToSalesforce.IEntitySync"/>).
/// </summary>
public interface IAggregateIngest
{
    string EntityType { get; }

    /// <summary>
    /// Takes this type's events from an aggregate, stores each valid one in its entity's inbox row, unless row holds
    /// a newer event.
    /// </summary>
    /// <returns>One problem per never-syncable (skipped) event; empty if all stored.</returns>
    Task<IReadOnlyList<string>> IngestAsync(AggregateChange change, CancellationToken ct);
}
