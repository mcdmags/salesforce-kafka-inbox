namespace SalesforceSync.ToSalesforce;

/// <summary>
/// Syncs one entity type, inbox table → Salesforce; worker loops run every registered one in turn. Send half: knows
/// inbox + Salesforce, not aggregates or Kafka (that is <see cref="Persistence.IAggregateIngest"/>).
/// </summary>
public interface IEntitySync
{
    string EntityType { get; }

    /// <returns>Inbox rows processed (0 = nothing to do).</returns>
    Task<int> SyncBatchAsync(CancellationToken ct);
}
