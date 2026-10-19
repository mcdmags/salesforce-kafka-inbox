namespace SalesforceSync.ToSalesforce;

/// <summary>
/// Syncs one entity type from its inbox table to Salesforce; the worker loops run every registered one in turn. The
/// send half of the sync: it knows the inbox and Salesforce, nothing of aggregates or Kafka (that is
/// <see cref="Persistence.IAggregateIngest"/>).
/// </summary>
public interface IEntitySync
{
    /// <summary>The type's name, e.g. "account".</summary>
    string EntityType { get; }

    /// <returns>Inbox rows processed (0 = nothing to do).</returns>
    Task<int> SyncBatchAsync(CancellationToken ct);
}
