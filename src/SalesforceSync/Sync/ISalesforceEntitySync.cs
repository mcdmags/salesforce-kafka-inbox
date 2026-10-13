namespace SalesforceSync.Sync;

/// <summary>Syncs one entity type from the inbox to Salesforce; the worker loops run every registered one in turn.</summary>
public interface ISalesforceEntitySync
{
    string EntityType { get; }

    /// <returns>Inbox rows processed (0 = nothing to do).</returns>
    Task<int> SyncBatchAsync(CancellationToken ct);
}
