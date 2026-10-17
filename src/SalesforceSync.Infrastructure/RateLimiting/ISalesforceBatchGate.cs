namespace SalesforceSync.Infrastructure.RateLimiting;

/// <summary>Whether a batch may talk to Salesforce right now, across every worker in every pod.</summary>
public interface ISalesforceBatchGate
{
    /// <returns>The admission to dispose when the batch is over, or null if there is no capacity right now.</returns>
    Task<IAsyncDisposable?> TryEnterAsync(CancellationToken ct);
}
