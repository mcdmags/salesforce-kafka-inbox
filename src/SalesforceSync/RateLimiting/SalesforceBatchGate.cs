namespace SalesforceSync.RateLimiting;

/// <summary>Whether a batch may talk to Salesforce right now.</summary>
public interface ISalesforceBatchGate
{
    /// <returns>False during a pause (rate limits): no batch starts then.</returns>
    Task<bool> IsOpenAsync(CancellationToken ct);
}

/// <summary>No batch starts during the rate limiter's pause.</summary>
public sealed class SalesforceBatchGate(SalesforceRateLimiter limiter) : ISalesforceBatchGate
{
    public async Task<bool> IsOpenAsync(CancellationToken ct)
    {
        return !await limiter.IsPausedAsync(ct);
    }
}