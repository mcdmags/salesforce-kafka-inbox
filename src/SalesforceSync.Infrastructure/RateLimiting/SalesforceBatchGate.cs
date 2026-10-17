namespace SalesforceSync.Infrastructure.RateLimiting;

/// <summary>Whether a batch may talk to Salesforce right now, across every worker in every pod.</summary>
public interface ISalesforceBatchGate
{
    /// <returns>The admission to dispose when the batch is over, or null if there is no capacity right now.</returns>
    Task<IAsyncDisposable?> TryEnterAsync(CancellationToken ct);
}

/// <summary>A batch runs while it holds a global concurrency slot, and none starts during a shared pause (rate limits).</summary>
public sealed class SalesforceBatchGate(SalesforceConcurrencySlots slots, SalesforceRateLimiter limiter) : ISalesforceBatchGate
{
    public async Task<IAsyncDisposable?> TryEnterAsync(CancellationToken ct)
    {
        var slot = await slots.TryAcquireAsync(ct);
        if (slot is null)
        {
            return null;
        }

        bool paused;
        try
        {
            paused = await limiter.IsPausedAsync(ct);
        }
        catch
        {
            await slot.DisposeAsync();
            throw;
        }
        if (!paused)
        {
            return slot;
        }

        await slot.DisposeAsync();
        return null;
    }
}