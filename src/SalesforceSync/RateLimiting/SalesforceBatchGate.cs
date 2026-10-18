using SalesforceSync.Data.RateLimiting;
using SalesforceSync.Infrastructure.RateLimiting;

namespace SalesforceSync.RateLimiting;

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