using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace SalesforceSync.Data.RateLimiting;

/// <summary>
/// Global cap on in-flight Salesforce batches, across all pods. A slot is a lease in the <c>slots</c> column of the shared
/// <c>sf_rate_limit</c> row (<see cref="SlotSql"/>): taking and giving one back are each one short statement, so no
/// connection or transaction is held while the batch runs, and the statements run on whatever context the caller has,
/// scoped or from a factory. If a pod dies holding a slot, the lease simply ends and the slot is free again, as an
/// entity's lease in the inbox does.
/// </summary>
/// <param name="contexts">Where the slot statements get their context (a short one per statement).</param>
/// <param name="maxConcurrent">How many batches may hold a slot at once.</param>
/// <param name="lease">How long a slot is held before it counts as free: longer than a batch takes (the sync's own lease).</param>
/// <param name="log">Where a slot that could not be released says so.</param>
public sealed class SalesforceConcurrencySlots(
    IDbContextFactory<SyncDbContext> contexts,
    int maxConcurrent,
    TimeSpan lease,
    ILogger<SalesforceConcurrencySlots>? log = null)
{
    /// <returns>A held slot, or null when all are busy.</returns>
    public async Task<IAsyncDisposable?> TryAcquireAsync(CancellationToken ct)
    {
        // A fresh token per acquire. The statement removes a token's own lease first, so the execution strategy re-running
        // it after a lost answer takes no second slot
        var token = Guid.NewGuid().ToString("N");
        var taken = await contexts.RunAsync((db, t) => db.Database.ExecuteAsync(SlotSql.Acquire, t,
            new { token, lease, max = maxConcurrent }), ct);
        return taken > 0 ? new Slot(contexts, token, log) : null;
    }

    private sealed class Slot(IDbContextFactory<SyncDbContext> contexts, string token, ILogger? log) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            try
            {
                await contexts.RunAsync((db, t) => db.Database.ExecuteAsync(SlotSql.Release, t, new { token }), CancellationToken.None);
            }
            catch (Exception ex) when (PostgresRetryStrategy.IsTransient(ex))
            {
                // The lease ends by itself
                log?.LogDebug(ex, "Slot {Token} could not be released; its lease will end", token);
            }
        }
    }
}
