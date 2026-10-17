using Microsoft.EntityFrameworkCore;
using SalesforceSync.Data;

namespace SalesforceSync.RateLimiting;

/// <summary>
/// Global cap on in-flight Salesforce batches using session-level advisory locks: a slot is a connection holding
/// one of N lock keys for the duration of a batch. Requires direct or session-mode pooled connections
/// (not PgBouncer transaction mode). If the connection drops, Postgres releases the lock with the session.
/// The connection is a context's, opened explicitly and held (not returned to the pool) until the slot is disposed.
/// </summary>
public sealed class SalesforceConcurrencySlots(
    IDbContextFactory<SyncDbContext> contexts,
    int maxConcurrent,
    ILogger<SalesforceConcurrencySlots>? log = null)
{
    // Advisory lock keys share one number space per database with any other code that uses them, so ours sit in
    // a distinctive range instead of small numbers like 1 or 42 that others pick:
    // - high 16 bits 0x5346 are ASCII "SF" (Salesforce), the namespace; SyncSchema's migration lock is 0x5346_5348 "SFSH";
    // - low 16 bits are the slot index, key = KeyBase + i (up to 65,536 slots; the org limit is at most 25);
    // - the key fits in 32 bits, so pg_locks shows classid = 0 and objid = the key (1397096448 + slot).
    // The value is a convention: it must only be identical in every pod and unused by other applications.
    private const long KeyBase = 0x5346_0000;

    /// <summary>
    /// A retry (a transient failure while taking a slot) starts on a fresh session: the attempt's connection is closed
    /// and opened again, so a lock taken by the failed attempt is gone with its session.
    /// </summary>
    /// <returns>A held slot, or null when all are busy.</returns>
    public async Task<IAsyncDisposable?> TryAcquireAsync(CancellationToken ct)
    {
        var db = await contexts.CreateDbContextAsync(ct);
        try
        {
            var strategy = db.Database.CreateExecutionStrategy();
            var key = await strategy.ExecuteAsync(async t =>
            {
                await db.Database.CloseConnectionAsync();
                await db.Database.OpenConnectionAsync(t); // stays open: EF would otherwise release it after each statement
                foreach (var i in Enumerable.Range(0, maxConcurrent).OrderBy(_ => Random.Shared.Next()))
                {
                    var candidate = KeyBase + i;
                    if (await db.Database.QuerySingleAsync<bool>("SELECT pg_try_advisory_lock(@key)", t, new { key = candidate }))
                    {
                        return candidate;
                    }
                }
                return (long?)null; // all slots busy
            }, ct);

            if (key is { } held)
            {
                return new Slot(db, held, log);
            }
        }
        catch
        {
            await db.DisposeAsync(); // the session (and any lock it took) ends with it
            throw;
        }

        await db.DisposeAsync();
        return null;
    }

    private sealed class Slot(SyncDbContext db, long key, ILogger? log) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            try
            {
                await db.Database.ExecuteAsync("SELECT pg_advisory_unlock(@key)", CancellationToken.None, new { key });
            }
            catch (Exception ex) when (PostgresRetryStrategy.IsTransient(ex) || ex is InvalidOperationException)
            {
                // The connection broke while the batch ran; its session, and so the lock, is already gone
                log?.LogDebug(ex, "Slot {Key} connection was lost; lock released with the session", key);
            }
            finally
            {
                await db.DisposeAsync();
            }
        }
    }
}