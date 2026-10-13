using Dapper;
using Npgsql;
using SalesforceSync.Data;

namespace SalesforceSync.RateLimiting;

/// <summary>
/// Global cap on in-flight Salesforce batches using session-level advisory locks: a slot is a connection holding
/// one of N lock keys for the duration of a batch. Requires direct or session-mode pooled connections
/// (not PgBouncer transaction mode). If the connection drops, Postgres releases the lock with the session.
/// </summary>
public sealed class SalesforceConcurrencySlots(
    NpgsqlDataSource db,
    PostgresExecutionStrategy pg,
    int maxConcurrent,
    ILogger<SalesforceConcurrencySlots>? log = null)
{
    private const long KeyBase = 0x5346_0000; // "SF" namespace for advisory lock keys

    /// <returns>A held slot, or null when all are busy.</returns>
    public Task<IAsyncDisposable?> TryAcquireAsync(CancellationToken ct)
        => pg.RetryAsync<IAsyncDisposable?>(async token =>
        {
            var conn = await db.OpenConnectionAsync(token);
            try
            {
                foreach (var i in Enumerable.Range(0, maxConcurrent).OrderBy(_ => Random.Shared.Next()))
                {
                    var key = KeyBase + i;
                    if (await conn.ExecuteScalarAsync<bool>(
                            new CommandDefinition("select pg_try_advisory_lock(@key)", new { key }, cancellationToken: token)))
                        return new Slot(conn, key, log);
                }
            }
            catch
            {
                await conn.DisposeAsync(); // the session (and any lock it took) ends with it
                throw;
            }

            await conn.DisposeAsync();
            return null; // all slots busy
        }, ct);

    private sealed class Slot(NpgsqlConnection conn, long key, ILogger? log) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            try
            {
                await conn.ExecuteAsync("select pg_advisory_unlock(@key)", new { key });
            }
            catch (Exception ex) when (PostgresExecutionStrategy.IsTransient(ex) || ex is InvalidOperationException)
            {
                // The connection broke while the batch ran; its session, and so the lock, is already gone
                log?.LogDebug(ex, "Slot {Key} connection was lost; lock released with the session", key);
            }
            finally
            {
                await conn.DisposeAsync();
            }
        }
    }
}
