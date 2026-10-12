using Dapper;
using Npgsql;

namespace SalesforceSync.RateLimiting;

/// <summary>
/// Global cap on in-flight Salesforce batches using session-level advisory locks.
/// Requires direct or session-mode pooled connections (not PgBouncer transaction mode).
/// </summary>
public sealed class SalesforceConcurrencySlots(NpgsqlDataSource db, int maxConcurrent)
{
    private const long KeyBase = 0x5346_0000; // "SF" namespace for advisory lock keys

    public async Task<IAsyncDisposable?> TryAcquireAsync(CancellationToken ct)
    {
        var conn = await db.OpenConnectionAsync(ct);
        try
        {
            foreach (var i in Enumerable.Range(0, maxConcurrent).OrderBy(_ => Random.Shared.Next()))
            {
                var key = KeyBase + i;
                if (await conn.ExecuteScalarAsync<bool>(
                        new CommandDefinition("select pg_try_advisory_lock(@key)", new { key }, cancellationToken: ct)))
                    return new Slot(conn, key);
            }
        }
        catch
        {
            await conn.DisposeAsync();
            throw;
        }

        await conn.DisposeAsync();
        return null; // all slots busy
    }

    private sealed class Slot(NpgsqlConnection conn, long key) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            try { await conn.ExecuteAsync("select pg_advisory_unlock(@key)", new { key }); }
            finally { await conn.DisposeAsync(); }
        }
    }
}
