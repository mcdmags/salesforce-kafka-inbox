using Microsoft.Extensions.Options;
using Npgsql;

namespace SalesforceSync.Data;

/// <summary>
/// Runs a unit of work against Postgres and runs it again, from the start, when it fails with a transient error:
/// a dropped connection, a timeout, a failover or restart, a serialization failure or deadlock (the same idea as
/// EF Core's NpgsqlRetryingExecutionStrategy, for plain Npgsql/Dapper).
/// <para>
/// The whole unit is retried (new connection, new transaction), never a single statement, so a transaction is
/// never half-applied. If a commit fails, it is unknown whether it went through, so every unit run through this
/// must be safe to run twice; the callers document why theirs are.
/// </para>
/// </summary>
public sealed class PostgresExecutionStrategy(
    NpgsqlDataSource db,
    IOptions<PostgresRetryOptions> options,
    ILogger<PostgresExecutionStrategy> log)
{
    private readonly PostgresRetryOptions _opt = options.Value;

    /// <summary>Runs <paramref name="operation"/> on its own connection, retrying transient failures.</summary>
    public Task<T> ExecuteAsync<T>(Func<NpgsqlConnection, CancellationToken, Task<T>> operation, CancellationToken ct)
    {
        return RetryAsync(async token =>
            {
                await using var conn = await db.OpenConnectionAsync(token);
                return await operation(conn, token);
            }, ct);
    }

    public Task ExecuteAsync(Func<NpgsqlConnection, CancellationToken, Task> operation, CancellationToken ct)
    {
        return ExecuteAsync(async (conn, token) =>
            {
                await operation(conn, token);
                return true;
            }, ct);
    }

    /// <summary>Runs <paramref name="operation"/> in a transaction and commits; the whole unit is retried on transient failures.</summary>
    public Task<T> ExecuteInTransactionAsync<T>(
        Func<NpgsqlConnection, NpgsqlTransaction, CancellationToken, Task<T>> operation, CancellationToken ct)
    {
        return RetryAsync(async token =>
            {
                await using var conn = await db.OpenConnectionAsync(token);
                await using var tx = await conn.BeginTransactionAsync(token);
                var result = await operation(conn, tx, token);
                await tx.CommitAsync(token);
                return result;
            }, ct);
    }

    public Task ExecuteInTransactionAsync(Func<NpgsqlConnection, NpgsqlTransaction, CancellationToken, Task> operation, CancellationToken ct)
    {
        return ExecuteInTransactionAsync(async (conn, tx, token) =>
            {
                await operation(conn, tx, token);
                return true;
            }, ct);
    }

    /// <summary>The retry loop itself, for operations that manage their own connection.</summary>
    public async Task<T> RetryAsync<T>(Func<CancellationToken, Task<T>> attempt, CancellationToken ct)
    {
        for (var retry = 0; ; retry++)
        {
            try
            {
                return await attempt(ct);
            }
            catch (Exception ex) when (retry < _opt.MaxRetryCount && IsTransient(ex) && !ct.IsCancellationRequested)
            {
                // When the server restarts, fails over or terminates sessions, the idle connections in the pool are
                // dead too, and Npgsql only finds out when one is used. Without clearing, every retry could draw
                // another dead connection and the retries would run out before reaching the server.
                if (IsConnectionLost(ex))
                {
                    db.Clear();
                }

                var delay = Delay(retry);
                log.LogWarning("Transient Postgres error ({Error}); retry {Retry}/{Max} in {Delay}",
                    ex.Message, retry + 1, _opt.MaxRetryCount, delay);
                await Task.Delay(delay, ct);
            }
        }
    }

    /// <summary>
    /// Npgsql's own classification (network errors, timeouts, serialization failures, deadlocks, too many
    /// connections, ...), plus server shutdown/restart and connection-class SQLSTATEs.
    /// </summary>
    public static bool IsTransient(Exception ex)
    {
        return ex switch
        {
            PostgresException pg => pg.IsTransient
                                    || pg.SqlState.StartsWith("08", StringComparison.Ordinal)   // connection exception
                                    || pg.SqlState is "57P01" or "57P02" or "57P03"             // admin/crash shutdown, cannot connect now
                                    || pg.SqlState is "40001" or "40P01",                       // serialization failure, deadlock
            NpgsqlException npgsql => npgsql.IsTransient,
            TimeoutException => true,
            _ => false
        };
    }

    /// <summary>
    /// The session itself is gone (not just the statement or transaction failing): a connection-class SQLSTATE,
    /// a server shutdown, or an I/O error or timeout on the client side.
    /// </summary>
    public static bool IsConnectionLost(Exception ex)
    {
        return ex switch
        {
            PostgresException pg => pg.SqlState.StartsWith("08", StringComparison.Ordinal)
                                    || pg.SqlState is "57P01" or "57P02" or "57P03",
            NpgsqlException => true, // raised by the client, not reported by the server
            _ => false
        };
    }

    private TimeSpan Delay(int retry)
    {
        var exponential = _opt.BaseDelay.TotalMilliseconds * Math.Pow(2, retry);
        var capped = Math.Min(exponential, _opt.MaxRetryDelay.TotalMilliseconds);
        return TimeSpan.FromMilliseconds(capped * (0.5 + (Random.Shared.NextDouble() / 2))); // jitter: 50-100%
    }
}