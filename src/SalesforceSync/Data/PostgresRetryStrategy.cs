using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Npgsql;
using Npgsql.EntityFrameworkCore.PostgreSQL;

namespace SalesforceSync.Data;

/// <summary>
/// EF Core's retrying execution strategy for Npgsql, tuned to what this service needs: it runs a unit of work again,
/// from the start, when it fails with a transient error (a dropped connection, a timeout, a failover or restart, a
/// serialization failure or deadlock). It is used through <c>Database.CreateExecutionStrategy()</c> (see
/// <see cref="DbContextFactoryExtensions"/>), which retries the whole unit (new transaction), never a single statement,
/// so a transaction is never half-applied. If a commit fails, it is unknown whether it went through, so every unit
/// run through it must be safe to run twice; the callers document why theirs are.
/// <para>
/// Compared with Npgsql's default it also retries the connection-class SQLSTATEs (08xxx), backs off from
/// <see cref="PostgresRetryOptions"/> (with jitter), and clears the connection pool when the session itself is gone.
/// </para>
/// </summary>
public sealed class PostgresRetryStrategy(
    ExecutionStrategyDependencies dependencies,
    NpgsqlDataSource db,
    PostgresRetryOptions options)
    : NpgsqlRetryingExecutionStrategy(dependencies, options.MaxRetryCount, options.MaxRetryDelay, null)
{
    /// <summary>Npgsql's own classification, plus server shutdown/restart and connection-class SQLSTATEs.</summary>
    protected override bool ShouldRetryOn(Exception? exception)
    {
        return exception is not null && (base.ShouldRetryOn(exception) || IsTransient(exception));
    }

    /// <summary>
    /// Exponential from <see cref="PostgresRetryOptions.BaseDelay"/>, capped at <see cref="PostgresRetryOptions.MaxRetryDelay"/>,
    /// 50-100% of it (jitter); null once the retries are used up.
    /// </summary>
    protected override TimeSpan? GetNextDelay(Exception lastException)
    {
        var retry = ExceptionsEncountered.Count - 1;
        if (retry >= options.MaxRetryCount)
        {
            return null;
        }

        // When the server restarts, fails over or terminates sessions, the idle connections in the pool are dead too,
        // and Npgsql only finds out when one is used. Without clearing, every retry could draw another dead
        // connection and the retries would run out before reaching the server.
        if (IsConnectionLost(lastException))
        {
            db.Clear();
        }

        var exponential = options.BaseDelay.TotalMilliseconds * Math.Pow(2, retry);
        var capped = Math.Min(exponential, options.MaxRetryDelay.TotalMilliseconds);
        return TimeSpan.FromMilliseconds(capped * (0.5 + (Random.Shared.NextDouble() / 2)));
    }

    /// <summary>
    /// Transient by Npgsql's classification (network errors, timeouts, serialization failures, deadlocks, too many
    /// connections, ...), or a server shutdown/restart or connection-class SQLSTATE.
    /// </summary>
    public static bool IsTransient(Exception ex)
    {
        return ex switch
        {
            // SaveChanges wraps the database's error
            DbUpdateException { InnerException: { } inner } => IsTransient(inner),
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
            DbUpdateException { InnerException: { } inner } => IsConnectionLost(inner),
            PostgresException pg => pg.SqlState.StartsWith("08", StringComparison.Ordinal)
                                    || pg.SqlState is "57P01" or "57P02" or "57P03",
            NpgsqlException => true, // raised by the client, not reported by the server
            _ => false
        };
    }
}

public sealed class PostgresRetryOptions
{
    public const string Section = "Postgres:Retry";

    /// <summary>Retries after the first attempt (0 = no retries).</summary>
    public int MaxRetryCount { get; set; } = 5;

    /// <summary>Delay before the first retry; doubles per retry (with jitter) up to <see cref="MaxRetryDelay"/>.</summary>
    public TimeSpan BaseDelay { get; set; } = TimeSpan.FromMilliseconds(100);

    public TimeSpan MaxRetryDelay { get; set; } = TimeSpan.FromSeconds(5);
}