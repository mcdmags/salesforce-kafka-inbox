using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Npgsql;
using Npgsql.EntityFrameworkCore.PostgreSQL;

namespace SalesforceSync.Data;

/// <summary>
/// EF Core's retrying execution strategy for Npgsql. It reruns a unit of work from the start when it fails with a
/// transient error (dropped connection, timeout, failover or restart, serialization failure or deadlock). Use it
/// through <c>Database.CreateExecutionStrategy()</c> (see <see cref="DbContextFactoryExtensions"/>), which retries the
/// whole unit (new transaction), never a single statement, so a transaction is never half-applied. If a commit fails,
/// it is unknown whether it went through, so every unit run through it must be safe to run twice; callers document why.
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
    /// <summary>Npgsql's own classification, plus every connection-class SQLSTATE.</summary>
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

        // On server restart, failover or session termination, idle pooled connections are dead too, and Npgsql only
        // finds out on use. Without clearing, each retry could draw another dead connection and the retries would run
        // out before reaching the server.
        if (IsConnectionLost(lastException))
        {
            db.Clear();
        }

        var exponential = options.BaseDelay.TotalMilliseconds * Math.Pow(2, retry);
        var capped = Math.Min(exponential, options.MaxRetryDelay.TotalMilliseconds);
        return TimeSpan.FromMilliseconds(capped * (0.5 + (Random.Shared.NextDouble() / 2)));
    }

    /// <summary>
    /// Transient by Npgsql's classification (network errors, timeouts, serialization failures, deadlocks, server
    /// shutdown or restart, too many connections, ...), or any connection-class SQLSTATE.
    /// </summary>
    public static bool IsTransient(Exception ex)
    {
        return ex switch
        {
            // SaveChanges wraps the database's error
            DbUpdateException { InnerException: { } inner } => IsTransient(inner),
            // Npgsql already counts serialization failures, deadlocks, shutdowns and most of class 08, not all of it
            // (08P01, a protocol violation, is not)
            PostgresException pg => pg.IsTransient || pg.SqlState.StartsWith("08", StringComparison.Ordinal),
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