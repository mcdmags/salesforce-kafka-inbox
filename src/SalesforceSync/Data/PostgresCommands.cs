using Dapper;

namespace SalesforceSync.Data;

/// <summary>Single statements through <see cref="PostgresExecutionStrategy"/>: own connection, retried on transient errors.</summary>
public static class PostgresCommands
{
    /// <returns>Rows affected.</returns>
    public static Task<int> ExecuteAsync(this PostgresExecutionStrategy pg, string sql, object? args, CancellationToken ct)
    {
        return pg.ExecuteAsync((conn, t) => conn.ExecuteAsync(new CommandDefinition(sql, args, cancellationToken: t)), ct);
    }

    public static Task<T?> ExecuteScalarAsync<T>(this PostgresExecutionStrategy pg, string sql, object? args, CancellationToken ct)
    {
        return pg.ExecuteAsync((conn, t) => conn.ExecuteScalarAsync<T>(new CommandDefinition(sql, args, cancellationToken: t)), ct);
    }
}