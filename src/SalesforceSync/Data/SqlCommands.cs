using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Npgsql;

namespace SalesforceSync.Data;

/// <summary>
/// The statements EF can't express, run as written: named parameters (<c>@name</c>) in the text, an
/// <see cref="NpgsqlParameter"/> each. They join the context's current transaction, if any.
/// </summary>
public static class SqlCommands
{
    public static NpgsqlParameter Param(string name, object? value) => new(name, value ?? DBNull.Value);

    /// <returns>Rows affected.</returns>
    public static Task<int> ExecuteAsync(this DatabaseFacade db, string sql, CancellationToken ct, params NpgsqlParameter[] args)
        => db.ExecuteSqlRawAsync(sql, args, ct);

    /// <summary>
    /// The rows of a statement, including UPDATE ... RETURNING. A scalar <typeparamref name="T"/> is the first column; any
    /// other type is matched by column name (case-insensitively) or <c>[Column]</c>. Read as is, never composed with
    /// further LINQ: EF would then wrap the statement in a subquery.
    /// </summary>
    public static Task<List<T>> QueryAsync<T>(this DatabaseFacade db, string sql, CancellationToken ct, params NpgsqlParameter[] args)
        => db.SqlQueryRaw<T>(sql, args).ToListAsync(ct);

    public static async Task<T> QuerySingleAsync<T>(this DatabaseFacade db, string sql, CancellationToken ct, params NpgsqlParameter[] args)
        => (await db.QueryAsync<T>(sql, ct, args)).Single();
}
