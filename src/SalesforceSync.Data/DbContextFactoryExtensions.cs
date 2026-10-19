using System.Collections.Concurrent;
using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Npgsql;

namespace SalesforceSync.Data;

/// <summary>
/// Unit of work on own context (own connection), via <see cref="PostgresRetryStrategy"/>: transient error reruns lambda.
/// </summary>
public static class DbContextFactoryExtensions
{
    /// <summary>Run <paramref name="unit"/>, no transaction, retried whole.</summary>
    public static async Task<T> RunAsync<T>(this IDbContextFactory<SyncDbContext> contexts,
        Func<SyncDbContext, CancellationToken, Task<T>> unit, CancellationToken ct)
    {
        await using var db = await contexts.CreateDbContextAsync(ct);
        var strategy = db.Database.CreateExecutionStrategy();
        return await strategy.ExecuteAsync(async t =>
        {
            db.ChangeTracker.Clear();
            return await unit(db, t);
        }, ct);
    }

    public static async Task RunAsync(this IDbContextFactory<SyncDbContext> contexts,
        Func<SyncDbContext, CancellationToken, Task> unit, CancellationToken ct)
    {
        await contexts.RunAsync(async (db, t) =>
        {
            await unit(db, t);
            return true;
        }, ct);
    }
}

/// <summary>
/// Raw SQL EF can't express. <c>@name</c> params bound from object properties (<c>new { ids, token }</c>), one
/// <see cref="NpgsqlParameter"/> each. Join current transaction if any.
/// </summary>
public static class SqlCommands
{
    private static readonly ConcurrentDictionary<Type, PropertyInfo[]> Properties = new();

    public static Task<int> ExecuteAsync(this DatabaseFacade db, string sql, CancellationToken ct, object? args = null)
    {
        return db.ExecuteSqlRawAsync(sql, Params(args), ct);
    }

    /// <summary>
    /// Rows of statement, UPDATE ... RETURNING too. Scalar <typeparamref name="T"/>: first column; else by column name
    /// (case-insensitive) or <c>[Column]</c>. No further LINQ: EF wraps it in subquery.
    /// </summary>
    public static Task<List<T>> QueryAsync<T>(this DatabaseFacade db, string sql, CancellationToken ct, object? args = null)
    {
        return db.SqlQueryRaw<T>(sql, Params(args)).ToListAsync(ct);
    }

    public static async Task<T> QuerySingleAsync<T>(this DatabaseFacade db, string sql, CancellationToken ct, object? args = null)
    {
        return (await db.QueryAsync<T>(sql, ct, args)).Single();
    }

    /// <summary>One param per property of <paramref name="args"/> (new { entityId } binds @entityId).</summary>
    private static NpgsqlParameter[] Params(object? args)
    {
        if (args is null)
        {
            return [];
        }
        var properties = Properties.GetOrAdd(args.GetType(), type => type.GetProperties());
        return [.. properties.Select(p => new NpgsqlParameter(p.Name, p.GetValue(args) ?? DBNull.Value))];
    }
}