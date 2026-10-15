using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Npgsql;

namespace SalesforceSync.Data;

/// <summary>
/// A unit of work on a context of its own (its own connection), run through EF's execution strategy
/// (<see cref="PostgresRetryStrategy"/>): when it fails with a transient error, the lambda runs again from the start.
/// </summary>
public static class DbContextFactoryExtensions
{
    /// <summary>
    /// Runs <paramref name="unit"/> in a transaction and commits. On a retry, the context's change tracker is cleared and a
    /// new transaction begun, so a unit that tracks entities never re-sends the state of a failed attempt. Anything the
    /// unit keeps outside the lambda (see <c>InboxRepository.ClaimAsync</c>) survives the attempts.
    /// </summary>
    public static async Task<T> RunInTransactionAsync<T>(this IDbContextFactory<SyncDbContext> contexts,
        Func<SyncDbContext, CancellationToken, Task<T>> unit, CancellationToken ct)
    {
        await using var db = await contexts.CreateDbContextAsync(ct);
        var strategy = db.Database.CreateExecutionStrategy();
        return await strategy.ExecuteAsync(async t =>
        {
            db.ChangeTracker.Clear();
            await using var tx = await db.Database.BeginTransactionAsync(t);
            var result = await unit(db, t);
            await tx.CommitAsync(t);
            return result;
        }, ct);
    }

    public static async Task RunInTransactionAsync(this IDbContextFactory<SyncDbContext> contexts,
        Func<SyncDbContext, CancellationToken, Task> unit, CancellationToken ct)
    {
        await contexts.RunInTransactionAsync(async (db, t) =>
        {
            await unit(db, t);
            return true;
        }, ct);
    }

    /// <summary>Runs <paramref name="unit"/>, a single statement or a few that need no transaction, retried as a whole.</summary>
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
/// The statements EF can't express, run as written: named parameters (<c>@name</c>) in the text, an
/// <see cref="NpgsqlParameter"/> each. They join the context's current transaction, if any.
/// </summary>
public static class SqlCommands
{
    public static NpgsqlParameter Param(string name, object? value)
    {
        return new(name, value ?? DBNull.Value);
    }

    /// <returns>Rows affected.</returns>
    public static Task<int> ExecuteAsync(this DatabaseFacade db, string sql, CancellationToken ct, params NpgsqlParameter[] args)
    {
        return db.ExecuteSqlRawAsync(sql, args, ct);
    }

    /// <summary>
    /// The rows of a statement, including UPDATE ... RETURNING. A scalar <typeparamref name="T"/> is the first column; any
    /// other type is matched by column name (case-insensitively) or <c>[Column]</c>. Read as is, never composed with
    /// further LINQ: EF would then wrap the statement in a subquery.
    /// </summary>
    public static Task<List<T>> QueryAsync<T>(this DatabaseFacade db, string sql, CancellationToken ct, params NpgsqlParameter[] args)
    {
        return db.SqlQueryRaw<T>(sql, args).ToListAsync(ct);
    }

    public static async Task<T> QuerySingleAsync<T>(this DatabaseFacade db, string sql, CancellationToken ct, params NpgsqlParameter[] args)
    {
        return (await db.QueryAsync<T>(sql, ct, args)).Single();
    }
}