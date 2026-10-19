using System.Collections.Concurrent;
using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Npgsql;

namespace SalesforceSync.Data;

/// <summary>
/// Unit of work on its own context (own connection), run through EF's execution strategy
/// (<see cref="PostgresRetryStrategy"/>): on a transient error the lambda reruns from the start.
/// </summary>
public static class DbContextFactoryExtensions
{
    /// <summary>
    /// Runs <paramref name="unit"/> in a transaction and commits. On retry, clears the change tracker and begins a
    /// new transaction, so a unit that tracks entities never re-sends a failed attempt's state. State kept outside the
    /// lambda (see <c>InboxRepository.ClaimAsync</c>) survives attempts.
    /// </summary>
    public static async Task<T> RunInTransactionAsync<T>(this IDbContextFactory<SyncDbContext> contexts,
        Func<SyncDbContext, CancellationToken, Task<T>> unit, CancellationToken ct)
    {
        await using var db = await contexts.CreateDbContextAsync(ct);
        // A failed SaveChanges fails the unit, which retries from the start. A savepoint would add two round trips
        // (SAVEPOINT, RELEASE) to every SaveChanges. Reset when the context returns to the pool.
        db.Database.AutoSavepointsEnabled = false;
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
/// The statements EF can't express, run as written: named parameters (<c>@name</c>) in the text, bound from the
/// properties of an object (<c>new { ids, token }</c>), one <see cref="NpgsqlParameter"/> each. They join the
/// context's current transaction, if any.
/// </summary>
public static class SqlCommands
{
    private static readonly ConcurrentDictionary<Type, PropertyInfo[]> Properties = new();

    /// <returns>Rows affected.</returns>
    public static Task<int> ExecuteAsync(this DatabaseFacade db, string sql, CancellationToken ct, object? args = null)
    {
        return db.ExecuteSqlRawAsync(sql, Params(args), ct);
    }

    /// <summary>
    /// Rows of a statement, including UPDATE ... RETURNING. Scalar <typeparamref name="T"/> is the first column; any
    /// other type matches by column name (case-insensitive) or <c>[Column]</c>. Read as is, never compose with
    /// further LINQ: EF would wrap the statement in a subquery.
    /// </summary>
    public static Task<List<T>> QueryAsync<T>(this DatabaseFacade db, string sql, CancellationToken ct, object? args = null)
    {
        return db.SqlQueryRaw<T>(sql, Params(args)).ToListAsync(ct);
    }

    public static async Task<T> QuerySingleAsync<T>(this DatabaseFacade db, string sql, CancellationToken ct, object? args = null)
    {
        return (await db.QueryAsync<T>(sql, ct, args)).Single();
    }

    /// <summary>One parameter per property of <paramref name="args"/>, named after it (new { entityId } binds @entityId).</summary>
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