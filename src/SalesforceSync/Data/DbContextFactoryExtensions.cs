using Microsoft.EntityFrameworkCore;

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
        await contexts.RunInTransactionAsync<bool>(async (db, t) =>
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
        await contexts.RunAsync<bool>(async (db, t) =>
        {
            await unit(db, t);
            return true;
        }, ct);
    }
}
