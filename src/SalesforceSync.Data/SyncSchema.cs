using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace SalesforceSync.Data;

/// <summary>
/// Brings the database to the model of <see cref="SyncDbContext"/> by applying its EF migrations (Data/Migrations).
/// </summary>
public static class SyncSchema
{
    // Advisory lock key in the "SF" namespace (see SalesforceConcurrencySlots.KeyBase): ASCII "SFSH", SF schema.
    // Its low 16 bits (0x5348 = 21,320) are far above any concurrency slot index, so the two never collide.
    private const long MigrationLockKey = 0x5346_5348;

    /// <summary>
    /// Applies the pending migrations, one process at a time: pods starting together wait for each other on an advisory
    /// lock (session-level, on the context's connection, held for the whole run), and each finds the work done by the
    /// one before. Retried as a whole on transient errors; every step is safe to run again.
    /// </summary>
    public static async Task MigrateAsync(IDbContextFactory<SyncDbContext> contexts, CancellationToken ct = default)
    {
        await contexts.RunAsync(async (db, t) =>
        {
            await db.Database.OpenConnectionAsync(t); // stays open: the lock is the session's
            try
            {
                await db.Database.ExecuteAsync("SELECT pg_advisory_lock(@key)", t, new { key = MigrationLockKey });
                await db.Database.MigrateAsync(t);
            }
            finally
            {
                // Explicitly: closing only returns the session to the pool, lock and all
                await db.Database.ExecuteAsync("SELECT pg_advisory_unlock(@key)", CancellationToken.None, new { key = MigrationLockKey });
                await db.Database.CloseConnectionAsync();
            }
        }, ct);
    }
}

/// <summary>
/// Runs before the workers start (hosted services start in registration order): migrates the database
/// (<see cref="SyncSchema.MigrateAsync"/>).
/// </summary>
public sealed class SyncSchemaInitializer(IDbContextFactory<SyncDbContext> contexts, ILogger<SyncSchemaInitializer> log) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await SyncSchema.MigrateAsync(contexts, cancellationToken);
        log.LogInformation("Database schema is up to date");
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}