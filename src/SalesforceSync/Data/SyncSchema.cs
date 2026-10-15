using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using SalesforceSync.Data.Migrations;
using static SalesforceSync.Data.SqlCommands;

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
    /// What the current schema has: the migration history (@history), else the sync's tables in the shape of
    /// <see cref="InitialSchema"/> (made by sql/schema.sql before there were migrations; sf_sync_state.lock_token came
    /// with its last change), in an older shape, or none. Looked up in the current schema (the search path's first), where
    /// the migrations create their tables (EF's own check for the history table looks in public).
    /// </summary>
    private const string ExistingShape = """
        select case
          when exists (select 1 from information_schema.tables
                       where table_schema = current_schema() and table_name::text = @history) then 'migrated'
          when exists (select 1 from information_schema.columns
                       where table_schema = current_schema() and table_name = 'sf_sync_state' and column_name = 'lock_token') then 'initial'
          when exists (select 1 from information_schema.tables
                       where table_schema = current_schema() and table_name::text = any(@tables)) then 'older'
          else 'none' end
        """;

    /// <summary>
    /// Applies the pending migrations, one process at a time: pods starting together wait for each other on an advisory
    /// lock (session-level, on the context's connection, held for the whole run), and each finds the work done by the
    /// one before. Retried as a whole on transient errors; every step is safe to run again.
    /// </summary>
    /// <exception cref="InvalidOperationException">The database has the sync's tables in a shape from before
    /// <see cref="InitialSchema"/>, which is no longer upgraded.</exception>
    public static async Task MigrateAsync(IDbContextFactory<SyncDbContext> contexts, CancellationToken ct = default)
    {
        await contexts.RunAsync(async (db, t) =>
        {
            await db.Database.OpenConnectionAsync(t); // stays open: the lock is the session's
            try
            {
                await db.Database.ExecuteAsync("select pg_advisory_lock(@key)", t, Param("key", MigrationLockKey));
                await BaselineAsync(db, t);
                await db.Database.MigrateAsync(t);
            }
            finally
            {
                // Explicitly: closing only returns the session to the pool, lock and all
                await db.Database.ExecuteAsync("select pg_advisory_unlock(@key)", CancellationToken.None, Param("key", MigrationLockKey));
                await db.Database.CloseConnectionAsync();
            }
        }, ct);
    }

    /// <summary>
    /// A database created by sql/schema.sql (before there were migrations) has the tables but no migration history.
    /// When its tables have the shape <see cref="InitialSchema"/> creates, that migration is recorded as applied
    /// instead of run; later migrations then apply as usual. Older shapes stop startup: the operator migrates them by
    /// hand or starts from a fresh database.
    /// </summary>
    private static async Task BaselineAsync(SyncDbContext db, CancellationToken ct)
    {
        string[] tables = [.. db.Model.GetEntityTypes().Select(e => e.GetTableName()!), "sf_inbox"];
        var shape = await db.Database.QuerySingleAsync<string>(ExistingShape, ct,
            Param("history", HistoryRepository.DefaultTableName), Param("tables", tables));
        if (shape == "older")
        {
            throw new InvalidOperationException(
                "The database has the sync's tables in a shape from before the InitialSchema migration (e.g. sf_inbox, or inboxes " +
                "with retry_count and an sf_sync_state without lock_token), which is no longer upgraded at startup. Migrate it by " +
                "hand to the shape InitialSchema creates and record that migration in __EFMigrationsHistory, or start from a fresh database.");
        }

        if (shape == "initial")
        {
            var history = db.GetService<IHistoryRepository>();
            var initialSchema = typeof(InitialSchema).GetCustomAttribute<MigrationAttribute>()!.Id;
            await using var tx = await db.Database.BeginTransactionAsync(ct);
            await db.Database.ExecuteAsync(history.GetCreateIfNotExistsScript(), ct);
            await db.Database.ExecuteAsync(history.GetInsertScript(new HistoryRow(initialSchema, ProductInfo.GetVersion())), ct);
            await tx.CommitAsync(ct);
        }
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