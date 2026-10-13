using Dapper;
using Npgsql;
using SalesforceSync.Data;

namespace SalesforceSync.Inbox;

public static class InboxSchema
{
    private const string ResourceName = "SalesforceSync.schema.sql";
    // Advisory lock key: ASCII "SFSH" (SF schema), a distinctive value rather than a small number like 1 or 42 that
    // other code sharing the database might pick.
    private const long MigrationLockKey = 0x5346_5348;

    /// <summary>
    /// Applies sql/schema.sql (idempotent, so safe to retry). Serialized across processes with an advisory lock.
    /// Without an execution strategy, runs once (benchmarks, tools).
    /// </summary>
    public static async Task EnsureCreatedAsync(NpgsqlDataSource db, CancellationToken ct = default, PostgresExecutionStrategy? pg = null)
    {
        string sql;
        await using (var stream = typeof(InboxSchema).Assembly.GetManifestResourceStream(ResourceName)
                                  ?? throw new InvalidOperationException($"Embedded resource {ResourceName} not found"))
        using (var reader = new StreamReader(stream))
        {
            sql = await reader.ReadToEndAsync(ct);
        }

        async Task Apply(NpgsqlConnection conn, NpgsqlTransaction tx, CancellationToken t)
        {
            await conn.ExecuteAsync(new CommandDefinition("select pg_advisory_xact_lock(@key)",
                new { key = MigrationLockKey }, tx, cancellationToken: t));
            await conn.ExecuteAsync(new CommandDefinition(sql, transaction: tx, cancellationToken: t));
        }

        if (pg is not null)
        {
            await pg.ExecuteInTransactionAsync(Apply, ct);
            return;
        }

        await using var conn = await db.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        await Apply(conn, tx, ct);
        await tx.CommitAsync(ct);
    }
}

/// <summary>Runs before the workers start (hosted services start in registration order).</summary>
public sealed class InboxSchemaInitializer(NpgsqlDataSource db, PostgresExecutionStrategy pg,
    ILogger<InboxSchemaInitializer> log) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await InboxSchema.EnsureCreatedAsync(db, cancellationToken, pg);
        log.LogInformation("Inbox schema is up to date");
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
