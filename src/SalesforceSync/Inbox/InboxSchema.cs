using Dapper;
using Npgsql;

namespace SalesforceSync.Inbox;

public static class InboxSchema
{
    private const string ResourceName = "SalesforceSync.schema.sql";
    private const long MigrationLockKey = 0x5346_5348; // "SFSH"

    /// <summary>Applies sql/schema.sql (idempotent). Serialized across processes with an advisory lock.</summary>
    public static async Task EnsureCreatedAsync(NpgsqlDataSource db, CancellationToken ct = default)
    {
        string sql;
        await using (var stream = typeof(InboxSchema).Assembly.GetManifestResourceStream(ResourceName)
                                  ?? throw new InvalidOperationException($"Embedded resource {ResourceName} not found"))
        using (var reader = new StreamReader(stream))
        {
            sql = await reader.ReadToEndAsync(ct);
        }

        await using var conn = await db.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        await conn.ExecuteAsync(new CommandDefinition("select pg_advisory_xact_lock(@key)",
            new { key = MigrationLockKey }, tx, cancellationToken: ct));
        await conn.ExecuteAsync(new CommandDefinition(sql, transaction: tx, cancellationToken: ct));
        await tx.CommitAsync(ct);
    }
}

/// <summary>Runs before the workers start (hosted services start in registration order).</summary>
public sealed class InboxSchemaInitializer(NpgsqlDataSource db, ILogger<InboxSchemaInitializer> log) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await InboxSchema.EnsureCreatedAsync(db, cancellationToken);
        log.LogInformation("Inbox schema is up to date");
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
