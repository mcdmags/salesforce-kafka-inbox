using Dapper;
using Npgsql;
using SalesforceSync.Data;
using SalesforceSync.Sync;

namespace SalesforceSync.Inbox;

public static class InboxSchema
{
    private const string ResourceName = "SalesforceSync.schema.sql";
    // Advisory lock key in the "SF" namespace (see SalesforceConcurrencySlots.KeyBase): ASCII "SFSH", SF schema.
    // Its low 16 bits (0x5348 = 21,320) are far above any concurrency slot index, so the two never collide.
    private const long MigrationLockKey = 0x5346_5348;

    private const string TemplateStart = "-- >>> inbox table template";
    private const string TemplateEnd = "-- <<< inbox table template";
    private const string TablePlaceholder = "{inbox}";

    /// <summary>
    /// sql/schema.sql with its inbox table template (the part between the two marker lines) repeated once per table.
    /// Table names are spliced into the text, so they must be plain identifiers (<see cref="InboxSql.RequireIdentifier"/>).
    /// </summary>
    public static string Script(IEnumerable<string> inboxTables)
    {
        using var stream = typeof(InboxSchema).Assembly.GetManifestResourceStream(ResourceName)
                           ?? throw new InvalidOperationException($"Embedded resource {ResourceName} not found");
        using var reader = new StreamReader(stream);
        var sql = reader.ReadToEnd();

        var start = sql.IndexOf(TemplateStart, StringComparison.Ordinal);
        var end = sql.IndexOf(TemplateEnd, StringComparison.Ordinal);
        if (start < 0 || end < start) throw new InvalidOperationException("schema.sql has no inbox table template");
        var template = sql[start..end];

        var tables = string.Concat(inboxTables.Distinct(StringComparer.Ordinal)
            .Select(t => template.Replace(TablePlaceholder, InboxSql.RequireIdentifier(t))));
        return sql[..start] + tables + sql[(end + TemplateEnd.Length)..];
    }

    /// <summary>Applies the schema on a connection the caller owns (tests: inside a transaction, in a schema of their own).</summary>
    public static async Task ApplyAsync(NpgsqlConnection conn, NpgsqlTransaction? tx, IEnumerable<string> inboxTables, CancellationToken ct = default)
    {
        await conn.ExecuteAsync(new CommandDefinition("select pg_advisory_xact_lock(@key)",
            new { key = MigrationLockKey }, tx, cancellationToken: ct));
        await conn.ExecuteAsync(new CommandDefinition(Script(inboxTables), transaction: tx, cancellationToken: ct));
    }

    /// <summary>
    /// Applies sql/schema.sql with one inbox table per name in <paramref name="inboxTables"/> (idempotent, so safe to
    /// retry). Serialized across processes with an advisory lock. Without an execution strategy, runs once
    /// (benchmarks, tools).
    /// </summary>
    public static async Task EnsureCreatedAsync(NpgsqlDataSource db, IEnumerable<string> inboxTables, CancellationToken ct = default,
        PostgresExecutionStrategy? pg = null)
    {
        var tables = inboxTables.ToList();
        if (pg is not null)
        {
            await pg.ExecuteInTransactionAsync((conn, tx, t) => ApplyAsync(conn, tx, tables, t), ct);
            return;
        }

        await using var conn = await db.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        await ApplyAsync(conn, tx, tables, ct);
        await tx.CommitAsync(ct);
    }
}

/// <summary>
/// Runs before the workers start (hosted services start in registration order): creates the shared tables and one
/// inbox table per registered <see cref="ISalesforceEntitySync"/>.
/// </summary>
public sealed class InboxSchemaInitializer(NpgsqlDataSource db, PostgresExecutionStrategy pg, IServiceScopeFactory scopes,
    ILogger<InboxSchemaInitializer> log) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        string[] tables;
        using (var scope = scopes.CreateScope())
            tables = scope.ServiceProvider.GetServices<ISalesforceEntitySync>().Select(s => s.InboxTable).ToArray();

        await InboxSchema.EnsureCreatedAsync(db, tables, cancellationToken, pg);
        log.LogInformation("Inbox schema is up to date ({Tables})", string.Join(", ", tables));
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
