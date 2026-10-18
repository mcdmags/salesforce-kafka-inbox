using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Npgsql;
using SalesforceSync.Data;

namespace SalesforceSync.IntegrationTests;

[Collection(SyncCollection.Name)]
public sealed class SchemaTests(SyncFixture app)
{
    private static readonly string[] InboxTables =
        [SyncFixture.InboxTable(), SyncFixture.InboxTable(SyncFixture.Relationships), SyncFixture.InboxTable(SyncFixture.Contacts)];

    [Fact]
    public async Task Startup_applied_every_migration_and_migrating_again_changes_nothing()
    {
        // The worker already migrated at startup; a restart against a persisted volume migrates again
        var before = await TablesAndIndexesAsync();
        before.Should().Contain(["sf_account_inbox", "sf_account_relationship_inbox", "sf_contact_inbox",
            "__EFMigrationsHistory"]);
        before.Should().Contain([
            "sf_account_inbox_pkey", "sf_account_relationship_inbox_pkey", "sf_contact_inbox_pkey",
            "sf_account_relationship_inbox_ready", "sf_contact_inbox_ready"]);
        before.Should().NotContain("sf_sync_state", "every entity's state is in its inbox row");
        before.Should().NotContain(["sf_api_usage", "sf_rate_limit"], "rate limiting lives in each process and reads the daily allocation from Sforce-Limit-Info");
        before.Where(r => r.StartsWith("sf_account_inbox", StringComparison.Ordinal)).Should().BeEquivalentTo(
            ["sf_account_inbox", "sf_account_inbox_pkey", "sf_account_inbox_ready"]);

        await SyncSchema.MigrateAsync(app.Contexts);

        (await TablesAndIndexesAsync()).Should().BeEquivalentTo(before);
        await using var db = await app.Contexts.CreateDbContextAsync();
        (await db.Database.GetAppliedMigrationsAsync()).Should().Equal(db.Database.GetMigrations());
    }

    [Fact]
    public async Task Each_inbox_has_one_row_per_entity_the_ready_index_is_partial_the_hot_tables_vacuum_often()
    {
        await using var conn = await app.Db.OpenConnectionAsync();

        var indexes = (await conn.QueryAsync<(string Name, string Definition)>(
                "SELECT indexname::text, indexdef FROM pg_indexes WHERE schemaname = current_schema()"))
            .ToDictionary(i => i.Name, i => i.Definition);
        foreach (var table in InboxTables)
        {
            // The entity id is the primary key: its one unique index, and there is no other (no separate row id)
            indexes[$"{table}_pkey"].Should().StartWith("CREATE UNIQUE INDEX").And.EndWith($"{table} USING btree (entity_id)");
            indexes[$"{table}_ready"].Should().EndWith($"{table} USING btree (next_attempt_at) WHERE (next_attempt_at IS NOT NULL)");
        }

        foreach (var table in InboxTables)
        {
            (await conn.QuerySingleAsync<string>("SELECT array_to_string(reloptions, ', ') FROM pg_class WHERE oid = @table::regclass", new { table }))
                .Should().Be("autovacuum_vacuum_scale_factor=0.02, autovacuum_vacuum_threshold=1000", table);
        }

    }

    [Fact]
    public async Task Every_inbox_has_the_same_columns_and_of_the_event_only_its_time()
    {
        await using var conn = await app.Db.OpenConnectionAsync();
        async Task<List<string>> Columns(string table)
        {
            return [.. await conn.QueryAsync<string>(
            "SELECT column_name::text || ' ' || data_type FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = @table ORDER BY ordinal_position",
            new { table })];
        }

        var account = await Columns(SyncFixture.InboxTable());
        account.Select(c => c.Split(' ')[0]).Should().Equal(
            "entity_id", "payload", "event_time", "attempt_count", "last_error", "last_updated",
            "next_attempt_at", "lock_token");
        (await Columns(SyncFixture.InboxTable(SyncFixture.Relationships))).Should().Equal(account);
        (await Columns(SyncFixture.InboxTable(SyncFixture.Contacts))).Should().Equal(account);
        (await conn.QuerySingleAsync<long>("SELECT count(*) FROM pg_proc WHERE proname LIKE 'sf\\_event\\_%'"))
            .Should().Be(0, "no SQL function reads inside an event");
    }

    [Fact]
    public async Task The_EF_model_maps_every_column_of_its_tables_with_the_type_the_database_has()
    {
        await using var db = await app.Contexts.CreateDbContextAsync();
        await using var conn = await app.Db.OpenConnectionAsync();

        var mappedByTable = new Dictionary<string, List<(string Name, string Type, bool Nullable)>>();
        foreach (var entity in db.Model.GetEntityTypes())
        {
            var table = entity.GetTableName()!;
            var store = StoreObjectIdentifier.Table(table);
            var mapped = entity.GetProperties()
                .Select(p => (Name: p.GetColumnName(store)!, Type: p.GetColumnType(store), Nullable: p.IsColumnNullable(store)))
                .ToList();
            mappedByTable[table] = mapped;

            var actual = await conn.QueryAsync<(string Name, string Type, string Nullable)>(
                "SELECT column_name::text, data_type::text, is_nullable::text FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = @table",
                new { table });
            actual.Should().NotBeEmpty($"{table} is created by the migrations");
            mapped.Select(c => c.Name).Should().BeEquivalentTo(actual.Select(c => c.Name),
                $"every column of {table} is mapped and the model maps nothing else");
            foreach (var column in mapped)
            {
                var found = actual.Single(c => c.Name == column.Name);
                found.Type.Should().Be(column.Type, $"{table}.{column.Name}");
                (found.Nullable == "YES").Should().Be(column.Nullable, $"{table}.{column.Name} nullability");
            }
        }

        mappedByTable[SyncFixture.InboxTable()].Should().Equal(mappedByTable[SyncFixture.InboxTable(SyncFixture.Relationships)],
            "every inbox has exactly the same fields");
        mappedByTable[SyncFixture.InboxTable()].Should().Equal(mappedByTable[SyncFixture.InboxTable(SyncFixture.Contacts)]);
        db.InboxTables.Should().BeEquivalentTo(InboxTables);
    }

    private async Task<List<string>> TablesAndIndexesAsync()
    {
        await using var conn = await app.Db.OpenConnectionAsync();
        return await TablesAndIndexesAsync(conn);
    }

    private static async Task<List<string>> TablesAndIndexesAsync(NpgsqlConnection conn)
    {
        return [.. await conn.QueryAsync<string>(
                "SELECT c.relname::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = current_schema() AND c.relkind IN ('r', 'i') ORDER BY 1")];
    }
}