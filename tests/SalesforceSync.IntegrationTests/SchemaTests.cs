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
        before.Should().Contain(["sf_account_inbox", "sf_account_relationship_inbox", "sf_contact_inbox", "sf_sync_state", "sf_rate_limit",
            "sf_api_usage", "__EFMigrationsHistory"]);
        before.Should().Contain([
            "sf_account_inbox_pending", "sf_account_inbox_pkey", "sf_account_relationship_inbox_pending", "sf_contact_inbox_pending",
            "sf_sync_state_pkey", "sf_sync_state_ready"]);
        before.Where(r => r.StartsWith("sf_account_inbox", StringComparison.Ordinal)).Should().BeEquivalentTo(
            ["sf_account_inbox", "sf_account_inbox_pkey", "sf_account_inbox_pending"], "the claim's state is in sf_sync_state, not indexed here");

        await SyncSchema.MigrateAsync(app.Contexts);

        (await TablesAndIndexesAsync()).Should().BeEquivalentTo(before);
        await using var db = await app.Contexts.CreateDbContextAsync();
        (await db.Database.GetAppliedMigrationsAsync()).Should().Equal(db.Database.GetMigrations());
    }

    [Fact]
    public async Task The_claims_indexes_are_partial_the_hot_tables_vacuum_often_and_the_rate_limit_row_and_view_exist()
    {
        await using var conn = await app.Db.OpenConnectionAsync();

        var indexes = (await conn.QueryAsync<(string Name, string Definition)>(
                "select indexname::text, indexdef from pg_indexes where schemaname = current_schema()"))
            .ToDictionary(i => i.Name, i => i.Definition);
        foreach (var table in InboxTables)
        {
            indexes[$"{table}_pending"].Should().EndWith($"{table} USING btree (entity_id, id) WHERE (status = 'pending'::text)");
        }
        indexes["sf_sync_state_ready"].Should().EndWith("sf_sync_state USING btree (entity_type, next_attempt_at) WHERE (next_attempt_at IS NOT NULL)");

        foreach (var table in InboxTables.Append("sf_sync_state"))
        {
            (await conn.QuerySingleAsync<string>("select array_to_string(reloptions, ', ') from pg_class where oid = @table::regclass", new { table }))
                .Should().Be("autovacuum_vacuum_scale_factor=0.02, autovacuum_vacuum_threshold=1000", table);
        }

        (await conn.QuerySingleAsync<double>("select capacity from sf_rate_limit where name = 'salesforce'"))
            .Should().BeGreaterThan(0, "the migration creates the shared bucket's row (startup then configures it)");
        (await conn.ExecuteScalarAsync<long>("select count(*) from sf_limit_status")).Should().Be(1);
    }

    [Fact]
    public async Task Every_inbox_has_the_same_columns_and_none_for_the_event()
    {
        await using var conn = await app.Db.OpenConnectionAsync();
        async Task<List<string>> Columns(string table)
        {
            return [.. await conn.QueryAsync<string>(
            "select column_name::text || ' ' || data_type from information_schema.columns where table_schema = current_schema() and table_name = @table order by ordinal_position",
            new { table })];
        }

        var account = await Columns(SyncFixture.InboxTable());
        account.Select(c => c.Split(' ')[0]).Should().Equal(
            "id", "entity_id", "payload", "status", "attempt_count", "last_error", "last_error_code", "received_at", "last_updated");
        (await Columns(SyncFixture.InboxTable(SyncFixture.Relationships))).Should().Equal(account);
        (await Columns(SyncFixture.InboxTable(SyncFixture.Contacts))).Should().Equal(account);
        (await conn.ExecuteScalarAsync<long>("select count(*) from pg_proc where proname like 'sf\\_event\\_%'"))
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
                "select column_name::text, data_type::text, is_nullable::text from information_schema.columns where table_schema = current_schema() and table_name = @table",
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

    [Fact]
    public async Task A_database_created_before_there_were_migrations_is_recorded_as_migrated_and_keeps_its_rows()
    {
        const string schema = "baseline_test";
        await using var db = await app.CreateSchemaAsync(schema);
        var contexts = SyncDbContext.CreateFactory(db);
        // The tables as sql/schema.sql made them, which are the ones InitialSchema creates, without a migration history
        await SyncSchema.MigrateAsync(contexts);
        await using var conn = await db.OpenConnectionAsync();
        await conn.StoreAsync(SyncFixture.InboxTable(), SyncFixture.Accounts, "ACC-1", "{}");
        await conn.ExecuteAsync("""drop table "__EFMigrationsHistory" """);
        var before = await TablesAndIndexesAsync(conn);

        // Running InitialSchema again would fail on the tables that exist
        await SyncSchema.MigrateAsync(contexts);
        await SyncSchema.MigrateAsync(contexts);

        await using var context = await contexts.CreateDbContextAsync();
        (await context.Database.GetAppliedMigrationsAsync()).Should().Equal(context.Database.GetMigrations());
        (await TablesAndIndexesAsync(conn)).Should().BeEquivalentTo([.. before, "__EFMigrationsHistory", "PK___EFMigrationsHistory"]);
        (await conn.ExecuteScalarAsync<long>($"select count(*) from {SyncFixture.InboxTable()}")).Should().Be(1);
        await conn.ExecuteAsync($"drop schema {schema} cascade");
    }

    [Theory]
    [InlineData("create table sf_inbox (id bigserial primary key, payload jsonb not null)")]
    [InlineData("create table sf_sync_state (entity_type text, entity_key text, synced_version bigint, primary key (entity_type, entity_key))")]
    [InlineData("""
        create table sf_account_inbox (id bigserial primary key, entity_id text not null, payload jsonb not null, retry_count int not null,
          next_attempt_at timestamptz, locked_until timestamptz, lock_token uuid, synced_time bigint);
        create table sf_sync_state (entity_type text, entity_id text, synced_time bigint, primary key (entity_type, entity_id))
        """)]
    public async Task A_database_with_tables_older_than_that_stops_startup_untouched(string olderTables)
    {
        var schema = $"older_{Guid.NewGuid():N}";
        await using var db = await app.CreateSchemaAsync(schema);
        await using var conn = await db.OpenConnectionAsync();
        await conn.ExecuteAsync(olderTables);
        var before = await TablesAndIndexesAsync(conn);

        var migrate = () => SyncSchema.MigrateAsync(SyncDbContext.CreateFactory(db));

        await migrate.Should().ThrowAsync<InvalidOperationException>().WithMessage("*no longer upgraded*fresh database*");
        (await TablesAndIndexesAsync(conn)).Should().Equal(before);
        await conn.ExecuteAsync($"drop schema {schema} cascade");
    }

    private async Task<List<string>> TablesAndIndexesAsync()
    {
        await using var conn = await app.Db.OpenConnectionAsync();
        return await TablesAndIndexesAsync(conn);
    }

    private static async Task<List<string>> TablesAndIndexesAsync(NpgsqlConnection conn)
    {
        return [.. await conn.QueryAsync<string>(
                "select c.relname::text from pg_class c join pg_namespace n on n.oid = c.relnamespace where n.nspname = current_schema() and c.relkind in ('r', 'i') order by 1")];
    }
}