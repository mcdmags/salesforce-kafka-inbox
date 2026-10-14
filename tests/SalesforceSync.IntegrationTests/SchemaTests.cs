using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Npgsql;
using SalesforceSync.Inbox;

namespace SalesforceSync.IntegrationTests;

[Collection(SyncCollection.Name)]
public sealed class SchemaTests(SyncFixture app)
{
    private static readonly string[] InboxTables = [SyncFixture.InboxTable(), SyncFixture.InboxTable(SyncFixture.Relationships)];

    [Fact]
    public async Task Startup_created_one_inbox_table_per_object_type_and_reapplying_changes_nothing()
    {
        // The worker already applied the schema at startup; a restart against a persisted volume applies it again
        var before = await TablesAndIndexesAsync();
        before.Should().Contain(["sf_account_inbox", "sf_account_relationship_inbox", "sf_sync_state", "sf_rate_limit", "sf_api_usage"]);
        before.Should().Contain([
            "sf_account_inbox_ready", "sf_account_inbox_pending", "sf_account_inbox_pkey",
            "sf_account_relationship_inbox_ready", "sf_account_relationship_inbox_pending"]);
        before.Should().NotContain("sf_inbox", "there is no shared inbox any more");

        await InboxSchema.EnsureCreatedAsync(app.Contexts, InboxTables);
        await InboxSchema.EnsureCreatedAsync(app.Contexts, InboxTables);

        (await TablesAndIndexesAsync()).Should().BeEquivalentTo(before);
    }

    [Fact]
    public async Task Both_inboxes_have_the_same_columns_and_none_for_the_event()
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
            "id", "entity_id", "payload", "status", "retry_count", "last_updated", "last_error", "last_error_code",
            "next_attempt_at", "received_at");
        (await Columns(SyncFixture.InboxTable(SyncFixture.Relationships))).Should().Equal(account);
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
            actual.Should().NotBeEmpty($"{table} is created by sql/schema.sql");
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
            "both inboxes have exactly the same fields");
        db.InboxTables.Should().BeEquivalentTo(InboxTables);
    }

    [Fact]
    public async Task An_inbox_from_before_is_kept_as_sf_inbox_legacy_and_the_new_tables_are_created()
    {
        await using var conn = await app.Db.OpenConnectionAsync();
        // In a throwaway schema, rolled back at the end, so the live inboxes are untouched
        await using var tx = await conn.BeginTransactionAsync();
        var schema = $"legacy_{Guid.NewGuid():N}";
        await conn.ExecuteAsync($"create schema {schema}; set local search_path to {schema}", transaction: tx);
        // The previous shape: one table of raw payloads, read by SQL functions with expression indexes on them
        await conn.ExecuteAsync("""
            create function sf_event_type(event jsonb) returns text language sql immutable strict
              return jsonb_path_query_first(event, '$.keyvalue() ? (@.key != "meta").key') #>> '{}';
            create function sf_event_key(event jsonb) returns text language sql immutable strict
              return event -> sf_event_type(event) ->> 'externalId';
            create function sf_event_version(event jsonb) returns bigint language sql immutable strict
              return (extract(epoch from (event -> 'meta' ->> 'time')::timestamptz) * 1000000)::bigint;
            create table sf_inbox (id bigserial primary key, payload jsonb not null, status text not null default 'pending');
            create index sf_inbox_pending_key on sf_inbox (sf_event_type(payload), sf_event_key(payload)) where status = 'pending';
            create index sf_inbox_status on sf_inbox (status);
            create table sf_sync_state (entity_type text not null, entity_key text not null, synced_version bigint not null,
              synced_at timestamptz not null default now(), constraint sf_sync_state_pkey primary key (entity_type, entity_key));
            insert into sf_inbox (payload) values ('{"meta": {"time": "2026-10-14T01:02:03Z"}, "account": {"externalId": "ACC-1"}}');
            insert into sf_sync_state (entity_type, entity_key, synced_version) values ('account', 'ACC-1', 1791939723000000);
            """, transaction: tx);

        await PgSql.ApplySchemaAsync(conn, tx, InboxTables);
        await PgSql.ApplySchemaAsync(conn, tx, InboxTables); // still idempotent after the upgrade

        (await conn.ExecuteScalarAsync<long>("select count(*) from sf_inbox_legacy where payload -> 'account' ->> 'externalId' = 'ACC-1'", transaction: tx))
            .Should().Be(1, "the old rows are kept aside for inspection");
        var relations = await TablesAndIndexesAsync(conn, tx);
        relations.Should().Contain(["sf_inbox_legacy", "sf_inbox_legacy_pkey", "sf_inbox_legacy_status", "sf_account_inbox", "sf_account_relationship_inbox"])
            .And.NotContain("sf_inbox");
        (await conn.ExecuteScalarAsync<long>(
                "select count(*) from pg_proc p join pg_namespace n on n.oid = p.pronamespace where n.nspname = current_schema() and proname like 'sf\\_event\\_%'", transaction: tx))
            .Should().Be(0, "the functions that read the payload are dropped (with the legacy index built on them)");
        (await conn.ExecuteScalarAsync<long>("select synced_time from sf_sync_state where entity_type = 'account' and entity_id = 'ACC-1'", transaction: tx))
            .Should().Be(1791939723000000, "watermarks survive under their new column names");

        // A second upgrade (an sf_inbox appears again next to an existing legacy table) doesn't collide
        await conn.ExecuteAsync("create table sf_inbox (id bigserial primary key, payload jsonb not null)", transaction: tx);
        await PgSql.ApplySchemaAsync(conn, tx, InboxTables);
        (await TablesAndIndexesAsync(conn, tx)).Should().Contain(["sf_inbox_legacy", "sf_inbox_legacy_2"]).And.NotContain("sf_inbox");

        await tx.RollbackAsync();
    }

    private async Task<List<string>> TablesAndIndexesAsync()
    {
        await using var conn = await app.Db.OpenConnectionAsync();
        return await TablesAndIndexesAsync(conn, null);
    }

    private static async Task<List<string>> TablesAndIndexesAsync(NpgsqlConnection conn, NpgsqlTransaction? tx)
    {
        return [.. await conn.QueryAsync<string>(
                "select c.relname::text from pg_class c join pg_namespace n on n.oid = c.relnamespace where n.nspname = current_schema() and c.relkind in ('r', 'i') order by 1",
                transaction: tx)];
    }
}