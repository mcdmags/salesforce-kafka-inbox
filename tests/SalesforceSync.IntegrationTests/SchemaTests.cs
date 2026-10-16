using System.Globalization;
using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Npgsql;
using SalesforceSync.Data;
using SalesforceSync.Data.Migrations;

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
        before.Should().Contain(["sf_account_inbox", "sf_account_relationship_inbox", "sf_contact_inbox", "__EFMigrationsHistory"]);
        before.Should().Contain([
            "sf_account_inbox_entity_id", "sf_account_inbox_pkey", "sf_account_relationship_inbox_entity_id", "sf_contact_inbox_entity_id",
            "sf_account_relationship_inbox_ready", "sf_contact_inbox_ready"]);
        before.Should().NotContain("sf_sync_state", "every entity's state is in its inbox row");
        before.Where(r => r.StartsWith("sf_account_inbox", StringComparison.Ordinal)).Should().BeEquivalentTo(
            ["sf_account_inbox", "sf_account_inbox_pkey", "sf_account_inbox_entity_id", "sf_account_inbox_ready"]);

        await SyncSchema.MigrateAsync(app.Contexts);

        (await TablesAndIndexesAsync()).Should().BeEquivalentTo(before);
        await using var db = await app.Contexts.CreateDbContextAsync();
        (await db.Database.GetAppliedMigrationsAsync()).Should().Equal(db.Database.GetMigrations());
    }

    [Fact]
    public async Task Each_inbox_has_one_row_per_entity_the_ready_index_is_partial_and_the_hot_tables_vacuum_often()
    {
        await using var conn = await app.Db.OpenConnectionAsync();

        var indexes = (await conn.QueryAsync<(string Name, string Definition)>(
                "select indexname::text, indexdef from pg_indexes where schemaname = current_schema()"))
            .ToDictionary(i => i.Name, i => i.Definition);
        foreach (var table in InboxTables)
        {
            indexes[$"{table}_entity_id"].Should().StartWith("CREATE UNIQUE INDEX").And.EndWith($"{table} USING btree (entity_id)");
            indexes[$"{table}_ready"].Should().EndWith($"{table} USING btree (next_attempt_at) WHERE (next_attempt_at IS NOT NULL)");
        }

        foreach (var table in InboxTables)
        {
            (await conn.QuerySingleAsync<string>("select array_to_string(reloptions, ', ') from pg_class where oid = @table::regclass", new { table }))
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
            "select column_name::text || ' ' || data_type from information_schema.columns where table_schema = current_schema() and table_name = @table order by ordinal_position",
            new { table })];
        }

        var account = await Columns(SyncFixture.InboxTable());
        account.Select(c => c.Split(' ')[0]).Should().Equal(
            "id", "entity_id", "payload", "status", "attempt_count", "last_error", "last_error_code", "received_at", "last_updated",
            "event_time", "next_attempt_at", "locked_until", "lock_token", "synced_time");
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
        await MigrateToAsync(contexts, typeof(InitialSchema));
        await using var conn = await db.OpenConnectionAsync();
        var payload = SyncFixture.AccountEvent("ACC-1", 1, [], []).ToJsonString();
        await conn.ExecuteAsync($"insert into {SyncFixture.InboxTable()} (entity_id, payload) values ('ACC-1', @payload::jsonb)", new { payload });
        await conn.ExecuteAsync("""drop table "__EFMigrationsHistory" """);
        var before = await TablesAndIndexesAsync(conn);

        // Running InitialSchema again would fail on the tables that exist; the later migrations apply as usual
        await SyncSchema.MigrateAsync(contexts);
        await SyncSchema.MigrateAsync(contexts);

        await using var context = await contexts.CreateDbContextAsync();
        (await context.Database.GetAppliedMigrationsAsync()).Should().Equal(context.Database.GetMigrations());
        // InboxOneRowPerRecord replaced each inbox's pending index with its unique entity_id index; StateInInboxRow
        // dropped sf_sync_state and gave each inbox its ready index
        string[] state = ["sf_sync_state", "sf_sync_state_pkey", "sf_sync_state_ready"];
        (await TablesAndIndexesAsync(conn)).Should().BeEquivalentTo(
        [
            .. before.Except(state).Select(r => r.Replace("_pending", "_entity_id", StringComparison.Ordinal)),
            .. InboxTables.Select(t => $"{t}_ready"), "__EFMigrationsHistory", "PK___EFMigrationsHistory"
        ]);
        (await conn.QuerySingleAsync<long>($"select (extract(epoch from event_time) * 1000000)::bigint from {SyncFixture.InboxTable()}"))
            .Should().Be(SyncFixture.SyncedTime(1), "the row is kept, and InboxEventTime filled its event_time");
        await conn.ExecuteAsync($"drop schema {schema} cascade");
    }

    [Fact]
    public async Task InboxEventTime_fills_the_event_time_of_existing_rows_to_the_microsecond_the_engine_reads()
    {
        const string schema = "event_time_test";
        await using var db = await app.CreateSchemaAsync(schema);
        var contexts = SyncDbContext.CreateFactory(db);
        await MigrateToAsync(contexts, typeof(InitialSchema));

        // Rows stored before the column existed: times as the consumer admits them, and events it would never have stored
        string[] times =
        [
            "2026-10-14T01:02:03.456Z", "2026-10-14T03:02:03+02:00", "2026-10-14T01:02:03.1234567Z", "2026-10-14T01:02:03.123456951Z",
            "2026-10-14T01:02:03.99999996Z"
        ];
        string[] unreadable =
        [
            "{}", /*lang=json,strict*/ """{ "meta": { "time": "2026-10-14T01:02:03" } }""", /*lang=json,strict*/ """{ "meta": { "time": 1 } }"""
        ];
        await using var conn = await db.OpenConnectionAsync();
        var table = SyncFixture.InboxTable();
        var entity = 0;
        foreach (var payload in times.Select(t => $$"""{ "meta": { "time": "{{t}}" } }""").Concat(unreadable))
        {
            // An entity each: InboxOneRowPerRecord, applied after, would keep one row per entity
            await conn.ExecuteAsync($"insert into {table} (entity_id, payload) values (@id, @payload::jsonb)", new { id = $"ACC-{++entity}", payload });
        }

        await SyncSchema.MigrateAsync(contexts);

        // Null: the row got its received_at
        var stored = await conn.QueryAsync<long?>(
            $"select case when event_time <> received_at then (extract(epoch from event_time) * 1000000)::bigint end from {table} order by id");
        // What the engine compares with the watermark: the time as .NET reads it, cut to the microsecond (for .1234567 a
        // plain ::timestamptz would round up to .123457). An event the engine can't read is dead-lettered when claimed.
        var expected = times.Select(t => (long?)((DateTimeOffset.Parse(t, CultureInfo.InvariantCulture) - DateTimeOffset.UnixEpoch).Ticks / 10));
        stored.Should().Equal([.. expected, .. unreadable.Select(_ => (long?)null)]);
        (await conn.QuerySingleAsync<string>(
                "select is_nullable::text from information_schema.columns where table_schema = current_schema() and table_name = @table and column_name = 'event_time'",
                new { table }))
            .Should().Be("NO");
        await conn.ExecuteAsync($"drop schema {schema} cascade");
    }

    [Fact]
    public async Task InboxOneRowPerRecord_keeps_each_entitys_newest_row_and_then_makes_entity_id_unique()
    {
        const string schema = "one_row_per_record_test";
        await using var db = await app.CreateSchemaAsync(schema);
        var contexts = SyncDbContext.CreateFactory(db);
        await MigrateToAsync(contexts, typeof(InboxEventTime));

        // Several rows per entity, as the inboxes held one row per event: (entity, version, status) in arrival order
        (string Entity, int Version, string Status)[] stored =
        [
            ("ACC-1", 1, "done"), ("ACC-1", 3, "pending"), ("ACC-1", 2, "pending"),
            ("ACC-2", 5, "done"), ("ACC-2", 5, "superseded"),
            ("ACC-3", 1, "pending"), ("ACC-3", 2, "dead"),
            ("ACC-4", 7, "pending")
        ];
        await using var conn = await db.OpenConnectionAsync();
        foreach (var table in InboxTables)
        {
            foreach (var (entity, version, status) in stored)
            {
                var e = SyncFixture.AccountEvent(entity, version, [], []);
                await conn.ExecuteAsync($"insert into {table} (entity_id, payload, event_time, status) values (@entity, @payload::jsonb, @time, @status)",
                    new { entity, payload = e.ToJsonString(), time = SyncFixture.TimeOf(e).UtcDateTime, status });
            }
        }

        await SyncSchema.MigrateAsync(contexts);

        foreach (var table in InboxTables)
        {
            var kept = await conn.QueryAsync<(string Entity, long Time, string Status)>(
                $"select entity_id, (extract(epoch from event_time) * 1000000)::bigint, status from {table} order by entity_id");
            kept.Should().Equal([
                    ("ACC-1", SyncFixture.SyncedTime(3), "pending"), ("ACC-2", SyncFixture.SyncedTime(5), "superseded"),
                    ("ACC-3", SyncFixture.SyncedTime(2), "dead"), ("ACC-4", SyncFixture.SyncedTime(7), "pending")],
                "{0} keeps each entity's newest event (of two as new, the one stored last), with its status", table);
            var duplicate = () => conn.ExecuteAsync($"insert into {table} (entity_id, payload, event_time) values ('ACC-4', '{{}}', now())");
            (await duplicate.Should().ThrowAsync<PostgresException>()).Which.SqlState.Should().Be(PostgresErrorCodes.UniqueViolation);
        }
        await conn.ExecuteAsync($"drop schema {schema} cascade");
    }

    [Fact]
    public async Task StateInInboxRow_moves_each_entitys_state_into_its_row_and_Down_moves_it_back()
    {
        const string schema = "state_in_inbox_row_test";
        await using var db = await app.CreateSchemaAsync(schema);
        var contexts = SyncDbContext.CreateFactory(db);
        await MigrateToAsync(contexts, typeof(InboxOneRowPerRecord));

        // Every inbox with its type's state rows, each with values of its own (n), so state copied into another type's
        // inbox would show. ACC-1 due and synced, ACC-2 idle and synced, ACC-3 leased, ACC-4 pending without state, ACC-5
        // dead but due, ACC-9 a state row without an inbox row.
        (string Type, string Table)[] inboxes =
            [(SyncFixture.Accounts, InboxTables[0]), (SyncFixture.Relationships, InboxTables[1]), (SyncFixture.Contacts, InboxTables[2])];
        var lease = Guid.NewGuid();
        var at = SyncFixture.BaseTime.UtcDateTime;
        await using var conn = await db.OpenConnectionAsync();
        for (var n = 0; n < inboxes.Length; n++)
        {
            var (type, table) = inboxes[n];
            foreach (var (entity, status) in new[] { ("ACC-1", "pending"), ("ACC-2", "done"), ("ACC-3", "pending"), ("ACC-4", "pending"), ("ACC-5", "dead") })
            {
                var e = SyncFixture.AccountEvent(entity, 1, [], []);
                await conn.ExecuteAsync($"insert into {table} (entity_id, payload, event_time, status) values (@entity, @payload::jsonb, @time, @status)",
                    new { entity, payload = e.ToJsonString(), time = SyncFixture.TimeOf(e).UtcDateTime, status });
            }
            await conn.ExecuteAsync("""
                insert into sf_sync_state (entity_type, entity_id, next_attempt_at, locked_until, lock_token, synced_time) values
                  (@type, 'ACC-1', @at + make_interval(secs => @n), null, null, @n + 100),
                  (@type, 'ACC-2', null, null, null, @n + 200),
                  (@type, 'ACC-3', @at, @at + make_interval(secs => @n + 300), @lease, null),
                  (@type, 'ACC-5', @at, null, null, null),
                  (@type, 'ACC-9', @at, null, null, @n + 900)
                """, new { type, at, n, lease });
        }

        await SyncSchema.MigrateAsync(contexts);

        for (var n = 0; n < inboxes.Length; n++)
        {
            var table = inboxes[n].Table;
            var rows = await conn.QueryAsync<(string Entity, DateTime? Next, DateTime? LockedUntil, Guid? Token, long? Synced)>(
                $"select entity_id, next_attempt_at, locked_until, lock_token, synced_time from {table} order by entity_id");
            rows.Select(r => r.Entity).Should().Equal(["ACC-1", "ACC-2", "ACC-3", "ACC-4", "ACC-5"], "{0}: no row is added (ACC-9's state is dropped) or lost", table);
            rows[0].Should().Be(("ACC-1", at.AddSeconds(n), null, null, n + 100), "{0}: due and synced as it was", table);
            rows[1].Should().Be(("ACC-2", null, null, null, n + 200), "{0}: idle, the watermark kept", table);
            rows[2].Should().Be(("ACC-3", at, at.AddSeconds(n + 300), lease, null), "{0}: the lease goes on", table);
            rows[3].Next.Should().NotBeNull("{0}: a pending row without a due time is due now", table);
            rows[4].Next.Should().BeNull("{0}: only a pending row is due", table);
        }
        (await TablesAndIndexesAsync(conn)).Should().NotContain("sf_sync_state");

        // Down: sf_sync_state again, a state row per inbox row, and the inbox columns gone
        await MigrateToAsync(contexts, typeof(InboxOneRowPerRecord));
        (await conn.QueryAsync<(string Type, string Entity, long? Synced)>(
                "select entity_type, entity_id, synced_time from sf_sync_state where entity_id in ('ACC-1', 'ACC-2')"))
            .Should().BeEquivalentTo(inboxes.SelectMany((inbox, n) => new[] { (inbox.Type, "ACC-1", n + 100), (inbox.Type, "ACC-2", (long?)(n + 200)) }));
        (await conn.ExecuteScalarAsync<long>("select count(*) from sf_sync_state")).Should().Be(5 * inboxes.Length);
        (await conn.ExecuteScalarAsync<long>(
                "select count(*) from information_schema.columns where table_schema = current_schema() and column_name = 'synced_time'"))
            .Should().Be(1, "only sf_sync_state has the state again");
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

    /// <summary>Applies the migrations up to and including <paramref name="migration"/>, as a database of that time has them.</summary>
    private static async Task MigrateToAsync(IDbContextFactory<SyncDbContext> contexts, Type migration)
    {
        await using var db = await contexts.CreateDbContextAsync();
        await db.GetService<IMigrator>().MigrateAsync(migration.GetCustomAttribute<MigrationAttribute>()!.Id);
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