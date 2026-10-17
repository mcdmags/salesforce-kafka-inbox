using System.Globalization;
using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Npgsql;
using SalesforceSync.Data;
using SalesforceSync.Data.Migrations;
using SalesforceSync.Inbox;

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
        before.Should().Contain(["sf_account_inbox", "sf_account_relationship_inbox", "sf_contact_inbox", "sf_rate_limit", "sf_api_usage",
            "__EFMigrationsHistory"]);
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
    public async Task Each_inbox_has_one_row_per_entity_the_ready_index_is_partial_the_hot_tables_vacuum_often_and_the_rate_limit_row_and_view_exist()
    {
        await using var conn = await app.Db.OpenConnectionAsync();

        var indexes = (await conn.QueryAsync<(string Name, string Definition)>(
                "SELECT indexname::text, indexdef FROM pg_indexes WHERE schemaname = current_schema()"))
            .ToDictionary(i => i.Name, i => i.Definition);
        foreach (var table in InboxTables)
        {
            indexes[$"{table}_entity_id"].Should().StartWith("CREATE UNIQUE INDEX").And.EndWith($"{table} USING btree (entity_id)");
            indexes[$"{table}_ready"].Should().EndWith($"{table} USING btree (next_attempt_at) WHERE (next_attempt_at IS NOT NULL)");
        }

        foreach (var table in InboxTables)
        {
            (await conn.QuerySingleAsync<string>("SELECT array_to_string(reloptions, ', ') FROM pg_class WHERE oid = @table::regclass", new { table }))
                .Should().Be("autovacuum_vacuum_scale_factor=0.02, autovacuum_vacuum_threshold=1000", table);
        }

        (await conn.QuerySingleAsync<double>("SELECT capacity FROM sf_rate_limit WHERE name = 'salesforce'"))
            .Should().BeGreaterThan(0, "the migration creates the shared bucket's row (startup then configures it)");
        (await conn.ExecuteScalarAsync<long>("SELECT count(*) FROM sf_limit_status")).Should().Be(1);
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
            "id", "entity_id", "payload", "status", "attempt_count", "last_error", "last_error_code", "last_updated",
            "event_time", "next_attempt_at", "lock_token");
        (await Columns(SyncFixture.InboxTable(SyncFixture.Relationships))).Should().Equal(account);
        (await Columns(SyncFixture.InboxTable(SyncFixture.Contacts))).Should().Equal(account);
        (await conn.ExecuteScalarAsync<long>("SELECT count(*) FROM pg_proc WHERE proname LIKE 'sf\\_event\\_%'"))
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
        await conn.ExecuteAsync($"INSERT INTO {SyncFixture.InboxTable()} (entity_id, payload) VALUES ('ACC-1', @payload::jsonb)", new { payload });
        await conn.ExecuteAsync("""DROP TABLE "__EFMigrationsHistory" """);
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
        (await conn.QuerySingleAsync<long>($"SELECT (extract(epoch FROM event_time) * 1000000)::bigint FROM {SyncFixture.InboxTable()}"))
            .Should().Be(SyncFixture.Micros(1), "the row is kept, and InboxEventTime filled its event_time");
        await conn.ExecuteAsync($"DROP SCHEMA {schema} CASCADE");
    }

    [Fact]
    public async Task InboxEventTime_fills_the_event_time_of_existing_rows_to_the_microsecond_the_engine_reads()
    {
        const string schema = "event_time_test";
        await using var db = await app.CreateSchemaAsync(schema);
        var contexts = SyncDbContext.CreateFactory(db);
        await MigrateToAsync(contexts, typeof(LimitStatusWithoutOwnCalls));

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
            await conn.ExecuteAsync($"INSERT INTO {table} (entity_id, payload) VALUES (@id, @payload::jsonb)", new { id = $"ACC-{++entity}", payload });
        }

        // Up to InboxEventTime only: a later migration drops received_at, which the fill uses for unreadable rows
        await MigrateToAsync(contexts, typeof(InboxEventTime));

        // Null: the row got its received_at
        var stored = await conn.QueryAsync<long?>(
            $"SELECT CASE WHEN event_time <> received_at THEN (extract(epoch FROM event_time) * 1000000)::bigint END FROM {table} ORDER BY id");
        // The time as .NET reads it, cut to the microsecond as the consumer stores it (for .1234567 a
        // plain ::timestamptz would round up to .123457). An event the engine can't read is dead-lettered when claimed.
        var expected = times.Select(t => (long?)((DateTimeOffset.Parse(t, CultureInfo.InvariantCulture) - DateTimeOffset.UnixEpoch).Ticks / 10));
        stored.Should().Equal([.. expected, .. unreadable.Select(_ => (long?)null)]);
        (await conn.QuerySingleAsync<string>(
                "SELECT is_nullable::text FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = @table AND column_name = 'event_time'",
                new { table }))
            .Should().Be("NO");
        await conn.ExecuteAsync($"DROP SCHEMA {schema} CASCADE");
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
                await conn.ExecuteAsync($"INSERT INTO {table} (entity_id, payload, event_time, status) VALUES (@entity, @payload::jsonb, @time, @status)",
                    new { entity, payload = e.ToJsonString(), time = SyncFixture.TimeOf(e).UtcDateTime, status });
            }
        }

        await MigrateToAsync(contexts, typeof(InboxOneRowPerRecord));

        foreach (var table in InboxTables)
        {
            var kept = await conn.QueryAsync<(string Entity, long Time, string Status)>(
                $"SELECT entity_id, (extract(epoch FROM event_time) * 1000000)::bigint, status FROM {table} ORDER BY entity_id");
            kept.Should().Equal([
                    ("ACC-1", SyncFixture.Micros(3), "pending"), ("ACC-2", SyncFixture.Micros(5), "superseded"),
                    ("ACC-3", SyncFixture.Micros(2), "dead"), ("ACC-4", SyncFixture.Micros(7), "pending")],
                "{0} keeps each entity's newest event (of two as new, the one stored last), with its status", table);
            var duplicate = () => conn.ExecuteAsync($"INSERT INTO {table} (entity_id, payload, event_time) VALUES ('ACC-4', '{{}}', now())");
            (await duplicate.Should().ThrowAsync<PostgresException>()).Which.SqlState.Should().Be(PostgresErrorCodes.UniqueViolation);
        }
        await conn.ExecuteAsync($"DROP SCHEMA {schema} CASCADE");
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
                await conn.ExecuteAsync($"INSERT INTO {table} (entity_id, payload, event_time, status) VALUES (@entity, @payload::jsonb, @time, @status)",
                    new { entity, payload = e.ToJsonString(), time = SyncFixture.TimeOf(e).UtcDateTime, status });
            }
            await conn.ExecuteAsync("""
                INSERT INTO sf_sync_state (entity_type, entity_id, next_attempt_at, locked_until, lock_token, synced_time) VALUES
                  (@type, 'ACC-1', @at + make_interval(secs => @n), NULL, NULL, @n + 100),
                  (@type, 'ACC-2', NULL, NULL, NULL, @n + 200),
                  (@type, 'ACC-3', @at, @at + make_interval(secs => @n + 300), @lease, NULL),
                  (@type, 'ACC-5', @at, NULL, NULL, NULL),
                  (@type, 'ACC-9', @at, NULL, NULL, @n + 900)
                """, new { type, at, n, lease });
        }

        await MigrateToAsync(contexts, typeof(StateInInboxRow));

        for (var n = 0; n < inboxes.Length; n++)
        {
            var table = inboxes[n].Table;
            var rows = await conn.QueryAsync<(string Entity, DateTime? Next, DateTime? LockedUntil, Guid? Token, long? Synced)>(
                $"SELECT entity_id, next_attempt_at, locked_until, lock_token, synced_time FROM {table} ORDER BY entity_id");
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
                "SELECT entity_type, entity_id, synced_time FROM sf_sync_state WHERE entity_id IN ('ACC-1', 'ACC-2')"))
            .Should().BeEquivalentTo(inboxes.SelectMany((inbox, n) => new[] { (inbox.Type, "ACC-1", n + 100), (inbox.Type, "ACC-2", (long?)(n + 200)) }));
        (await conn.ExecuteScalarAsync<long>("SELECT count(*) FROM sf_sync_state")).Should().Be(5 * inboxes.Length);
        (await conn.ExecuteScalarAsync<long>(
                "SELECT count(*) FROM information_schema.columns WHERE table_schema = current_schema() AND column_name = 'synced_time'"))
            .Should().Be(1, "only sf_sync_state has the state again");
        await conn.ExecuteAsync($"DROP SCHEMA {schema} CASCADE");
    }

    [Fact]
    public async Task LeaseInNextAttemptAt_ends_a_live_lease_at_next_attempt_at_leaves_an_expired_one_due_and_Down_moves_it_back()
    {
        const string schema = "lease_in_next_attempt_at_test";
        await using var db = await app.CreateSchemaAsync(schema);
        var contexts = SyncDbContext.CreateFactory(db);
        await MigrateToAsync(contexts, typeof(DropReceivedAt));

        // ACC-1 leased (live), ACC-2 leased (expired: its worker died), ACC-3 due, ACC-4 idle; all but ACC-4 due long ago
        var at = SyncFixture.BaseTime.UtcDateTime;
        await using var conn = await db.OpenConnectionAsync();
        var leaseEnds = new Dictionary<string, DateTime>();
        foreach (var table in InboxTables)
        {
            await conn.ExecuteAsync($$"""
                INSERT INTO {{table}} (entity_id, payload, event_time, next_attempt_at, locked_until, lock_token, status) VALUES
                  ('ACC-1', '{}', @at, @at, now() + interval '1 hour', gen_random_uuid(), 'pending'),
                  ('ACC-2', '{}', @at, @at, @at + interval '1 second', gen_random_uuid(), 'pending'),
                  ('ACC-3', '{}', @at, @at, NULL, NULL, 'pending'),
                  ('ACC-4', '{}', @at, NULL, NULL, NULL, 'done')
                """, new { at });
            leaseEnds[table] = await conn.QuerySingleAsync<DateTime>($"SELECT locked_until FROM {table} WHERE entity_id = 'ACC-1'");
        }

        await MigrateToAsync(contexts, typeof(LeaseInNextAttemptAt));

        foreach (var table in InboxTables)
        {
            (await conn.QueryAsync<(string Entity, DateTime? Next)>($"SELECT entity_id, next_attempt_at FROM {table} ORDER BY entity_id"))
                .Should().Equal([("ACC-1", leaseEnds[table]), ("ACC-2", at), ("ACC-3", at), ("ACC-4", null)],
                    "{0}: a live lease ends at next_attempt_at, every other row keeps its due time", table);
            var claimed = await conn.QueryAsync<InboxEvent>(InboxSql.For(table).Claim,
                new { token = Guid.NewGuid(), lease = TimeSpan.FromMinutes(1), batchSize = 75 });
            claimed.Select(e => e.EntityId).Should().BeEquivalentTo(["ACC-2", "ACC-3"], "{0}: the expired lease is taken over, the live one isn't", table);
        }

        // Down: locked_until again, the end of every lease (ACC-1's, and the claim's on ACC-2 and ACC-3), and those rows
        // due no later than now, as the old claim left them
        await MigrateToAsync(contexts, typeof(DropReceivedAt));
        foreach (var table in InboxTables)
        {
            var rows = await conn.QueryAsync<(string Entity, bool? Due, bool? Leased)>(
                $"SELECT entity_id, next_attempt_at <= now(), locked_until > now() FROM {table} ORDER BY entity_id");
            rows.Should().Equal([("ACC-1", true, true), ("ACC-2", true, true), ("ACC-3", true, true), ("ACC-4", null, null)], table);
        }
        await conn.ExecuteAsync($"DROP SCHEMA {schema} CASCADE");
    }

    [Fact]
    public async Task DropSyncedTime_finishes_a_pending_row_its_watermark_already_covers_and_Down_sets_the_watermark_of_done_rows()
    {
        const string schema = "drop_synced_time_test";
        await using var db = await app.CreateSchemaAsync(schema);
        var contexts = SyncDbContext.CreateFactory(db);
        await MigrateToAsync(contexts, typeof(LeaseInNextAttemptAt));

        // Each row's event is version 2. ACC-1 a redelivery over a done row, not claimed yet (watermark = its time), and
        // ACC-2 the same while leased; ACC-3 newer than its watermark; ACC-4 superseded; ACC-5 dead; ACC-6 done
        await using var conn = await db.OpenConnectionAsync();
        foreach (var table in InboxTables)
        {
            await conn.ExecuteAsync($$"""
                INSERT INTO {{table}} (entity_id, payload, event_time, status, next_attempt_at, lock_token, last_error_code, synced_time) VALUES
                  ('ACC-1', '{}', @time, 'pending', now(), NULL, NULL, @v2),
                  ('ACC-2', '{}', @time, 'pending', now() + interval '1 hour', gen_random_uuid(), NULL, @v2),
                  ('ACC-3', '{}', @time, 'pending', now(), NULL, 'SOME_CODE', @v1),
                  ('ACC-4', '{}', @time, 'superseded', NULL, NULL, NULL, @v2),
                  ('ACC-5', '{}', @time, 'dead', NULL, NULL, 'SOME_CODE', @v2),
                  ('ACC-6', '{}', @time, 'done', NULL, NULL, NULL, @v2)
                """, new { time = SyncFixture.BaseTime.AddMilliseconds(2).UtcDateTime, v1 = SyncFixture.Micros(1), v2 = SyncFixture.Micros(2) });
        }

        await MigrateToAsync(contexts, typeof(DropSyncedTime));

        foreach (var table in InboxTables)
        {
            (await conn.QueryAsync<(string Entity, string Status, bool Idle, string? Code)>(
                    $"SELECT entity_id, status, next_attempt_at IS NULL AND lock_token IS NULL, last_error_code FROM {table} ORDER BY entity_id"))
                .Should().Equal([
                        ("ACC-1", "done", true, null), ("ACC-2", "done", true, null), ("ACC-3", "pending", false, "SOME_CODE"),
                        ("ACC-4", "done", true, null), ("ACC-5", "dead", true, "SOME_CODE"), ("ACC-6", "done", true, null)],
                    "{0}: what the claim would have superseded is done, the rest as it was", table);
        }

        // Down: the watermark of every done row is its event's time
        await MigrateToAsync(contexts, typeof(LeaseInNextAttemptAt));
        foreach (var table in InboxTables)
        {
            (await conn.QueryAsync<(string Entity, long? Synced)>($"SELECT entity_id, synced_time FROM {table} ORDER BY entity_id"))
                .Should().Equal([
                        ("ACC-1", SyncFixture.Micros(2)), ("ACC-2", SyncFixture.Micros(2)), ("ACC-3", null), ("ACC-4", SyncFixture.Micros(2)),
                        ("ACC-5", null), ("ACC-6", SyncFixture.Micros(2))], table);
        }
        await conn.ExecuteAsync($"DROP SCHEMA {schema} CASCADE");
    }

    [Theory]
    [InlineData("CREATE TABLE sf_inbox (id bigserial PRIMARY KEY, payload jsonb NOT NULL)")]
    [InlineData("CREATE TABLE sf_sync_state (entity_type text, entity_key text, synced_version bigint, PRIMARY KEY (entity_type, entity_key))")]
    [InlineData("""
        CREATE TABLE sf_account_inbox (id bigserial PRIMARY KEY, entity_id text NOT NULL, payload jsonb NOT NULL, retry_count int NOT NULL,
          next_attempt_at timestamptz, locked_until timestamptz, lock_token uuid, synced_time bigint);
        CREATE TABLE sf_sync_state (entity_type text, entity_id text, synced_time bigint, PRIMARY KEY (entity_type, entity_id))
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
        await conn.ExecuteAsync($"DROP SCHEMA {schema} CASCADE");
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
                "SELECT c.relname::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = current_schema() AND c.relkind IN ('r', 'i') ORDER BY 1")];
    }
}