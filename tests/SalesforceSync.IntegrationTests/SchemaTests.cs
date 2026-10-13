using Dapper;

namespace SalesforceSync.IntegrationTests;

[Collection(SyncCollection.Name)]
public sealed class SchemaTests(SyncFixture app)
{
    [Fact]
    public async Task Schema_can_be_reapplied_to_an_existing_database()
    {
        // The worker already applied it at startup; a restart against a persisted volume applies it again.
        var sql = await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "schema.sql"));

        var reapply = () => app.ExecuteSqlAsync(sql);

        await reapply.Should().NotThrowAsync();
    }

    [Fact]
    public async Task An_inbox_with_event_columns_is_kept_as_sf_inbox_legacy_and_replaced()
    {
        var sql = await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "schema.sql"));
        await using var conn = await app.Db.OpenConnectionAsync();
        // In a throwaway schema, rolled back at the end, so the live inbox is untouched
        await using var tx = await conn.BeginTransactionAsync();
        var schema = $"legacy_{Guid.NewGuid():N}";
        await conn.ExecuteAsync($"create schema {schema}; set local search_path to {schema}", transaction: tx);
        await conn.ExecuteAsync("""
            create table sf_inbox (
              id bigserial primary key, entity_type text not null, entity_key text not null, version bigint not null,
              payload jsonb not null, status text not null default 'pending',
              constraint sf_inbox_entity_version unique (entity_type, entity_key, version));
            create index sf_inbox_pending_key on sf_inbox (entity_type, entity_key, version) where status = 'pending';
            insert into sf_inbox (entity_type, entity_key, version, payload) values ('account', 'ACC-1', 7, '{}');
            """, transaction: tx);

        await conn.ExecuteAsync(sql, transaction: tx);
        await conn.ExecuteAsync(sql, transaction: tx); // still idempotent after the upgrade

        (await conn.ExecuteScalarAsync<long>(
                "select count(*) from sf_inbox_legacy where entity_key = 'ACC-1' and version = 7", transaction: tx))
            .Should().Be(1, "the old rows are kept aside for inspection");
        (await conn.QueryAsync<string>(
                "select indexname from pg_indexes where schemaname = current_schema() and tablename = 'sf_inbox' order by 1",
                transaction: tx))
            .Should().Equal("sf_inbox_event_unique", "sf_inbox_leased_key", "sf_inbox_pending_key", "sf_inbox_pkey", "sf_inbox_ready");
        var inserted = await conn.ExecuteAsync("""
            insert into sf_inbox (payload)
            values ('{"meta": {"time": "2026-10-14T01:02:03Z"}, "account": {"externalId": "ACC-1"}}'),
                   ('{"account": {"externalId": "ACC-1"}, "meta": {"time": "2026-10-14T01:02:03Z"}}')
            on conflict do nothing
            """, transaction: tx);
        inserted.Should().Be(1, "it is the same event twice (key order doesn't matter in jsonb)");

        await tx.RollbackAsync();
    }
}
