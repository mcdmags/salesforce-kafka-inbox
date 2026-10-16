using Microsoft.Extensions.Options;
using Npgsql;
using SalesforceSync.Data;
using SalesforceSync.Inbox;
using SalesforceSync.Sync;

namespace SalesforceSync.IntegrationTests;

/// <summary>
/// Sync:CoalesceDelay: an event that makes an idle entity due makes it due only after the delay; an entity already due
/// keeps its time. Through the worker's own repository, in a schema of its own, so the pods (which work the public
/// schema) never see its rows.
/// </summary>
[Collection(SyncCollection.Name)]
public sealed class CoalesceDelayTests(SyncFixture app)
{
    private const string Schema = "coalesce_delay_test";

    [Fact]
    public async Task An_idle_entity_is_claimable_only_after_the_delay_with_the_events_stored_meanwhile()
    {
        await using var db = await SchemaAsync();
        var inbox = Inbox(db, TimeSpan.FromSeconds(2));
        var entity = SyncFixture.NewAccountId();

        await inbox.AddAsync(entity, SyncFixture.BaseTime, "{}", CancellationToken.None);
        await inbox.AddAsync(entity, SyncFixture.BaseTime, "{}", CancellationToken.None);

        (await inbox.ClaimAsync(CancellationToken.None)).Events.Should().BeEmpty("the entity is due only after the delay");
        var claim = await Eventually.ValueAsync(() => inbox.ClaimAsync(CancellationToken.None), c => c.Events.Count > 0,
            "the entity to be due", TimeSpan.FromSeconds(10));
        claim.Events.Should().HaveCount(2, "the second event arrived during the delay and is claimed with the first");
        await DropAsync(db);
    }

    [Fact]
    public async Task An_entity_already_due_keeps_its_time()
    {
        await using var db = await SchemaAsync();
        var entity = SyncFixture.NewAccountId();

        await Inbox(db, TimeSpan.Zero).AddAsync(entity, SyncFixture.BaseTime, "{}", CancellationToken.None);
        var inbox = Inbox(db, TimeSpan.FromMinutes(1));
        await inbox.AddAsync(entity, SyncFixture.BaseTime, "{}", CancellationToken.None);

        (await inbox.ClaimAsync(CancellationToken.None)).Events.Should().HaveCount(2, "the first event made it due at once");
        await DropAsync(db);
    }

    [Fact]
    public async Task Zero_makes_an_idle_entity_due_at_once()
    {
        await using var db = await SchemaAsync();
        var inbox = Inbox(db, TimeSpan.Zero);

        await inbox.AddAsync(SyncFixture.NewAccountId(), SyncFixture.BaseTime, "{}", CancellationToken.None);

        (await inbox.ClaimAsync(CancellationToken.None)).Events.Should().HaveCount(1);
        await DropAsync(db);
    }

    private async Task<NpgsqlDataSource> SchemaAsync()
    {
        var db = await app.CreateSchemaAsync(Schema);
        await SyncSchema.MigrateAsync(SyncDbContext.CreateFactory(db));
        return db;
    }

    private static InboxRepository<AccountInboxRow> Inbox(NpgsqlDataSource db, TimeSpan coalesceDelay)
    {
        return new InboxRepository<AccountInboxRow>(SyncDbContext.CreateFactory(db),
            Options.Create(new SyncOptions { CoalesceDelay = coalesceDelay }));
    }

    private static async Task DropAsync(NpgsqlDataSource db)
    {
        await using var conn = await db.OpenConnectionAsync();
        await conn.ExecuteAsync($"drop schema {Schema} cascade");
    }
}