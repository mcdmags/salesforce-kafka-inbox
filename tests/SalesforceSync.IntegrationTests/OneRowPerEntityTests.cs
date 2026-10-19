using System.Text.Json.Nodes;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Npgsql;
using SalesforceSync.Data;
using SalesforceSync.Data.Inbox;

namespace SalesforceSync.IntegrationTests;

/// <summary>
/// An inbox keeps one row per entity: storing an event overwrites the entity's row unless it holds a newer event,
/// and an outcome is recorded on a claimed row only while it still holds the event the claim read. Runs through the
/// worker's own repository, in a schema of its own. Pods (which work the public schema) never see its rows, and
/// every step happens exactly when the test says (<see cref="SyncFixture.MigratedSchemaAsync"/>).
/// </summary>
[Collection(SyncCollection.Name)]
public sealed class OneRowPerEntityTests(SyncFixture app)
{
    private const string Schema = "one_row_per_entity_test";
    private static readonly string Table = SyncFixture.InboxTable();
    private IDbContextFactory<SyncDbContext>? _contexts;

    [Theory]
    [InlineData("pending")]
    [InlineData("done")]
    [InlineData("dead")]
    public async Task A_newer_event_overwrites_the_row_and_makes_it_pending_with_no_attempts_or_errors(string status)
    {
        var db = await SchemaAsync();
        var inbox = Inbox();
        var entity = SyncFixture.NewAccountId();
        await StoreAsync(inbox, entity, 1);
        await ExecuteAsync(db, $"UPDATE {Table} SET attempt_count = 3, {InState(status)}");
        var before = await RowAsync(db, entity);

        await StoreAsync(inbox, entity, 2);

        var after = await RowAsync(db, entity);
        after.Should().Be(new Row(2, "pending", 0, null, SyncFixture.TimeOf(Event(entity, 2)), after.LastUpdated),
            "the entity's one row now holds the newer event, as a new event");
        after.LastUpdated.Should().BeAfter(before.LastUpdated);
    }

    [Fact]
    public async Task A_mapping_error_holds_every_due_row_and_puts_the_error_on_the_broken_one()
    {
        var db = await SchemaAsync();
        var inbox = Inbox();
        string[] entities = [SyncFixture.NewAccountId(), SyncFixture.NewAccountId(), SyncFixture.NewAccountId()];
        foreach (var entity in entities)
        {
            await StoreAsync(inbox, entity, 1);
        }

        // The worker claims two of the three (the batch size), and finds the mapping wrong on the first
        var claim = await new InboxRepository<AccountInboxRow>(_contexts!, Options.Create(new SyncOptions { BatchSize = 2 })).ClaimAsync(CancellationToken.None);
        var broken = claim.Events[0].EntityId;
        await inbox.ReleaseAsync(claim, new Release(TimeSpan.FromMinutes(5), "stopped",
            [new FailedEvent(broken, "INVALID_FIELD: No such column 'Rating'")], StopInbox: true), CancellationToken.None);

        var rows = new List<(string Entity, Row Row, DateTime? NextAttemptAt)>();
        foreach (var entity in entities)
        {
            rows.Add((entity, await RowAsync(db, entity), await StateAsync(db, entity)));
        }

        rows.Should().OnlyContain(r => r.Row.Status == "pending" && r.Row.AttemptCount == 0,
            "nothing is dead-lettered, no attempt is spent");
        rows.Single(r => r.Entity == broken).Row.Should().Match<Row>(r => r.Error == "INVALID_FIELD: No such column 'Rating'");
        rows.Where(r => r.Entity == claim.Events[1].EntityId).Should().ContainSingle().Which.Row.Error.Should().Be("stopped");
        rows.Where(r => r.Entity == entities.Except(claim.EntityIds).Single()).Should().ContainSingle().Which.Row.Error.Should().BeNull("a row nobody claimed is only made to wait");
        rows.Should().OnlyContain(r => r.NextAttemptAt > DateTime.UtcNow + TimeSpan.FromMinutes(4), "the default MappingErrorHold is 5 minutes");
    }

    [Fact]
    public async Task An_older_event_changes_nothing()
    {
        var db = await SchemaAsync();
        var inbox = Inbox();
        var entity = SyncFixture.NewAccountId();
        await StoreAsync(inbox, entity, 2);
        await ExecuteAsync(db, $"UPDATE {Table} SET next_attempt_at = NULL, last_error = NULL");
        var before = await RowAsync(db, entity);

        await StoreAsync(inbox, entity, 1);

        (await RowAsync(db, entity)).Should().Be(before);
    }

    [Fact]
    public async Task An_event_as_new_as_a_done_row_a_redelivery_changes_nothing()
    {
        var db = await SchemaAsync();
        var inbox = Inbox();
        var entity = SyncFixture.NewAccountId();
        await StoreAsync(inbox, entity, 1);
        var first = await inbox.ClaimAsync(CancellationToken.None);
        await inbox.RecordAsync(first, Synced(first), CancellationToken.None);
        var before = await RowAsync(db, entity);

        await StoreAsync(inbox, entity, 1);

        (await RowAsync(db, entity)).Should().Be(before, "its time is in Salesforce already");
        (await inbox.ClaimAsync(CancellationToken.None)).Events.Should().BeEmpty("there is nothing to write");
    }

    [Theory]
    [InlineData("pending")]
    [InlineData("dead")]
    public async Task An_event_as_new_as_a_row_that_is_not_done_overwrites_it_and_is_claimed(string status)
    {
        var db = await SchemaAsync();
        var inbox = Inbox();
        var entity = SyncFixture.NewAccountId();
        await StoreAsync(inbox, entity, 1);
        await ExecuteAsync(db, $"UPDATE {Table} SET attempt_count = 3, {InState(status)}");

        await StoreAsync(inbox, entity, 1);

        (await RowAsync(db, entity)).Should().Match<Row>(r => r.Version == 1 && r.Status == "pending" && r.AttemptCount == 0 && r.Error == null);
        (await inbox.ClaimAsync(CancellationToken.None)).Events.Should().ContainSingle("a redelivery of an event not yet synced is written");
    }

    /// <summary>
    /// The race the one row per entity brings: a worker claims v1 and, while it writes v1 to Salesforce, the consumer
    /// overwrites the row with v2. Whatever the worker records for v1 must not land on v2: the row stays pending with
    /// v2, the entity is due, and the next claim takes v2.
    /// </summary>
    [Theory]
    [MemberData(nameof(SyncFixture.Outcomes), MemberType = typeof(SyncFixture))]
    public async Task An_outcome_for_a_claimed_event_is_not_recorded_on_the_newer_event_that_overwrote_it(string outcome)
    {
        var db = await SchemaAsync();
        var inbox = Inbox();
        var entity = SyncFixture.NewAccountId();
        await StoreAsync(inbox, entity, 1);
        var claim = await inbox.ClaimAsync(CancellationToken.None);
        claim.Events.Should().ContainSingle();

        // The consumer stores v2 while the worker is writing v1 to Salesforce
        await StoreAsync(inbox, entity, 2);
        await SyncFixture.RecordOutcomeAsync(inbox, claim, outcome);

        (await RowAsync(db, entity)).Should().Match<Row>(r => r.Version == 2 && r.Status == "pending" && r.AttemptCount == 0
                                                             && r.Error == null, "nothing of v1's outcome lands on v2");
        (await StateAsync(db, entity)).Should().NotBeNull("v2 is pending, so its entity must be due");

        var next = await Eventually.ValueAsync(() => inbox.ClaimAsync(CancellationToken.None), c => c.Events.Count > 0,
            "the entity to be claimed again", TimeSpan.FromSeconds(10));
        next.Events.Should().ContainSingle().Which.EventTime.Should().Be(SyncFixture.TimeOf(Event(entity, 2)), "the next claim writes v2");
        await inbox.RecordAsync(next, Synced(next), CancellationToken.None);
        (await RowAsync(db, entity)).Should().Match<Row>(r => r.Version == 2 && r.Status == "done");
    }

    private static BatchOutcomes Synced(InboxClaim claim)
    {
        return new BatchOutcomes(claim.EntityIds, [], []);
    }

    private static JsonObject Event(string entity, long version)
    {
        return SyncFixture.AccountEvent(entity, version, new JsonObject { ["name"] = $"v{version}" }, []);
    }

    private static Task StoreAsync(IRepository<AccountInboxRow> inbox, string entity, long version)
    {
        var e = Event(entity, version);
        return inbox.AddAsync(entity, SyncFixture.TimeOf(e), e.ToJsonString(), CancellationToken.None);
    }

    private async Task<NpgsqlDataSource> SchemaAsync()
    {
        (var db, _contexts) = await app.MigratedSchemaAsync(Schema);
        return db;
    }

    private InboxRepository<AccountInboxRow> Inbox()
    {
        return new InboxRepository<AccountInboxRow>(_contexts!, Options.Create(new SyncOptions { UnknownOutcomeHold = TimeSpan.FromMilliseconds(200) }));
    }

    /// <summary>The columns an inbox row has in the state: waiting for its next try, or ended by its error (dead).</summary>
    private static string InState(string status)
    {
        return status == "pending" ? "last_error = 'failed'" : status == "dead" ? "last_error = 'failed', next_attempt_at = NULL" : "last_error = NULL, next_attempt_at = NULL";
    }

    private static async Task<Row> RowAsync(NpgsqlDataSource db, string entity)
    {
        await using var conn = await db.OpenConnectionAsync();
        var row = await conn.QuerySingleAsync<StoredRow>(
            $"SELECT payload::text, {SyncFixture.StatusSql} AS status, attempt_count, last_error, event_time, last_updated FROM {Table} WHERE entity_id = @entity",
            new { entity });
        return new Row(SyncFixture.VersionOf(row.Payload), row.Status, row.AttemptCount, row.Error, row.EventTime, row.LastUpdated);
    }

    private static async Task<DateTime?> StateAsync(NpgsqlDataSource db, string entity)
    {
        await using var conn = await db.OpenConnectionAsync();
        return await conn.QuerySingleAsync<DateTime?>(
            $"SELECT next_attempt_at FROM {Table} WHERE entity_id = @entity", new { entity });
    }

    private static async Task ExecuteAsync(NpgsqlDataSource db, string sql, object? args = null)
    {
        await using var conn = await db.OpenConnectionAsync();
        await conn.ExecuteAsync(sql, args);
    }

    private sealed record StoredRow(string Payload, string Status, int AttemptCount, string? Error,
        DateTimeOffset EventTime, DateTimeOffset LastUpdated);

    private sealed record Row(long Version, string Status, int AttemptCount, string? Error,
        DateTimeOffset EventTime, DateTimeOffset LastUpdated);
}