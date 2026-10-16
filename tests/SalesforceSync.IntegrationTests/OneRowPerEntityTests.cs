using System.Text.Json.Nodes;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Npgsql;
using SalesforceSync.Data;
using SalesforceSync.Inbox;
using SalesforceSync.Sync;

namespace SalesforceSync.IntegrationTests;

/// <summary>
/// An inbox keeps one row per entity: storing an event overwrites the entity's row unless that holds a newer event,
/// and an outcome is recorded on a claimed row only while it still holds the event the claim read. Through the
/// worker's own repository, in a schema of its own, so the pods (which work the public schema) never see its rows and
/// every step happens exactly when the test says. The schema is created once and emptied before each test: a data
/// source per test would cost EF a service provider each, and more than twenty fail the run.
/// </summary>
[Collection(SyncCollection.Name)]
public sealed class OneRowPerEntityTests(SyncFixture app)
{
    private const string Schema = "one_row_per_entity_test";
    private static readonly string Table = SyncFixture.InboxTable();
    private static NpgsqlDataSource? _db;
    private static IDbContextFactory<SyncDbContext>? _contexts;

    [Theory]
    [InlineData("pending")]
    [InlineData("done")]
    [InlineData("superseded")]
    [InlineData("dead")]
    public async Task A_newer_event_overwrites_the_row_and_makes_it_pending_with_no_attempts_or_errors(string status)
    {
        var db = await SchemaAsync();
        var inbox = Inbox();
        var entity = SyncFixture.NewAccountId();
        await StoreAsync(inbox, entity, 1);
        await ExecuteAsync(db, $"update {Table} set status = @status, attempt_count = 3, last_error = 'failed', last_error_code = 'SOME_CODE'",
            new { status });
        var before = await RowAsync(db, entity);

        await StoreAsync(inbox, entity, 2);

        var after = await RowAsync(db, entity);
        after.Should().Be(new Row(before.Id, 2, "pending", 0, null, null, SyncFixture.TimeOf(Event(entity, 2)), after.ReceivedAt),
            "the entity's one row now holds the newer event, as a new event");
        after.ReceivedAt.Should().BeAfter(before.ReceivedAt);
    }

    [Fact]
    public async Task An_older_event_changes_nothing()
    {
        var db = await SchemaAsync();
        var inbox = Inbox();
        var entity = SyncFixture.NewAccountId();
        await StoreAsync(inbox, entity, 2);
        await ExecuteAsync(db, $"update {Table} set status = 'done'");
        var before = await RowAsync(db, entity);

        await StoreAsync(inbox, entity, 1);

        (await RowAsync(db, entity)).Should().Be(before);
    }

    [Fact]
    public async Task An_event_as_new_as_the_row_a_redelivery_makes_it_pending_again_and_its_claim_reads_it_with_its_watermark()
    {
        var db = await SchemaAsync();
        var inbox = Inbox();
        var entity = SyncFixture.NewAccountId();
        await StoreAsync(inbox, entity, 1);
        var first = await inbox.ClaimAsync(CancellationToken.None);
        await inbox.RecordAsync(first, Synced(first), CancellationToken.None);

        await StoreAsync(inbox, entity, 1);

        (await RowAsync(db, entity)).Should().Match<Row>(r => r.Version == 1 && r.Status == "pending" && r.AttemptCount == 0);
        var again = await inbox.ClaimAsync(CancellationToken.None);
        again.Events.Should().ContainSingle().Which.Should().Match<InboxEvent>(e => e.EventTime == SyncFixture.TimeOf(Event(entity, 1))
            && e.SyncedTime == SyncFixture.SyncedTime(1),
            "the engine supersedes an event that is not newer than the watermark (SyncEndToEndTests: without a write)");
    }

    /// <summary>
    /// The race the one row per entity brings: a worker claims v1 and, while it writes v1 to Salesforce, the consumer
    /// overwrites the row with v2. Whatever the worker records for v1 must not land on v2: the row stays pending with v2,
    /// the watermark is not moved, and the entity is due, so the next claim takes v2.
    /// </summary>
    [Theory]
    [InlineData("done")]
    [InlineData("failed")]
    [InlineData("dead")]
    [InlineData("superseded")]
    [InlineData("released")]
    [InlineData("failed all")]
    [InlineData("held")]
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
        await RecordAsync(inbox, claim, outcome);

        (await RowAsync(db, entity)).Should().Match<Row>(r => r.Version == 2 && r.Status == "pending" && r.AttemptCount == 0
                                                             && r.Error == null && r.ErrorCode == null, "nothing of v1's outcome lands on v2");
        var state = await StateAsync(db, entity);
        state.SyncedTime.Should().BeNull("the watermark doesn't move to v2, which was never written");
        state.LockToken.Should().BeNull();
        state.NextAttemptAt.Should().NotBeNull("v2 is pending, so its entity must be due");

        var next = await Eventually.ValueAsync(() => inbox.ClaimAsync(CancellationToken.None), c => c.Events.Count > 0,
            "the entity to be claimed again", TimeSpan.FromSeconds(10));
        next.Events.Should().ContainSingle().Which.EventTime.Should().Be(SyncFixture.TimeOf(Event(entity, 2)), "the next claim writes v2");
        await inbox.RecordAsync(next, Synced(next), CancellationToken.None);
        (await RowAsync(db, entity)).Should().Match<Row>(r => r.Version == 2 && r.Status == "done");
        (await StateAsync(db, entity)).SyncedTime.Should().Be(SyncFixture.SyncedTime(2));
    }

    private static Task RecordAsync(IRepository<AccountInboxRow> inbox, InboxClaim claim, string outcome)
    {
        var row = claim.RowIds.Single();
        var failure = new FailedEvent(row, "failed", "SOME_CODE");
        var none = CancellationToken.None;
        return outcome switch
        {
            "done" => inbox.RecordAsync(claim, Synced(claim), none),
            "failed" => inbox.RecordAsync(claim, new BatchOutcomes([], [], [], [], [failure]), none),
            "dead" => inbox.RecordAsync(claim, new BatchOutcomes([], [], [], [failure], []), none),
            "superseded" => inbox.RecordAsync(claim, new BatchOutcomes([], [row], [], [], []), none),
            "released" => inbox.RecordAsync(claim, new BatchOutcomes([], [], [row], [], []), none),
            "failed all" => inbox.FailAllAsync(claim, "failed", "SOME_CODE", none),
            "held" => inbox.HoldAsync(claim, "lost", none),
            _ => throw new ArgumentOutOfRangeException(nameof(outcome))
        };
    }

    private static BatchOutcomes Synced(InboxClaim claim)
    {
        return new BatchOutcomes(claim.RowIds, [], [], [], []);
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

    /// <summary>The test's schema, migrated, with no rows.</summary>
    private async Task<NpgsqlDataSource> SchemaAsync()
    {
        if (_db is null)
        {
            var db = await app.CreateSchemaAsync(Schema);
            _contexts = SyncDbContext.CreateFactory(db);
            await SyncSchema.MigrateAsync(_contexts);
            _db = db;
        }
        await ExecuteAsync(_db, $"truncate {Table}");
        return _db;
    }

    /// <summary>The worker's repository; a hold short enough to see it end.</summary>
    private static InboxRepository<AccountInboxRow> Inbox()
    {
        return new InboxRepository<AccountInboxRow>(_contexts!, Options.Create(new SyncOptions
        {
            CoalesceDelay = TimeSpan.Zero,
            UnknownOutcomeHold = TimeSpan.FromMilliseconds(200)
        }));
    }

    private static async Task<Row> RowAsync(NpgsqlDataSource db, string entity)
    {
        await using var conn = await db.OpenConnectionAsync();
        var row = await conn.QuerySingleAsync<StoredRow>(
            $"select id, payload::text, status, attempt_count, last_error, last_error_code, event_time, received_at from {Table} where entity_id = @entity",
            new { entity });
        var version = (long)(SyncFixture.TimeOf(JsonNode.Parse(row.Payload)!.AsObject()) - SyncFixture.BaseTime).TotalMilliseconds;
        return new Row(row.Id, version, row.Status, row.AttemptCount, row.Error, row.ErrorCode, row.EventTime, row.ReceivedAt);
    }

    private static async Task<SyncStateRow> StateAsync(NpgsqlDataSource db, string entity)
    {
        await using var conn = await db.OpenConnectionAsync();
        return await conn.QuerySingleAsync<SyncStateRow>(
            $"select next_attempt_at, locked_until, lock_token, synced_time from {Table} where entity_id = @entity", new { entity });
    }

    private static async Task ExecuteAsync(NpgsqlDataSource db, string sql, object? args = null)
    {
        await using var conn = await db.OpenConnectionAsync();
        await conn.ExecuteAsync(sql, args);
    }

    /// <summary>An inbox row as read, its payload as stored.</summary>
    private sealed record StoredRow(long Id, string Payload, string Status, int AttemptCount, string? Error, string? ErrorCode,
        DateTimeOffset EventTime, DateTimeOffset ReceivedAt);

    /// <summary>An inbox row as these tests compare it; the version is read from its payload.</summary>
    private sealed record Row(long Id, long Version, string Status, int AttemptCount, string? Error, string? ErrorCode,
        DateTimeOffset EventTime, DateTimeOffset ReceivedAt);
}