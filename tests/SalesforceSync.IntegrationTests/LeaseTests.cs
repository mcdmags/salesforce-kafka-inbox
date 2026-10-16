using System.Text.Json.Nodes;
using SalesforceSync.Inbox;

namespace SalesforceSync.IntegrationTests;

/// <summary>
/// The lease, tested by playing a worker by hand: the test claims an entity with the worker's own statements
/// (<see cref="InboxSql"/> and the repository) while the real pods run, so "another worker is in the middle of its Salesforce call"
/// lasts exactly as long as the test wants.
/// </summary>
[Collection(SyncCollection.Name)]
public sealed class LeaseTests(SyncFixture app)
{
    private static readonly InboxSql Sql = InboxSql.For(SyncFixture.InboxTable());

    [Fact]
    public async Task Event_arriving_while_its_entity_is_being_processed_waits_for_the_lease_then_wins_if_newer()
    {
        var acct = SyncFixture.NewAccountId();
        var token = Guid.NewGuid();
        // "A worker" claims v1 the instant it is stored and then takes its time
        var claim = await InsertAndClaimAsync(acct, token, TimeSpan.FromMinutes(1),
            SyncFixture.AccountEvent(acct, 1, new JsonObject { ["name"] = "v1" }, []));
        claim.Events.Should().ContainSingle();

        // A newer event arrives meanwhile, through Kafka like any other, and overwrites the entity's row
        await app.PublishAsync(acct, 2, new JsonObject { ["name"] = "v2" }, []);
        await Eventually.ValueAsync(() => app.GetInboxRowAsync(acct), row => row?.Version == 2, "v2 to be stored");

        // The pods poll every second or faster; none of them may touch the account while it is leased
        await Task.Delay(TimeSpan.FromSeconds(2.5));
        var during = (await app.GetInboxRowAsync(acct))!;
        during.Should().Match<InboxRow>(r => r.Version == 2 && r.Status == "pending" && r.AttemptCount == 0,
            "v2 must wait while another worker holds its account");
        during.LockToken.Should().Be(token, "the account is still leased to the first worker");
        (await app.GetRecordAsync("Account", acct)).Should().BeNull("nobody has written the account yet");

        // The first worker finishes without having written (as if its records were not attempted) and releases
        await app.AccountInbox.RecordAsync(claim, new BatchOutcomes([], [], []), CancellationToken.None);

        (await app.WaitForInboxSettledAsync(acct, version: 2)).Should().Be((2L, "done"));
        ((string?)(await app.GetRecordAsync("Account", acct))!["Name"]).Should().Be("v2");
        (await app.GetHistoryAsync("Account", acct)).Should().ContainSingle();
    }

    /// <summary>
    /// The consumer overwrites the row with v2 while the worker that claimed v1 is writing it to Salesforce. Had the
    /// worker's outcome for v1 landed on v2 (done or dead), v2 would never be written.
    /// </summary>
    [Theory]
    [InlineData("done")]
    [InlineData("failed")]
    [InlineData("dead")]
    public async Task A_newer_event_stored_while_the_claimed_one_is_written_is_written_next_whatever_the_claimed_ones_outcome(string outcome)
    {
        var acct = SyncFixture.NewAccountId();
        var token = Guid.NewGuid();
        var claim = await InsertAndClaimAsync(acct, token, TimeSpan.FromMinutes(1),
            SyncFixture.AccountEvent(acct, 1, new JsonObject { ["name"] = "v1" }, []));
        var row = claim.RowIds.Single();

        await using (var conn = await app.Db.OpenConnectionAsync())
        {
            var v2 = SyncFixture.AccountEvent(acct, 2, new JsonObject { ["name"] = "v2" }, []);
            await conn.StoreAsync(Sql.Table, acct, SyncFixture.TimeOf(v2), v2.ToJsonString());
        }
        (await app.GetInboxRowAsync(acct)).Should().Match<InboxRow>(r => r.Id == row && r.Version == 2 && r.Status == "pending" && r.AttemptCount == 0);
        var failure = new FailedEvent(row, "failed", "SOME_CODE");
        await app.AccountInbox.RecordAsync(claim, outcome switch
        {
            "done" => Synced(row),
            "failed" => new BatchOutcomes([], [], [failure]),
            _ => new BatchOutcomes([], [failure], [])
        }, CancellationToken.None);

        (await app.WaitForInboxSettledAsync(acct, version: 2)).Should().Be((2L, "done"));
        ((string?)(await app.GetRecordAsync("Account", acct))?["Name"]).Should().Be("v2", "the pods wrote v2 once the lease ended");
    }

    [Fact]
    public async Task Worker_whose_lease_expired_can_record_nothing()
    {
        var acct = SyncFixture.NewAccountId();
        var stale = Guid.NewGuid();
        // "A worker" claims v1 with a lease that runs out before it finishes
        var claim = await InsertAndClaimAsync(acct, stale, TimeSpan.FromSeconds(1),
            SyncFixture.AccountEvent(acct, 1, new JsonObject { ["name"] = "v1" }, []));

        // A real worker takes the entity over once the lease has expired, and syncs it
        (await app.WaitForInboxSettledAsync(acct, version: 1)).Should().Be((1L, "done"));
        // The stale worker's claim gets the row's new time, so that only the token stands in its way
        await ExecuteAsync($"update {Sql.Table} set event_time = event_time + interval '1000 seconds' where id = @id", new { id = claim.RowIds[0] });
        await using (var conn = await app.Db.OpenConnectionAsync())
        {
            var time = await conn.QuerySingleAsync<DateTime>($"select event_time from {Sql.Table} where entity_id = @acct", new { acct });
            claim = claim with { Events = [claim.Events.Single() with { EventTime = new DateTimeOffset(time) }] };
        }
        var (claimed, times) = (claim.RowIds, claim.EventTimes);
        var rowBefore = await app.GetInboxRowAsync(acct);
        var stateBefore = await app.GetSyncStateAsync(acct);

        // The first worker comes back and tries everything it could do with its old token
        var errors = $$"""{ "{{claimed[0]}}": ["late failure", "UNABLE_TO_LOCK_ROW"] }""";
        (await ExecuteAsync(Sql.Done, new { ids = claimed, times, token = stale }))
            .Should().Be(0, "only the worker that holds the lease records anything");
        (await ExecuteAsync(Sql.Fail, new
        {
            ids = claimed,
            times,
            token = stale,
            errors,
            maxRetries = 8,
            baseDelaySecs = 1d,
            maxDelaySecs = 1d
        })).Should().Be(0);

        // Every outcome the repository records is conditional on the token as well: none of them touches the row
        var inbox = app.AccountInbox;
        var failed = new FailedEvent(claimed[0], "x", "UNKNOWN_OUTCOME");
        await inbox.RecordAsync(claim, new BatchOutcomes(claimed, [failed], [failed]), CancellationToken.None);
        await inbox.HoldAsync(claim, "x", CancellationToken.None);
        await inbox.DeferAsync(claim, TimeSpan.FromHours(1), CancellationToken.None);
        await inbox.FailAllAsync(claim, "x", "UNKNOWN_OUTCOME", CancellationToken.None);

        (await app.GetInboxAsync(acct)).Should().Be((1L, "done"));
        (await app.GetInboxRowAsync(acct)).Should().Be(rowBefore, "the row is exactly as the worker that took over left it");
        (await app.GetSyncStateAsync(acct)).Should().Be(stateBefore, "the worker that took over is unaffected");
    }

    [Fact]
    public async Task Done_completes_an_event_only_under_its_lease_and_only_once()
    {
        var acct = SyncFixture.NewAccountId();
        var token = Guid.NewGuid();
        var claim = await InsertAndClaimAsync(acct, token, TimeSpan.FromMinutes(1),
            SyncFixture.AccountEvent(acct, 1, new JsonObject { ["name"] = "v1" }, []));
        var claimed = claim.RowIds;

        (await ExecuteAsync(Sql.Done, new { ids = claimed, times = claim.EventTimes, token = Guid.NewGuid() }))
            .Should().Be(0, "a lost lease completes nothing");
        (await app.GetInboxAsync(acct)).Should().Be((1L, "pending"));

        await app.AccountInbox.RecordAsync(claim, Synced(claimed[0]), CancellationToken.None);
        await app.AccountInbox.RecordAsync(claim, Synced(claimed[0]), CancellationToken.None);

        var row = (await app.GetInboxRowAsync(acct))!;
        row.Should().Match<InboxRow>(r => r.Status == "done" && r.LockToken == null && r.NextAttemptAt == null);
        (await app.GetSyncStateAsync(acct)).Should().Be(new SyncStateRow(null, null),
            "recorded again (after a commit whose outcome was unknown), it finds the lease already ended and nothing is left to do");
    }

    /// <summary>
    /// Stores the event and leases its row in one transaction, as InboxSql.Claim would lease it, so no pod can get in
    /// between (the new row is theirs to see only once it is committed, leased). The claim is what the worker's own claim
    /// returns: the entity's row with the event_time it read.
    /// </summary>
    private async Task<InboxClaim> InsertAndClaimAsync(string acct, Guid token, TimeSpan lease, JsonObject e)
    {
        await using var conn = await app.Db.OpenConnectionAsync();
        await using var tx = await conn.BeginTransactionAsync();
        await conn.StoreAsync(Sql.Table, acct, SyncFixture.TimeOf(e), e.ToJsonString(), tx);
        var rows = await conn.QueryAsync<InboxEvent>($"""
            update {Sql.Table} set lock_token = @token, next_attempt_at = now() + @lease
            where entity_id = @acct
            returning id, entity_id, payload::text, event_time
            """, new { token, lease, acct }, tx);
        await tx.CommitAsync();
        return new InboxClaim(token, rows);
    }

    private static BatchOutcomes Synced(long id)
    {
        return new BatchOutcomes([id], [], []);
    }

    private async Task<int> ExecuteAsync(string sql, object args)
    {
        await using var conn = await app.Db.OpenConnectionAsync();
        return await conn.ExecuteAsync(sql, args);
    }
}