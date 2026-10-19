using System.Text.Json.Nodes;
using SalesforceSync.Data.Inbox;

namespace SalesforceSync.IntegrationTests;

/// <summary>
/// The lease, tested by playing a worker by hand: the test claims an entity with the worker's own statements
/// (<see cref="InboxStatements"/> and the repository) while the real pods run. "Another worker is mid Salesforce
/// call" lasts exactly as long as the test wants.
/// </summary>
[Collection(SyncCollection.Name)]
public sealed class LeaseTests(SyncFixture app)
{
    private static readonly InboxStatements Sql = InboxStatements.For(SyncFixture.InboxTable());

    [Fact]
    public async Task Event_arriving_while_its_entity_is_being_processed_waits_for_the_lease_then_wins_if_newer()
    {
        var acct = SyncFixture.NewAccountId();
        // "A worker" claims v1 the instant it is stored and then takes its time
        var claim = await InsertAndClaimAsync(acct, TimeSpan.FromMinutes(1),
            SyncFixture.AccountEvent(acct, 1, new JsonObject { ["name"] = "v1" }, []));
        claim.Events.Should().ContainSingle();

        // A newer event arrives meanwhile, through Kafka like any other, and overwrites the entity's row
        await app.PublishAsync(acct, 2, new JsonObject { ["name"] = "v2" }, []);
        await Eventually.ValueAsync(() => app.GetInboxRowAsync(acct), row => row?.Version == 2, "v2 to be stored");

        // The pods poll every second or faster; none of them may touch the account while it is leased
        await Task.Delay(TimeSpan.FromSeconds(2.5));
        var during = (await app.GetInboxRowAsync(acct))!;
        during.Should().Match<InboxRowState>(r => r.Version == 2 && r.Status == "pending" && r.AttemptCount == 0,
            "v2 must wait while another worker holds its account");
        during.NextAttemptAt.Should().BeAfter(DateTime.UtcNow, "the account is still leased to the first worker");
        (await app.GetRecordAsync("Account", acct)).Should().BeNull("nobody has written the account yet");

        // The first worker finishes without having written (as if its records were not attempted) and releases
        await app.AccountInbox.ReleaseAsync(claim, new Release(TimeSpan.Zero), CancellationToken.None);

        (await app.WaitForInboxSettledAsync(acct, version: 2)).Should().Be((2L, "done"));
        ((string?)(await app.GetRecordAsync("Account", acct))!["Name"]).Should().Be("v2");
        (await app.GetHistoryAsync("Account", acct)).Should().ContainSingle();
    }

    /// <summary>
    /// The consumer overwrites the row with v2 while the worker that claimed v1 writes it to Salesforce. If the
    /// worker's outcome for v1 landed on v2 (done or dead), v2 would never be written.
    /// </summary>
    [Theory]
    [InlineData("done")]
    [InlineData("failed")]
    [InlineData("dead")]
    public async Task A_newer_event_stored_while_the_claimed_one_is_written_is_written_next_whatever_the_claimed_ones_outcome(string outcome)
    {
        var acct = SyncFixture.NewAccountId();
        var claim = await InsertAndClaimAsync(acct, TimeSpan.FromMinutes(1),
            SyncFixture.AccountEvent(acct, 1, new JsonObject { ["name"] = "v1" }, []));
        var row = claim.EntityIds.Single();

        await using (var conn = await app.Db.OpenConnectionAsync())
        {
            var v2 = SyncFixture.AccountEvent(acct, 2, new JsonObject { ["name"] = "v2" }, []);
            await conn.StoreAsync(Sql.Table, acct, SyncFixture.TimeOf(v2), v2.ToJsonString());
        }
        (await app.GetInboxRowAsync(acct)).Should().Match<InboxRowState>(r => r.Version == 2 && r.Status == "pending" && r.AttemptCount == 0);
        var failure = new FailedEvent(row, "failed");
        await (outcome switch
        {
            "done" => app.AccountInbox.MarkDoneAsync(claim, [row], CancellationToken.None),
            "failed" => app.AccountInbox.MarkFailedAsync(claim, [failure], CancellationToken.None),
            _ => app.AccountInbox.MarkFailedAsync(claim, [failure with { Permanent = true }], CancellationToken.None)
        });

        (await app.WaitForInboxSettledAsync(acct, version: 2)).Should().Be((2L, "done"));
        ((string?)(await app.GetRecordAsync("Account", acct))?["Name"]).Should().Be("v2", "the pods wrote v2 once the lease ended");
    }

    [Fact]
    public async Task Worker_whose_entity_was_finished_by_another_can_record_nothing()
    {
        var acct = SyncFixture.NewAccountId();
        // "A worker" claims v1 with a lease that runs out before it finishes
        var claim = await InsertAndClaimAsync(acct, TimeSpan.FromSeconds(1),
            SyncFixture.AccountEvent(acct, 1, new JsonObject { ["name"] = "v1" }, []));

        // A real worker takes the entity over once the lease has expired, and syncs it
        (await app.WaitForInboxSettledAsync(acct, version: 1)).Should().Be((1L, "done"));
        var (claimed, times, leaseEnds) = (claim.EntityIds, claim.EventTimes, claim.LeaseEnds);
        var rowBefore = await app.GetInboxRowAsync(acct);

        // The first worker comes back and tries everything it could do with its old claim
        var errors = $$"""{ "{{claimed[0]}}": "late failure: UNABLE_TO_LOCK_ROW" }""";
        (await ExecuteAsync(Sql.Done, new { entityIds = claimed, times, leaseEnds }))
            .Should().Be(0, "an entity that is finished has nothing left to record");
        (await ExecuteAsync(Sql.Fail, new
        {
            entityIds = claimed,
            times,
            leaseEnds,
            errors,
            deadIds = Array.Empty<string>(),
            maxRetries = 8,
            baseDelaySecs = 1d,
            maxDelaySecs = 1d
        })).Should().Be(0);

        // Every outcome the repository records is conditional on the row being pending as well: none of them touches it
        var inbox = app.AccountInbox;
        var failed = new FailedEvent(claimed[0], "x");
        await inbox.MarkDoneAsync(claim, claimed, CancellationToken.None);
        await inbox.MarkFailedAsync(claim, [failed with { Permanent = true }], CancellationToken.None);
        await inbox.MarkFailedAsync(claim, [failed], CancellationToken.None);
        await inbox.ReleaseAsync(claim, new Release(TimeSpan.FromMinutes(10), "x"), CancellationToken.None);
        await inbox.ReleaseAsync(claim, new Release(TimeSpan.FromHours(1)), CancellationToken.None);

        (await app.GetInboxAsync(acct)).Should().Be((1L, "done"));
        (await app.GetInboxRowAsync(acct)).Should().Be(rowBefore, "the row is exactly as the worker that took over left it");
    }

    [Fact]
    public async Task Done_completes_an_event_only_while_it_is_pending_and_only_once()
    {
        var acct = SyncFixture.NewAccountId();
        var claim = await InsertAndClaimAsync(acct, TimeSpan.FromMinutes(1),
            SyncFixture.AccountEvent(acct, 1, new JsonObject { ["name"] = "v1" }, []));
        var claimed = claim.EntityIds;

        (await ExecuteAsync(Sql.Done, new { entityIds = claimed, times = new[] { claim.EventTimes[0].AddSeconds(1) }, leaseEnds = claim.LeaseEnds }))
            .Should().Be(0, "an event that is not the claimed one completes nothing");
        (await app.GetInboxAsync(acct)).Should().Be((1L, "pending"));

        await app.AccountInbox.MarkDoneAsync(claim, [claimed[0]], CancellationToken.None);
        await app.AccountInbox.MarkDoneAsync(claim, [claimed[0]], CancellationToken.None);

        (await app.GetInboxRowAsync(acct)).Should().Match<InboxRowState>(r => r.Status == "done" && r.NextAttemptAt == null,
            "recorded again (after a commit whose outcome was unknown), it finds the row idle and nothing is left to do");
    }

    /// <summary>
    /// Stores the event and leases its row in one transaction, as the claim statement would, so no pod can get in
    /// between (pods see the new row only once committed, leased). The claim is what the worker's own claim returns:
    /// the entity's row with the event_time and lease end it read.
    /// </summary>
    private async Task<InboxClaim> InsertAndClaimAsync(string acct, TimeSpan lease, JsonObject e)
    {
        await using var conn = await app.Db.OpenConnectionAsync();
        await using var tx = await conn.BeginTransactionAsync();
        await conn.StoreAsync(Sql.Table, acct, SyncFixture.TimeOf(e), e.ToJsonString(), tx);
        var rows = await conn.QueryAsync<InboxEvent>($"""
            UPDATE {Sql.Table} SET next_attempt_at = now() + @lease
            WHERE entity_id = @acct
            RETURNING entity_id, payload::text, event_time, next_attempt_at
            """, new { lease, acct }, tx);
        await tx.CommitAsync();
        return new InboxClaim(rows);
    }

    private async Task<int> ExecuteAsync(string sql, object args)
    {
        await using var conn = await app.Db.OpenConnectionAsync();
        return await conn.ExecuteAsync(sql, args);
    }
}