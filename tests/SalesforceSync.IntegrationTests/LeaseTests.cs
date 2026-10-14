using System.Text.Json.Nodes;
using SalesforceSync.Inbox;
using SalesforceSync.Sync;

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
        var claimed = await InsertAndClaimAsync(acct, token, TimeSpan.FromMinutes(1),
            SyncFixture.AccountEvent(acct, 1, new JsonObject { ["name"] = "v1" }, []));
        claimed.Should().ContainSingle();

        // A newer event arrives meanwhile, through Kafka like any other
        await app.PublishAsync(acct, 2, new JsonObject { ["name"] = "v2" }, []);
        await Eventually.ValueAsync(() => app.GetInboxRowsAsync(acct), rows => rows.Count == 2, "v2 to be stored");

        // The pods poll every second or faster; none of them may touch the account while it is leased
        await Task.Delay(TimeSpan.FromSeconds(2.5));
        var during = await app.GetInboxRowsAsync(acct);
        during[1].Should().Match<InboxRow>(r => r.Version == 2 && r.Status == "pending" && r.RetryCount == 0 && r.LockToken == null,
            "v2 must wait while another worker holds its account");
        during[0].LockToken.Should().Be(token);
        (await app.GetRecordAsync("Account", acct)).Should().BeNull("nobody has written the account yet");

        // The first worker finishes without having written (as if its records were not attempted) and releases
        await app.AccountInbox.RecordAsync(ClaimOf(acct, token, claimed), Outcomes(released: claimed), CancellationToken.None);

        var settled = await app.WaitForInboxSettledAsync(acct, expectedRows: 2);
        settled.Should().Equal((1L, "superseded"), (2L, "done"));
        ((string?)(await app.GetRecordAsync("Account", acct))!["Name"]).Should().Be("v2");
        (await app.GetHistoryAsync("Account", acct)).Should().ContainSingle();
    }

    [Fact]
    public async Task Worker_whose_lease_expired_can_record_nothing_and_cannot_move_the_watermark()
    {
        var acct = SyncFixture.NewAccountId();
        var stale = Guid.NewGuid();
        // "A worker" claims v1 with a lease that runs out before it finishes
        var claimed = await InsertAndClaimAsync(acct, stale, TimeSpan.FromSeconds(1),
            SyncFixture.AccountEvent(acct, 1, new JsonObject { ["name"] = "v1" }, []));

        // A real worker takes the entity over once the lease has expired, and syncs it
        (await app.WaitForInboxSettledAsync(acct, expectedRows: 1)).Should().Equal((1L, "done"));
        var watermark = await WatermarkAsync(acct);
        watermark.Should().NotBeNull();
        var rowsBefore = await app.GetInboxRowsAsync(acct);

        // The first worker comes back and tries everything it could do with its old token
        var farFuture = watermark!.Value + 1_000_000_000;
        var errors = $$"""{ "{{claimed[0]}}": ["late failure", "UNABLE_TO_LOCK_ROW"] }""";
        (await ExecuteAsync(Sql.MarkSynced, new { entityType = AccountSyncService.Type, token = stale, entityIds = new[] { acct }, times = new[] { farFuture } }))
            .Should().Be(0, "the watermark only moves for the worker that holds the lease");
        (await ExecuteAsync(Sql.Fail, new { ids = claimed, token = stale, errors, maxRetries = 8, baseDelaySecs = 1d, maxDelaySecs = 1d })).Should().Be(0);
        (await ExecuteAsync(Sql.Dead, new { ids = claimed, token = stale, errors })).Should().Be(0);

        // Every outcome the repository records is conditional on the token as well: none of them touches the row
        var inbox = app.AccountInbox;
        var claim = ClaimOf(acct, stale, claimed);
        var failed = new FailedEvent(claimed[0], "x", "UNKNOWN_OUTCOME");
        await inbox.RecordAsync(claim, new BatchOutcomes([new SyncedEntity(acct, farFuture, claimed[0])], claimed, claimed, [failed], [failed]), CancellationToken.None);
        await inbox.RecordAsync(claim, new BatchOutcomes([], [], [], [], []) { Held = [failed] },
            CancellationToken.None);
        await inbox.HoldAsync(claim, "x", CancellationToken.None);
        await inbox.FailAllAsync(claim, "x", "UNKNOWN_OUTCOME", CancellationToken.None);

        (await app.GetInboxAsync(acct)).Should().Equal((1L, "done"));
        (await app.GetInboxRowsAsync(acct)).Should().Equal(rowsBefore, "the row is exactly as the worker that took over left it");
        (await WatermarkAsync(acct)).Should().Be(watermark, "the worker that took over is unaffected");
    }

    [Fact]
    public async Task Deep_backlog_of_one_entity_is_taken_a_bounded_number_of_events_at_a_time()
    {
        var acct = SyncFixture.NewAccountId();
        var events = Enumerable.Range(1, 65)
            .Select(v => SyncFixture.AccountEvent(acct, v, new JsonObject { ["name"] = $"v{v}", ["description"] = $"v{v}" }, []))
            .ToArray();

        await app.InsertIntoInboxAsync(acct, events);

        var inbox = await app.WaitForInboxSettledAsync(acct, expectedRows: 65);
        inbox.Should().OnlyContain(r => r.Status == "done" || r.Status == "superseded");
        inbox[^1].Should().Be((65L, "done"));
        // Sync:MaxEventsPerEntity (20) events per claim, the newest of each written: 4 claims for 65 events
        var written = (await app.GetHistoryAsync("Account", acct)).Select(w => (string)w["Description"]!).ToList();
        written.Should().Equal("v20", "v40", "v60", "v65");
    }

    /// <summary>Stores the events and claims the entity in one transaction, so no pod can get in between.</summary>
    private async Task<long[]> InsertAndClaimAsync(string acct, Guid token, TimeSpan lease, params JsonObject[] events)
    {
        await using var conn = await app.Db.OpenConnectionAsync();
        await using var tx = await conn.BeginTransactionAsync();
        foreach (var e in events)
        {
            await conn.ExecuteAsync(PgSql.Insert(Sql.Table), new { entityId = acct, payload = e.ToJsonString() }, tx);
        }
        var rows = (await conn.QueryAsync<InboxEvent>(Sql.ClaimEntities,
            new { entityIds = new[] { acct }, token, lease, maxPerEntity = 20 }, tx)).ToList();
        await tx.CommitAsync();
        return [.. rows.Select(r => r.Id)];
    }

    private static InboxClaim ClaimOf(string acct, Guid token, long[] ids)
    {
        return new InboxClaim(AccountSyncService.Type, token, [.. ids.Select(id => new InboxEvent(id, acct, "{}", 1))], new Dictionary<string, long>());
    }

    private static BatchOutcomes Outcomes(long[] released)
    {
        return new BatchOutcomes([], [], released, [], []);
    }

    private async Task<int> ExecuteAsync(string sql, object args)
    {
        await using var conn = await app.Db.OpenConnectionAsync();
        return await conn.ExecuteAsync(sql, args);
    }

    private async Task<long?> WatermarkAsync(string acct)
    {
        await using var conn = await app.Db.OpenConnectionAsync();
        return await conn.ExecuteScalarAsync<long?>(
            "select synced_time from sf_sync_state where entity_type = @type and entity_id = @acct", new { type = AccountSyncService.Type, acct });
    }
}