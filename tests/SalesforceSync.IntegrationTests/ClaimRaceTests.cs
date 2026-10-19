using System.Text.Json.Nodes;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Npgsql;
using SalesforceSync.Data;
using SalesforceSync.Data.Inbox;

namespace SalesforceSync.IntegrationTests;

/// <summary>
/// Claims, stores and outcomes interleaved by hand on separate connections, in a schema of its own. Pods (which work
/// the public schema) never see its rows, and every step happens exactly when the test says. Storing an event,
/// claiming an entity and recording its outcome all update the entity's one inbox row, so Postgres's row lock orders
/// them. The schema is created once and emptied before each test (<see cref="SyncFixture.MigratedSchemaAsync"/>).
/// </summary>
[Collection(SyncCollection.Name)]
public sealed class ClaimRaceTests(SyncFixture app)
{
    private const string Schema = "claim_race_test";
    private static readonly string Table = SyncFixture.InboxTable();
    private static readonly InboxStatements Sql = InboxStatements.For(Table);
    private IDbContextFactory<SyncDbContext>? _contexts;

    [Fact]
    public async Task A_second_event_of_an_entity_waits_for_the_first_to_commit_then_overwrites_it_and_an_entity_being_stored_is_not_claimed()
    {
        await using var race = await StartAsync();
        await race.StoreAsync(race.Consumer1, 1);

        // Consumer 1 stores an event, uncommitted; consumer 2's event for the same entity waits for it
        var slowStore = await race.Consumer1.BeginTransactionAsync();
        await race.StoreAsync(race.Consumer1, 2, slowStore);
        var secondStore = race.StoreAsync(race.Consumer2, 3);
        await race.WaitUntilBlockedByAsync(race.Consumer1, secondStore);

        // Meanwhile a worker skips the entity: its row is locked
        await using (var txA = await race.WorkerA.BeginTransactionAsync())
        {
            (await race.ClaimAsync(race.WorkerA, txA)).Should().BeEmpty();
            await txA.CommitAsync();
        }

        await slowStore.CommitAsync();
        await secondStore;

        await using var tx = await race.WorkerA.BeginTransactionAsync();
        var events = await race.ClaimAsync(race.WorkerA, tx);
        await tx.CommitAsync();
        events.Should().ContainSingle("the entity has one row").Which.EventTime.Should().Be(race.TimeOf(3));
    }

    [Fact]
    public async Task A_second_worker_does_not_claim_an_entity_whose_claim_has_not_committed_yet()
    {
        await using var race = await StartAsync();
        await race.StoreAsync(race.Consumer1, 1);

        await using var txA = await race.WorkerA.BeginTransactionAsync();
        (await race.ClaimAsync(race.WorkerA, txA)).Should().ContainSingle();

        // B claims while A's claim is open: B can't see A's lease yet, but sees A's row lock and skips it
        await using (var txB = await race.WorkerB.BeginTransactionAsync())
        {
            (await race.ClaimAsync(race.WorkerB, txB)).Should().BeEmpty();
            await txB.CommitAsync();
        }

        await txA.CommitAsync();
        (await race.DueAsync()).Should().BeFalse("the entity is leased to A");
    }

    [Fact]
    public async Task Claiming_leases_nothing_of_an_entity_another_worker_holds_a_live_lease_on()
    {
        await using var race = await StartAsync();
        await race.StoreAsync(race.Consumer1, 1);

        await using (var txA = await race.WorkerA.BeginTransactionAsync())
        {
            (await race.ClaimAsync(race.WorkerA, txA)).Should().ContainSingle();
            await txA.CommitAsync();
        }

        // A newer event makes no difference: the entity stays A's until A's lease ends
        await race.StoreAsync(race.Consumer2, 2);
        await using (var txB = await race.WorkerB.BeginTransactionAsync())
        {
            (await race.ClaimAsync(race.WorkerB, txB)).Should().BeEmpty();
            await txB.CommitAsync();
        }

        (await race.DueAsync()).Should().BeFalse("the entity is leased to A");
    }

    /// <summary>
    /// The consumer holds the row lock, storing v2, when the worker records v1's outcome. The outcome statement waits
    /// for the store, then re-checks its WHERE on the row the store left, whose event_time is v2's. Nothing of v1's
    /// outcome lands on v2, the lease ends and v2 is due. (<see cref="OneRowPerEntityTests"/> covers a store that
    /// committed before the outcome.)
    /// </summary>
    [Theory]
    [MemberData(nameof(SyncFixture.Outcomes), MemberType = typeof(SyncFixture))]
    public async Task An_outcome_that_waits_for_a_store_of_a_newer_event_is_not_recorded_on_it(string outcome)
    {
        await using var race = await StartAsync();
        await race.StoreAsync(race.Consumer1, 1);
        var inbox = new InboxRepository<AccountInboxRow>(_contexts!, Options.Create(new SyncOptions()));
        var claim = await inbox.ClaimAsync(CancellationToken.None);
        claim.Events.Should().ContainSingle();

        await using (var store = await race.Consumer2.BeginTransactionAsync())
        {
            await race.StoreAsync(race.Consumer2, 2, store);
            var recording = SyncFixture.RecordOutcomeAsync(inbox, claim, outcome);
            await race.WaitUntilBlockedByAsync(race.Consumer2, recording);
            await store.CommitAsync();
            await recording;
        }

        (await race.RowAsync()).Should().Be((race.TimeOf(2), "pending", 0, null), "nothing of v1's outcome lands on v2");
        (await race.StateAsync()).Should().NotBeNull("v2 is pending, so it must be due, or it would never be claimed");
        (await race.DueAsync()).Should().Be(outcome != "held",
            "v2 is due when the outcome transaction ends (held: when the hold ends), not when v1's lease would have");
    }

    /// <summary>
    /// The worker's outcome transaction holds the row lock when the consumer stores a newer event. The store waits,
    /// then overwrites the row as the transaction left it, pending again and due. Either way round: the claimed event
    /// recorded done (row finished and idle), or the row already overwritten during the lease and only its lease
    /// ended.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task An_event_stored_while_the_outcome_transaction_has_the_row_overwrites_it_and_leaves_it_due(bool overwrittenDuringTheLease)
    {
        await using var race = await StartAsync();
        await race.StoreAsync(race.Consumer1, 1);
        List<InboxEvent> claimed;
        await using (var tx = await race.WorkerA.BeginTransactionAsync())
        {
            claimed = await race.ClaimAsync(race.WorkerA, tx);
            await tx.CommitAsync();
        }
        if (overwrittenDuringTheLease)
        {
            await race.StoreAsync(race.Consumer1, 2);
        }

        // The worker records v1 done and ends the lease, as the repository does; consumer 2 stores v3 meanwhile
        await using (var record = await race.WorkerA.BeginTransactionAsync())
        {
            var args = new { entityIds = claimed.Select(e => e.EntityId).ToArray(), times = claimed.Select(e => e.EventTime).ToArray() };
            (await race.WorkerA.ExecuteAsync(Sql.Done, args, record)).Should().Be(overwrittenDuringTheLease ? 0 : 1);
            (await race.WorkerA.ExecuteAsync(Sql.EndLease, new { args.entityIds, delay = TimeSpan.Zero }, record))
                .Should().Be(overwrittenDuringTheLease ? 1 : 0, "only an overwritten row is left under the lease");
            var lateStore = race.StoreAsync(race.Consumer2, 3);
            await race.WaitUntilBlockedByAsync(race.WorkerA, lateStore);
            await record.CommitAsync();
            await lateStore;
        }

        (await race.RowAsync()).Should().Be((race.TimeOf(3), "pending", 0, null));
        (await race.StateAsync()).Should().NotBeNull("v3 is pending, so it must be due, or it would never be claimed");
        (await race.DueAsync()).Should().BeTrue("v3 is due now, not when v1's lease would have ended");
    }

    /// <summary>
    /// A store of a newer event and the end of the old lease can start in the same microsecond (a transaction's now() is its
    /// start time). The lease must end all the same: ending it may not depend on the row's last_updated. Played in one
    /// transaction, so both statements have the very same now().
    /// </summary>
    [Fact]
    public async Task A_lease_ends_on_a_row_a_store_updated_at_the_very_same_time()
    {
        await using var race = await StartAsync();
        await race.StoreAsync(race.Consumer1, 1);
        List<InboxEvent> claimed;
        await using (var tx = await race.WorkerA.BeginTransactionAsync())
        {
            claimed = await race.ClaimAsync(race.WorkerA, tx);
            await tx.CommitAsync();
        }

        await using (var tx = await race.Consumer1.BeginTransactionAsync())
        {
            await race.StoreAsync(race.Consumer1, 2, tx);
            var ended = await race.Consumer1.ExecuteAsync(Sql.EndLease,
                new { entityIds = claimed.Select(e => e.EntityId).ToArray(), delay = TimeSpan.Zero }, tx);
            ended.Should().Be(1, "the row is pending under a lease whatever its last_updated is");
            await tx.CommitAsync();
        }

        (await race.DueAsync()).Should().BeTrue("v2 is due now, not when v1's lease would have ended");
    }

    /// <summary>The test's schema, migrated, with no rows, and a race in it.</summary>
    private async Task<Race> StartAsync()
    {
        var (db, contexts) = await app.MigratedSchemaAsync(Schema);
        _contexts = contexts;
        return new Race(await db.OpenConnectionAsync(), await db.OpenConnectionAsync(), await db.OpenConnectionAsync(),
            await db.OpenConnectionAsync(), await db.OpenConnectionAsync());
    }

    /// <summary>One entity, and connections for two consumers and two workers.</summary>
    private sealed class Race(NpgsqlConnection consumer1, NpgsqlConnection consumer2, NpgsqlConnection workerA, NpgsqlConnection workerB,
        NpgsqlConnection observer) : IAsyncDisposable
    {
        public NpgsqlConnection Consumer1 { get; } = consumer1;
        public NpgsqlConnection Consumer2 { get; } = consumer2;
        public NpgsqlConnection WorkerA { get; } = workerA;
        public NpgsqlConnection WorkerB { get; } = workerB;

        private string Entity { get; } = SyncFixture.NewAccountId();

        public Task StoreAsync(NpgsqlConnection consumer, int version, NpgsqlTransaction? tx = null)
        {
            var e = SyncFixture.AccountEvent(Entity, version, new JsonObject { ["name"] = $"v{version}" }, []);
            return consumer.StoreAsync(Table, Entity, SyncFixture.TimeOf(e), e.ToJsonString(), tx);
        }

        /// <summary>The event_time version N is stored with.</summary>
        public DateTimeOffset TimeOf(int version)
        {
            return SyncFixture.BaseTime.AddMilliseconds(version);
        }

        /// <summary>The entity's inbox row: its event_time, status, attempts and error.</summary>
        public async Task<(DateTimeOffset EventTime, string Status, int AttemptCount, string? Error)> RowAsync()
        {
            var (time, status, attempts, error) = await observer.QuerySingleAsync<(DateTime, string, int, string?)>(
                $"SELECT event_time, {SyncFixture.StatusSql}, attempt_count, last_error FROM {Table} WHERE entity_id = @Entity", new { Entity });
            return (new DateTimeOffset(time), status, attempts, error);
        }

        public Task<List<InboxEvent>> ClaimAsync(NpgsqlConnection worker, NpgsqlTransaction tx)
        {
            return worker.QueryAsync<InboxEvent>(Sql.Claim, new { lease = TimeSpan.FromMinutes(1), batchSize = 75 }, tx);
        }

        public Task<DateTime?> StateAsync()
        {
            return observer.QuerySingleAsync<DateTime?>($"SELECT next_attempt_at FROM {Table} WHERE entity_id = @Entity", new { Entity });
        }

        /// <summary>Whether the entity is due now (null: idle).</summary>
        public Task<bool?> DueAsync()
        {
            return observer.QuerySingleAsync<bool?>($"SELECT next_attempt_at <= now() FROM {Table} WHERE entity_id = @Entity", new { Entity });
        }

        /// <summary>Waits until <paramref name="statement"/> is blocked on a lock that <paramref name="holder"/>'s transaction holds.</summary>
        public async Task WaitUntilBlockedByAsync(NpgsqlConnection holder, Task statement)
        {
            var deadline = DateTime.UtcNow.AddSeconds(10);
            while (!await observer.QuerySingleAsync<bool>(
                       "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE @pid = ANY(pg_blocking_pids(pid)))", new { pid = holder.ProcessID }))
            {
                statement.IsCompleted.Should().BeFalse("the statement should be waiting for a lock");
                DateTime.UtcNow.Should().BeBefore(deadline, "the statement should be waiting for a lock");
                await Task.Delay(20);
            }
        }

        public async ValueTask DisposeAsync()
        {
            foreach (var conn in new[] { Consumer1, Consumer2, WorkerA, WorkerB, observer })
            {
                await conn.DisposeAsync();
            }
        }
    }
}