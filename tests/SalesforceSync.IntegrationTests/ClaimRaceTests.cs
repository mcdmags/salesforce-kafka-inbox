using System.Text.Json.Nodes;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Npgsql;
using SalesforceSync.Data;
using SalesforceSync.Inbox;
using SalesforceSync.Sync;

namespace SalesforceSync.IntegrationTests;

/// <summary>
/// Claims, stores and outcomes interleaved by hand on separate connections, in a schema of its own, so the pods (which
/// work the public schema) never see its rows and every step happens exactly when the test says. Storing an event,
/// claiming an entity and recording its outcome all update the entity's one inbox row, so Postgres's row lock orders
/// them. The schema is created once and emptied before each test (see <see cref="OneRowPerEntityTests"/>).
/// </summary>
[Collection(SyncCollection.Name)]
public sealed class ClaimRaceTests(SyncFixture app)
{
    private const string Schema = "claim_race_test";
    private static readonly string Table = SyncFixture.InboxTable();
    private static readonly InboxSql Sql = InboxSql.For(Table);
    private static NpgsqlDataSource? _db;
    private static IDbContextFactory<SyncDbContext>? _contexts;

    [Fact]
    public async Task A_second_event_of_an_entity_waits_for_the_first_to_commit_then_overwrites_it_and_an_entity_being_stored_is_not_claimed()
    {
        await using var race = await StartAsync();
        await race.StoreAsync(race.Consumer1, 1);

        // Consumer 1 stores an event and hasn't committed; consumer 2's event of the same entity has to wait for it
        var slowStore = await race.Consumer1.BeginTransactionAsync();
        await race.StoreAsync(race.Consumer1, 2, slowStore);
        var secondStore = race.StoreAsync(race.Consumer2, 3);
        await race.WaitUntilBlockedByAsync(race.Consumer1, secondStore);

        // Meanwhile a worker skips the entity: its row is locked
        await using (var txA = await race.WorkerA.BeginTransactionAsync())
        {
            (await race.ClaimAsync(race.WorkerA, txA, Guid.NewGuid())).Should().BeEmpty();
            await txA.CommitAsync();
        }

        await slowStore.CommitAsync();
        await secondStore;

        await using var tx = await race.WorkerA.BeginTransactionAsync();
        var events = await race.ClaimAsync(race.WorkerA, tx, Guid.NewGuid());
        await tx.CommitAsync();
        events.Should().ContainSingle("the entity has one row").Which.EventTime.Should().Be(race.TimeOf(3));
    }

    [Fact]
    public async Task A_second_worker_does_not_claim_an_entity_whose_claim_has_not_committed_yet()
    {
        await using var race = await StartAsync();
        await race.StoreAsync(race.Consumer1, 1);

        var tokenA = Guid.NewGuid();
        await using var txA = await race.WorkerA.BeginTransactionAsync();
        (await race.ClaimAsync(race.WorkerA, txA, tokenA)).Should().ContainSingle();

        // B claims while A's claim is still open: A's lease isn't visible to B yet, but A's row lock is, and B skips it
        await using (var txB = await race.WorkerB.BeginTransactionAsync())
        {
            (await race.ClaimAsync(race.WorkerB, txB, Guid.NewGuid())).Should().BeEmpty();
            await txB.CommitAsync();
        }

        await txA.CommitAsync();
        (await race.StateAsync()).LockToken.Should().Be(tokenA);
    }

    [Fact]
    public async Task Claiming_leases_nothing_of_an_entity_another_worker_holds_a_live_lease_on()
    {
        await using var race = await StartAsync();
        await race.StoreAsync(race.Consumer1, 1);

        var tokenA = Guid.NewGuid();
        await using (var txA = await race.WorkerA.BeginTransactionAsync())
        {
            (await race.ClaimAsync(race.WorkerA, txA, tokenA)).Should().ContainSingle();
            await txA.CommitAsync();
        }

        // A newer event makes no difference: the entity stays A's until A's lease ends
        await race.StoreAsync(race.Consumer2, 2);
        await using (var txB = await race.WorkerB.BeginTransactionAsync())
        {
            (await race.ClaimAsync(race.WorkerB, txB, Guid.NewGuid())).Should().BeEmpty();
            await txB.CommitAsync();
        }

        (await race.StateAsync()).LockToken.Should().Be(tokenA);
    }

    /// <summary>
    /// The consumer has the row locked, storing v2, when the worker records v1's outcome: the outcome statement waits
    /// for the store and then checks its WHERE again on the row the store left, whose event_time is v2's. Nothing of
    /// v1's outcome lands on v2, the lease ends and v2 is due. (<see cref="OneRowPerEntityTests"/> covers a store that
    /// committed before the outcome.)
    /// </summary>
    [Theory]
    [InlineData("done")]
    [InlineData("failed")]
    [InlineData("dead")]
    [InlineData("released")]
    [InlineData("failed all")]
    [InlineData("deferred")]
    [InlineData("held")]
    public async Task An_outcome_that_waits_for_a_store_of_a_newer_event_is_not_recorded_on_it(string outcome)
    {
        await using var race = await StartAsync();
        await race.StoreAsync(race.Consumer1, 1);
        var inbox = new InboxRepository<AccountInboxRow>(_contexts!, Options.Create(new SyncOptions { CoalesceDelay = TimeSpan.Zero }));
        var claim = await inbox.ClaimAsync(CancellationToken.None);
        claim.Events.Should().ContainSingle();

        await using (var store = await race.Consumer2.BeginTransactionAsync())
        {
            await race.StoreAsync(race.Consumer2, 2, store);
            var recording = Record(inbox, claim, outcome);
            await race.WaitUntilBlockedByAsync(race.Consumer2, recording);
            await store.CommitAsync();
            await recording;
        }

        (await race.RowAsync()).Should().Be((race.TimeOf(2), "pending", 0, null), "nothing of v1's outcome lands on v2");
        var state = await race.StateAsync();
        state.LockToken.Should().BeNull("the lease ended");
        state.NextAttemptAt.Should().NotBeNull("v2 is pending, so it must be due, or it would never be claimed");
        (await race.DueAsync()).Should().Be(outcome != "held",
            "v2 is due when the outcome transaction ends (held: when the hold ends), not when v1's lease would have");
    }

    /// <summary>
    /// The worker's outcome transaction has the row locked when the consumer stores a newer event: the store waits for
    /// it, then overwrites the row as the transaction left it, pending again and due. Either way round: the claimed
    /// event recorded done (the row finished and idle), or the row already overwritten during the lease and only its
    /// lease ended.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task An_event_stored_while_the_outcome_transaction_has_the_row_overwrites_it_and_leaves_it_due(bool overwrittenDuringTheLease)
    {
        await using var race = await StartAsync();
        await race.StoreAsync(race.Consumer1, 1);
        var token = Guid.NewGuid();
        List<InboxEvent> claimed;
        await using (var tx = await race.WorkerA.BeginTransactionAsync())
        {
            claimed = await race.ClaimAsync(race.WorkerA, tx, token);
            await tx.CommitAsync();
        }
        if (overwrittenDuringTheLease)
        {
            await race.StoreAsync(race.Consumer1, 2);
        }

        // The worker records v1 done and ends the lease, as the repository does; consumer 2 stores v3 meanwhile
        await using (var record = await race.WorkerA.BeginTransactionAsync())
        {
            var args = new { ids = claimed.Select(e => e.RowId).ToArray(), times = claimed.Select(e => e.EventTime).ToArray(), token };
            (await race.WorkerA.ExecuteAsync(Sql.Done, args, record)).Should().Be(overwrittenDuringTheLease ? 0 : 1);
            (await race.WorkerA.ExecuteAsync(Sql.EndLease, new { args.ids, token, delay = TimeSpan.Zero }, record))
                .Should().Be(overwrittenDuringTheLease ? 1 : 0, "only an overwritten row is left under the lease");
            var lateStore = race.StoreAsync(race.Consumer2, 3);
            await race.WaitUntilBlockedByAsync(race.WorkerA, lateStore);
            await record.CommitAsync();
            await lateStore;
        }

        (await race.RowAsync()).Should().Be((race.TimeOf(3), "pending", 0, null));
        var state = await race.StateAsync();
        state.LockToken.Should().BeNull();
        state.NextAttemptAt.Should().NotBeNull("v3 is pending, so it must be due, or it would never be claimed");
        (await race.DueAsync()).Should().BeTrue("v3 is due now, not when v1's lease would have ended");
    }

    private static Task Record(IRepository<AccountInboxRow> inbox, InboxClaim claim, string outcome)
    {
        var row = claim.RowIds.Single();
        var failure = new FailedEvent(row, "failed", "SOME_CODE");
        var none = CancellationToken.None;
        return outcome switch
        {
            "done" => inbox.RecordAsync(claim, new BatchOutcomes([row], [], [], []), none),
            "failed" => inbox.RecordAsync(claim, new BatchOutcomes([], [], [], [failure]), none),
            "dead" => inbox.RecordAsync(claim, new BatchOutcomes([], [], [failure], []), none),
            "released" => inbox.RecordAsync(claim, new BatchOutcomes([], [row], [], []), none),
            "failed all" => inbox.FailAllAsync(claim, "failed", "SOME_CODE", none),
            "deferred" => inbox.DeferAsync(claim, TimeSpan.Zero, none),
            "held" => inbox.HoldAsync(claim, "lost", none),
            _ => throw new ArgumentOutOfRangeException(nameof(outcome))
        };
    }

    /// <summary>The test's schema, migrated, with no rows, and a race in it.</summary>
    private async Task<Race> StartAsync()
    {
        if (_db is null)
        {
            var db = await app.CreateSchemaAsync(Schema);
            _contexts = SyncDbContext.CreateFactory(db);
            await SyncSchema.MigrateAsync(_contexts);
            _db = db;
        }
        await using (var conn = await _db.OpenConnectionAsync())
        {
            await conn.ExecuteAsync($"truncate {Table}");
        }
        return new Race(await _db.OpenConnectionAsync(), await _db.OpenConnectionAsync(), await _db.OpenConnectionAsync(),
            await _db.OpenConnectionAsync(), await _db.OpenConnectionAsync());
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
                $"select event_time, status, attempt_count, last_error from {Table} where entity_id = @Entity", new { Entity });
            return (new DateTimeOffset(time), status, attempts, error);
        }

        public Task<List<InboxEvent>> ClaimAsync(NpgsqlConnection worker, NpgsqlTransaction tx, Guid token)
        {
            return worker.QueryAsync<InboxEvent>(Sql.Claim, new { token, lease = TimeSpan.FromMinutes(1), batchSize = 75 }, tx);
        }

        public Task<SyncStateRow> StateAsync()
        {
            return observer.QuerySingleAsync<SyncStateRow>(
                $"select next_attempt_at, lock_token from {Table} where entity_id = @Entity", new { Entity });
        }

        /// <summary>Whether the entity is due now (null: idle).</summary>
        public Task<bool?> DueAsync()
        {
            return observer.QuerySingleAsync<bool?>($"select next_attempt_at <= now() from {Table} where entity_id = @Entity", new { Entity });
        }

        /// <summary>Waits until <paramref name="statement"/> is blocked on a lock that <paramref name="holder"/>'s transaction holds.</summary>
        public async Task WaitUntilBlockedByAsync(NpgsqlConnection holder, Task statement)
        {
            var deadline = DateTime.UtcNow.AddSeconds(10);
            while (!await observer.ExecuteScalarAsync<bool>(
                       "select exists (select 1 from pg_stat_activity where @pid = any(pg_blocking_pids(pid)))", new { pid = holder.ProcessID }))
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