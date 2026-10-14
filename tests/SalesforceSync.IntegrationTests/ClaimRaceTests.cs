using System.Text.Json.Nodes;
using Npgsql;
using SalesforceSync.Inbox;
using SalesforceSync.Sync;

namespace SalesforceSync.IntegrationTests;

/// <summary>
/// Claims interleaved by hand on separate connections, in an inbox table no pod works on, so every step happens
/// exactly when the test says. Each test checks the outcome that matters: every live lease on the entity is worker
/// A's. Two consumers store an event of one entity each and the lower id commits last, so the entity's head (its
/// oldest pending row) is a different row for each worker and the head row lock alone doesn't keep them apart.
/// </summary>
[Collection(SyncCollection.Name)]
public sealed class ClaimRaceTests(SyncFixture app)
{
    private const string Table = "sf_claim_race_inbox";
    private static readonly InboxSql Sql = InboxSql.For(Table);

    [Fact]
    public async Task A_row_committed_out_of_id_order_does_not_let_a_second_worker_claim_an_entity_in_flight()
    {
        await using var race = await Race.StartAsync(app);

        // The lower id commits: worker B now sees it as the entity's head, a row nobody has locked
        await race.SlowInsert.CommitAsync();
        await using var txB = await race.WorkerB.BeginTransactionAsync();
        var lockedByB = await LockHeadsAsync(race.WorkerB, txB);

        // A commits its lease; B goes on with whatever it locked
        await race.TxA.CommitAsync();
        foreach (var e in lockedByB) await ClaimEntitiesAsync(race.WorkerB, txB, e, Guid.NewGuid());
        await txB.CommitAsync();

        (await race.LiveLeaseTokensAsync()).Should().NotBeEmpty().And.OnlyContain(t => t == race.TokenA,
            "worker A holds a live lease on the entity, so worker B may not claim it");
    }

    [Fact]
    public async Task A_second_worker_does_not_claim_an_entity_whose_claim_has_not_committed_yet()
    {
        await using var race = await Race.StartAsync(app);
        await race.SlowInsert.CommitAsync();

        // B claims while A's claim is still open: A's lease isn't visible to B yet, only A's claim lock is
        await using var txB = await race.WorkerB.BeginTransactionAsync();
        var claimB = Task.Run(async () =>
        {
            foreach (var e in await LockHeadsAsync(race.WorkerB, txB)) await ClaimEntitiesAsync(race.WorkerB, txB, e, Guid.NewGuid());
        });

        // Without the claim lock B leases the unlocked row and then waits for A's row; let A commit then
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (!claimB.IsCompleted && !await race.IsWaitingOnALockAsync(race.WorkerB) && DateTime.UtcNow < deadline)
            await Task.Delay(20);
        await race.TxA.CommitAsync();
        await claimB;
        await txB.CommitAsync();

        (await race.LiveLeaseTokensAsync()).Should().NotBeEmpty().And.OnlyContain(t => t == race.TokenA,
            "worker A's claim of the entity was in progress, so worker B may not claim it");
    }

    [Fact]
    public async Task Claiming_leases_nothing_of_an_entity_another_worker_holds_a_live_lease_on()
    {
        await using var race = await Race.StartAsync(app);
        await race.SlowInsert.CommitAsync();
        await race.TxA.CommitAsync();

        // As if B's LockHeads had read the entity before A committed and taken its claim lock after
        await using var txB = await race.WorkerB.BeginTransactionAsync();
        (await ClaimEntitiesAsync(race.WorkerB, txB, race.Entity, Guid.NewGuid())).Should().Be(0);
        await txB.CommitAsync();

        (await race.LiveLeaseTokensAsync()).Should().NotBeEmpty().And.OnlyContain(t => t == race.TokenA);
    }

    private static async Task<string[]> LockHeadsAsync(NpgsqlConnection conn, NpgsqlTransaction tx)
        => (await conn.QueryAsync<string>(Sql.LockHeads, new { window = InboxSql.ClaimWindow, batchSize = 75 }, tx)).ToArray();

    private static async Task<int> ClaimEntitiesAsync(NpgsqlConnection conn, NpgsqlTransaction tx, string entity, Guid token)
        => (await conn.QueryAsync<InboxEvent>(Sql.ClaimEntities,
            new { entityIds = new[] { entity }, token, lease = TimeSpan.FromMinutes(1), maxPerEntity = 20 }, tx)).Count();

    /// <summary>
    /// The common start: the higher id is committed and the lower id is not (<see cref="SlowInsert"/>); worker A has
    /// claimed the entity through the higher id, its only visible row, and not committed (<see cref="TxA"/>).
    /// </summary>
    private sealed class Race : IAsyncDisposable
    {
        private readonly NpgsqlConnection _consumer1, _consumer2, _workerA;

        private Race(NpgsqlConnection consumer1, NpgsqlConnection consumer2, NpgsqlConnection workerA, NpgsqlConnection workerB,
            string entity, NpgsqlTransaction slowInsert, NpgsqlTransaction txA, Guid tokenA)
        {
            (_consumer1, _consumer2, _workerA, WorkerB) = (consumer1, consumer2, workerA, workerB);
            (Entity, SlowInsert, TxA, TokenA) = (entity, slowInsert, txA, tokenA);
        }

        public NpgsqlConnection WorkerB { get; }
        public string Entity { get; }
        public NpgsqlTransaction SlowInsert { get; }
        public NpgsqlTransaction TxA { get; }
        public Guid TokenA { get; }

        public static async Task<Race> StartAsync(SyncFixture app)
        {
            await using (var setup = await app.Db.OpenConnectionAsync())
                await PgSql.ApplySchemaAsync(setup, null, [Table]);

            var entity = SyncFixture.NewAccountId();
            string Event(int v) => SyncFixture.AccountEvent(entity, v, new JsonObject { ["name"] = $"v{v}" }, new JsonObject()).ToJsonString();

            var consumer1 = await app.Db.OpenConnectionAsync();
            var consumer2 = await app.Db.OpenConnectionAsync();
            var workerA = await app.Db.OpenConnectionAsync();
            var workerB = await app.Db.OpenConnectionAsync();

            // Two consumers store an event of the entity each; the one with the lower id commits last
            var slowInsert = await consumer1.BeginTransactionAsync();
            await consumer1.ExecuteAsync(PgSql.Insert(Table), new { entityId = entity, payload = Event(1) }, slowInsert);
            await consumer2.ExecuteAsync(PgSql.Insert(Table), new { entityId = entity, payload = Event(2) });

            // Worker A claims the entity: its head is the only row it can see, the higher id
            var tokenA = Guid.NewGuid();
            var txA = await workerA.BeginTransactionAsync();
            (await LockHeadsAsync(workerA, txA)).Should().Equal(entity);
            (await ClaimEntitiesAsync(workerA, txA, entity, tokenA)).Should().Be(1);

            return new Race(consumer1, consumer2, workerA, workerB, entity, slowInsert, txA, tokenA);
        }

        public async Task<List<Guid?>> LiveLeaseTokensAsync()
            => (await _consumer2.QueryAsync<Guid?>($"select lock_token from {Table} where entity_id = @Entity and locked_until > now()",
                new { Entity })).ToList();

        public async Task<bool> IsWaitingOnALockAsync(NpgsqlConnection conn)
            => await _consumer2.ExecuteScalarAsync<bool>(
                "select coalesce((select wait_event_type = 'Lock' from pg_stat_activity where pid = @pid), false)",
                new { pid = conn.ProcessID });

        public async ValueTask DisposeAsync()
        {
            foreach (var conn in new[] { _consumer1, _consumer2, _workerA, WorkerB }) await conn.DisposeAsync();
        }
    }
}
