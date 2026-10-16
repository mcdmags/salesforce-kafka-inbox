using System.Text.Json.Nodes;
using Npgsql;
using SalesforceSync.Inbox;

namespace SalesforceSync.IntegrationTests;

/// <summary>
/// Claims, stores and outcomes interleaved by hand on separate connections, in the account inbox but under an entity
/// type of the test's own, which no pod claims, so every step happens exactly when the test says. The races the claim used
/// to have to defend against (two consumers committing events of one entity out of id order, so that its oldest
/// pending event was a different row for two workers) can't arise any more: storing an event and claiming an entity
/// both lock the entity's one sf_sync_state row.
/// </summary>
[Collection(SyncCollection.Name)]
public sealed class ClaimRaceTests(SyncFixture app)
{
    private static readonly string Table = SyncFixture.InboxTable();
    private static readonly InboxSql Sql = InboxSql.For(Table);

    [Fact]
    public async Task Events_of_one_entity_commit_in_id_order_and_an_entity_being_stored_is_not_claimed()
    {
        await using var race = await Race.StartAsync(app);

        // Consumer 1 stores an event and hasn't committed; consumer 2's event of the same entity has to wait for it
        var slowStore = await race.Consumer1.BeginTransactionAsync();
        await race.StoreAsync(race.Consumer1, 1, slowStore);
        var secondStore = race.StoreAsync(race.Consumer2, 2);
        await race.WaitUntilWaitingOnALockAsync(race.Consumer2, secondStore);
        secondStore.IsCompleted.Should().BeFalse("the second event's store waits for the first to commit");

        // Meanwhile a worker skips the entity: it is locked
        await using (var txA = await race.WorkerA.BeginTransactionAsync())
        {
            (await race.ClaimAsync(race.WorkerA, txA, Guid.NewGuid())).Should().BeEmpty();
            await txA.CommitAsync();
        }

        await slowStore.CommitAsync();
        await secondStore;

        await using var tx = await race.WorkerA.BeginTransactionAsync();
        (await race.ClaimAsync(race.WorkerA, tx, Guid.NewGuid())).Should().Equal(race.Entity);
        var events = await race.ClaimEventsAsync(race.WorkerA, tx);
        await tx.CommitAsync();
        events.Should().HaveCount(2).And.BeInAscendingOrder(e => e.RowId);
    }

    [Fact]
    public async Task A_second_worker_does_not_claim_an_entity_whose_claim_has_not_committed_yet()
    {
        await using var race = await Race.StartAsync(app);
        await race.StoreAsync(race.Consumer1, 1);

        var tokenA = Guid.NewGuid();
        await using var txA = await race.WorkerA.BeginTransactionAsync();
        (await race.ClaimAsync(race.WorkerA, txA, tokenA)).Should().Equal(race.Entity);

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
        await using var race = await Race.StartAsync(app);
        await race.StoreAsync(race.Consumer1, 1);

        var tokenA = Guid.NewGuid();
        await using (var txA = await race.WorkerA.BeginTransactionAsync())
        {
            (await race.ClaimAsync(race.WorkerA, txA, tokenA)).Should().Equal(race.Entity);
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
    /// The race between a consumer storing an event and a worker ending the entity's lease (finding no pending event
    /// left would make the entity idle): whichever commits second, the new event's entity ends due.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task An_event_stored_while_its_entitys_lease_ends_leaves_the_entity_due(bool storeFirst)
    {
        await using var race = await Race.StartAsync(app);
        await race.StoreAsync(race.Consumer1, 1);
        var token = Guid.NewGuid();
        long[] claimed;
        await using (var tx = await race.WorkerA.BeginTransactionAsync())
        {
            (await race.ClaimAsync(race.WorkerA, tx, token)).Should().Equal(race.Entity);
            claimed = [.. (await race.ClaimEventsAsync(race.WorkerA, tx)).Select(e => e.RowId)];
            await tx.CommitAsync();
        }

        // The worker records v1 done and ends the lease; consumer 2 stores v2 at the same time
        var store = storeFirst ? await race.Consumer2.BeginTransactionAsync() : null;
        if (store is not null)
        {
            await race.StoreAsync(race.Consumer2, 2, store);
        }
        await using var record = await race.WorkerA.BeginTransactionAsync();
        var locking = race.LockHeldAsync(record, token);
        Task? lateStore = null;
        if (store is not null)
        {
            // The worker waits for the store to commit before it can lock the entity
            await race.WaitUntilWaitingOnALockAsync(race.WorkerA, locking);
            await store.CommitAsync();
        }
        (await locking).Should().Equal(race.Entity);
        await race.WorkerA.ExecuteAsync(Sql.Done, new
        {
            entityType = race.EntityType,
            ids = claimed,
            token,
            times = $$"""{ "{{claimed[0]}}": 1 }"""
        }, record);
        if (store is null)
        {
            // The store waits for the worker's transaction, which has locked the entity
            lateStore = race.StoreAsync(race.Consumer2, 2);
            await race.WaitUntilWaitingOnALockAsync(race.Consumer2, lateStore);
        }
        await race.WorkerA.ExecuteAsync(Sql.EndLease, new
        {
            entityType = race.EntityType,
            entityIds = new[] { race.Entity },
            token
        }, record);
        await record.CommitAsync();
        if (lateStore is not null)
        {
            await lateStore;
        }

        var state = await race.StateAsync();
        state.LockToken.Should().BeNull();
        state.NextAttemptAt.Should().NotBeNull("v2 is pending, so its entity must be due, or it would never be claimed");
    }

    /// <summary>One entity of an entity type of its own, and connections for two consumers and two workers.</summary>
    private sealed class Race : IAsyncDisposable
    {
        private Race(NpgsqlConnection consumer1, NpgsqlConnection consumer2, NpgsqlConnection workerA, NpgsqlConnection workerB,
            NpgsqlConnection observer)
        {
            (Consumer1, Consumer2, WorkerA, WorkerB, Observer) = (consumer1, consumer2, workerA, workerB, observer);
        }

        public NpgsqlConnection Consumer1 { get; }
        public NpgsqlConnection Consumer2 { get; }
        public NpgsqlConnection WorkerA { get; }
        public NpgsqlConnection WorkerB { get; }
        private NpgsqlConnection Observer { get; }

        public string EntityType { get; } = $"claimRace{Guid.NewGuid():N}";
        public string Entity { get; } = SyncFixture.NewAccountId();

        public static async Task<Race> StartAsync(SyncFixture app)
        {
            return new Race(await app.Db.OpenConnectionAsync(), await app.Db.OpenConnectionAsync(), await app.Db.OpenConnectionAsync(),
                await app.Db.OpenConnectionAsync(), await app.Db.OpenConnectionAsync());
        }

        public Task StoreAsync(NpgsqlConnection consumer, int version, NpgsqlTransaction? tx = null)
        {
            var e = SyncFixture.AccountEvent(Entity, version, new JsonObject { ["name"] = $"v{version}" }, []);
            return consumer.StoreAsync(Table, EntityType, Entity, SyncFixture.TimeOf(e), e.ToJsonString(), tx);
        }

        public async Task<List<string>> ClaimAsync(NpgsqlConnection worker, NpgsqlTransaction tx, Guid token)
        {
            return [.. (await worker.QueryAsync<(string EntityId, long? SyncedTime)>(InboxSql.ClaimEntities,
                new { entityType = EntityType, token, lease = TimeSpan.FromMinutes(1), batchSize = 75 }, tx)).Select(c => c.EntityId)];
        }

        public Task<List<InboxEvent>> ClaimEventsAsync(NpgsqlConnection worker, NpgsqlTransaction tx)
        {
            return worker.QueryAsync<InboxEvent>(Sql.ClaimEvents, new { entityIds = new[] { Entity }, maxPerEntity = 20 }, tx);
        }

        public Task<List<string>> LockHeldAsync(NpgsqlTransaction tx, Guid token)
        {
            return WorkerA.QueryAsync<string>(InboxSql.LockHeld, new { entityType = EntityType, entityIds = new[] { Entity }, token }, tx);
        }

        public async Task<SyncStateRow> StateAsync()
        {
            return await Observer.QuerySingleAsync<SyncStateRow>(
                "select next_attempt_at, locked_until, lock_token, synced_time from sf_sync_state where entity_type = @EntityType and entity_id = @Entity",
                new { EntityType, Entity });
        }

        /// <summary>Waits until <paramref name="conn"/>'s statement (<paramref name="statement"/>) is blocked on a lock.</summary>
        public async Task WaitUntilWaitingOnALockAsync(NpgsqlConnection conn, Task statement)
        {
            var deadline = DateTime.UtcNow.AddSeconds(10);
            while (!await Observer.ExecuteScalarAsync<bool>(
                       "select coalesce((select wait_event_type = 'Lock' from pg_stat_activity where pid = @pid), false)",
                       new { pid = conn.ProcessID }))
            {
                statement.IsCompleted.Should().BeFalse("the statement should be waiting for a lock");
                DateTime.UtcNow.Should().BeBefore(deadline, "the statement should be waiting for a lock");
                await Task.Delay(20);
            }
        }

        public async ValueTask DisposeAsync()
        {
            foreach (var conn in new[] { Consumer1, Consumer2, WorkerA, WorkerB, Observer })
            {
                await conn.DisposeAsync();
            }
        }
    }
}