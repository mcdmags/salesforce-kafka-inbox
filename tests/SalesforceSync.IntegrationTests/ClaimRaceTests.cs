using System.Text.Json.Nodes;
using Dapper;
using Npgsql;
using SalesforceSync.Inbox;
using SalesforceSync.Sync;

namespace SalesforceSync.IntegrationTests;

/// <summary>
/// Two claims interleaved by hand on separate connections, in an inbox table no pod works on, so every step happens
/// exactly when the test says. Reproduces the race in which the entity's head is a different row for each worker.
/// </summary>
[Collection(SyncCollection.Name)]
public sealed class ClaimRaceTests(SyncFixture app)
{
    private const string Table = "sf_claim_race_inbox";
    private static readonly InboxSql Sql = InboxSql.For(Table);

    [Fact]
    public async Task A_row_committed_out_of_id_order_does_not_let_a_second_worker_claim_an_entity_in_flight()
    {
        await using (var setup = await app.Db.OpenConnectionAsync())
            await InboxSchema.ApplyAsync(setup, null, [Table]);

        var entity = SyncFixture.NewAccountId();
        string Event(int v) => SyncFixture.AccountEvent(entity, v, new JsonObject { ["name"] = $"v{v}" }, new JsonObject()).ToJsonString();

        await using var consumer1 = await app.Db.OpenConnectionAsync();
        await using var consumer2 = await app.Db.OpenConnectionAsync();
        await using var workerA = await app.Db.OpenConnectionAsync();
        await using var workerB = await app.Db.OpenConnectionAsync();

        // Two consumers store an event of the entity each; the one with the lower id commits last
        await using var slowInsert = await consumer1.BeginTransactionAsync();
        await consumer1.ExecuteAsync(Sql.Insert, new { entityId = entity, payload = Event(1) }, slowInsert);
        await consumer2.ExecuteAsync(Sql.Insert, new { entityId = entity, payload = Event(2) });

        // Worker A claims the entity: its head is the only row it can see, the higher id. Not committed yet
        var tokenA = Guid.NewGuid();
        await using var txA = await workerA.BeginTransactionAsync();
        (await LockHeadsAsync(workerA, txA)).Should().Equal(entity);
        (await ClaimAsync(workerA, txA, entity, tokenA)).Should().Be(1);

        // The lower id commits: worker B now sees it as the entity's head, a row nobody has locked
        await slowInsert.CommitAsync();
        var tokenB = Guid.NewGuid();
        await using var txB = await workerB.BeginTransactionAsync();
        var claimedByB = await LockHeadsAsync(workerB, txB);

        // A commits its lease; B goes on with whatever it locked
        await txA.CommitAsync();
        foreach (var e in claimedByB) await ClaimAsync(workerB, txB, e, tokenB);
        await txB.CommitAsync();

        var tokens = (await workerA.QueryAsync<Guid?>($"select lock_token from {Table} where entity_id = @entity and locked_until > now()",
            new { entity })).ToList();
        tokens.Should().NotBeEmpty().And.OnlyContain(t => t == tokenA, "worker A holds a live lease on the entity, so worker B may not claim it");
        claimedByB.Should().NotContain(entity);
    }

    private static async Task<string[]> LockHeadsAsync(NpgsqlConnection conn, NpgsqlTransaction tx)
    {
        await conn.ExecuteAsync(InboxSql.PinPlans, transaction: tx);
        return (await conn.QueryAsync<string>(Sql.LockHeads, new { window = InboxSql.ClaimWindow, batchSize = 75 }, tx)).ToArray();
    }

    private static async Task<int> ClaimAsync(NpgsqlConnection conn, NpgsqlTransaction tx, string entity, Guid token)
        => (await conn.QueryAsync<InboxEvent>(Sql.ClaimEntities,
            new { entityIds = new[] { entity }, token, lease = TimeSpan.FromMinutes(1), maxPerEntity = 20 }, tx)).Count();
}
