using System.Text.Json.Nodes;
using Xunit.Abstractions;

namespace SalesforceSync.IntegrationTests;

/// <summary>
/// The mock org allows only 2 concurrent long-running requests (calls of 200 ms+ count as long-running here,
/// 20 s in Salesforce) while 3 pods x 4 workers send 250 ms calls.
/// </summary>
public sealed class ConcurrentLimitFixture : SyncFixture
{
    public ConcurrentLimitFixture() : base(pods: 3)
    {
        WorkerSettings["Sync:Workers"] = "4";
        WorkerSettings["Sync:MaxConcurrentRequests"] = "12";
        WorkerSettings["Sync:BatchSize"] = "2";
        WorkerSettings["Sync:IdleDelay"] = "00:00:00.050";
        WorkerSettings["RateLimit:MaxRequestsPerSecond"] = "1000";
        WorkerSettings["RateLimit:BurstCapacity"] = "1000";
        WorkerSettings["RateLimit:ConcurrentLimitPause"] = "00:00:00.500";
        WorkerSettings["RateLimit:DailyHardPause"] = "00:30:00"; // would stall the test if misclassified

        FakeOrgSettings["FakeSalesforce:Latency"] = "00:00:00.250";
        FakeOrgSettings["FakeSalesforce:LongRunningThreshold"] = "00:00:00.200";
        FakeOrgSettings["FakeSalesforce:ConcurrentLongRunningLimit"] = "2";
    }
}

[CollectionDefinition(Name)]
public sealed class ConcurrentLimitCollection : ICollectionFixture<ConcurrentLimitFixture>
{
    public const string Name = "ConcurrentLimit";
}

[Collection(ConcurrentLimitCollection.Name)]
public sealed class ConcurrentLimitTests(ConcurrentLimitFixture app, ITestOutputHelper output)
{
    [Fact]
    public async Task Concurrent_long_running_limit_pauses_briefly_and_everything_still_syncs()
    {
        var ids = Enumerable.Range(0, 40).Select(_ => SyncFixture.NewAccountId()).ToArray();
        foreach (var id in ids)
        {
            app.Publish(id, 1, new JsonObject { ["name"] = Any.String("Concurrent ") }, new JsonObject { ["tier"] = "Gold" });
        }

        app.Flush();

        // Short pause per rejection (ConcurrentPerOrgLongTxn), not the 30-minute daily pause, so everything lands
        await Eventually.ValueAsync(DoneAsync, done => done == ids.Length,
            "all events to sync despite REQUEST_LIMIT_EXCEEDED", TimeSpan.FromMinutes(2));

        var stats = await app.GetStatsAsync();
        output.WriteLine($"fake org: {stats}");
        stats.GetProperty("concurrencyRejections").GetInt64().Should().BePositive(
            "Salesforce should reject some requests for exceeding the concurrent long-running limit");
        stats.GetProperty("recordsFailed").GetInt64().Should().Be(0);

        // Rate-limited attempts are handed back without spending a retry
        await using var conn = await app.Db.OpenConnectionAsync();
        var maxRetry = await conn.QuerySingleAsync<int>(
            $"SELECT max(attempt_count) FROM {SyncFixture.InboxTable()} WHERE entity_id = ANY(@ids)", new { ids });
        maxRetry.Should().Be(0, "rate-limited attempts are handed back without spending a retry");

        async Task<long> DoneAsync()
        {
            await using var c = await app.Db.OpenConnectionAsync();
            return await c.QuerySingleAsync<long>(
                $"SELECT count(*) FROM {SyncFixture.InboxTable()} WHERE status = 'done' AND entity_id = ANY(@ids)", new { ids });
        }
    }
}