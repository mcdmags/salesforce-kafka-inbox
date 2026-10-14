using System.Net;
using System.Text.Json.Nodes;
using SalesforceSync.Salesforce;
using Xunit.Abstractions;

namespace SalesforceSync.IntegrationTests;

/// <summary>
/// A tiny daily allocation: the configuration allows 1,000 calls, but the mock org reports
/// <c>Sforce-Limit-Info: api-usage=n/60</c>, the lower, so RateLimit:DailyBudgetShare = 0.5 gives this integration
/// 30 calls per rolling 24 hours.
/// </summary>
public sealed class DailyBudgetFixture : SyncFixture
{
    public const int Budget = 30;

    public DailyBudgetFixture()
    {
        WorkerSettings["SalesforceLimits:DailyApiAllocation"] = "1000";
        FakeOrgSettings["FakeSalesforce:DailyApiLimit"] = "60";
        WorkerSettings["RateLimit:DailyBudgetShare"] = "0.5";
        // Normally the worker slows down at 80% and stop at 95% of the budget (unit-tested); turned off here to
        // test the hard cap itself
        WorkerSettings["RateLimit:DailySoftRatio"] = "2";
        WorkerSettings["RateLimit:DailyHardRatio"] = "2";
        WorkerSettings["RateLimit:MaxRequestsPerSecond"] = "1000";
        WorkerSettings["RateLimit:BurstCapacity"] = "1000";
        WorkerSettings["Sync:BatchSize"] = "1"; // 3 calls per account, so the budget runs out mid-way
        WorkerSettings["Sync:IdleDelay"] = "00:00:00.050";
    }
}

[CollectionDefinition(Name)]
public sealed class DailyBudgetCollection : ICollectionFixture<DailyBudgetFixture>
{
    public const string Name = "DailyBudget";
}

[Collection(DailyBudgetCollection.Name)]
public sealed class DailyBudgetTests(DailyBudgetFixture app, ITestOutputHelper output)
{
    [Fact]
    public async Task The_worker_stops_at_the_integrations_share_of_the_daily_allocation()
    {
        var ids = Enumerable.Range(0, 30).Select(_ => SyncFixture.NewAccountId()).ToArray();
        foreach (var id in ids)
        {
            app.Publish(id, 1, new JsonObject { ["name"] = Any.String("Budget ") }, new JsonObject { ["tier"] = "Gold" });
        }

        app.Flush();

        // The budget is used up...
        await Eventually.ValueAsync(OwnCallsAsync, used => used >= DailyBudgetFixture.Budget,
            "the daily budget to be used up", TimeSpan.FromMinutes(1));

        // ...and stays used up: no other call is made. Proving that nothing happens needs a fixed wait; the worker
        // polls every 50 ms (Sync:IdleDelay), so 3 s gives it dozens of chances to overspend.
        var stats = await app.GetStatsAsync();
        await Task.Delay(TimeSpan.FromSeconds(3));
        var later = await app.GetStatsAsync();
        output.WriteLine($"fake org: {later}");

        var apiCalls = later.GetProperty("apiCalls").GetInt64();
        (await OwnCallsAsync()).Should().Be(DailyBudgetFixture.Budget, "sf_api_usage counts exactly the budget");
        apiCalls.Should().Be(DailyBudgetFixture.Budget, "the worker makes exactly the budgeted calls");
        apiCalls.Should().Be(stats.GetProperty("apiCalls").GetInt64(), "no call is made once the budget is used up");

        // Work that didn't fit is deferred, not failed: still pending, retries not spent, nothing dead-lettered
        await using var conn = await app.Db.OpenConnectionAsync();
        var rows = (await conn.QueryAsync<(string Status, int RetryCount, DateTime NextAttempt)>(
            "select status, retry_count, next_attempt_at from sf_account_inbox where entity_id = any(@ids)",
            new { ids })).ToList();
        var pending = rows.Where(r => r.Status == "pending").ToList();
        output.WriteLine($"done={rows.Count(r => r.Status == "done")} pending={pending.Count}");
        pending.Should().NotBeEmpty("the budget can't cover every event");
        rows.Should().NotContain(r => r.Status == "dead", "work that didn't fit is deferred, not dead-lettered");
        pending.Should().OnlyContain(r => r.RetryCount == 0, "a deferred event doesn't spend a retry");
        pending.Select(r => r.NextAttempt).Should().OnlyContain(next => next > DateTime.UtcNow.AddSeconds(30),
            "events are deferred until the budget frees up");
    }

    private async Task<long> OwnCallsAsync()
    {
        await using var conn = await app.Db.OpenConnectionAsync();
        return await conn.ExecuteScalarAsync<long>("select coalesce(sum(calls), 0) from sf_api_usage");
    }
}

/// <summary>
/// The mock org allows only 1 concurrent long-running request (calls of 200 ms+ count as long-running here, 20 s in
/// Salesforce), and another integration (the test) keeps it busy with 250 ms calls until the worker has been turned
/// away with REQUEST_LIMIT_EXCEEDED.
/// </summary>
public sealed class ConcurrentLimitFixture : SyncFixture
{
    public ConcurrentLimitFixture()
    {
        WorkerSettings["Sync:BatchSize"] = "2";
        WorkerSettings["Sync:IdleDelay"] = "00:00:00.050";
        WorkerSettings["RateLimit:MaxRequestsPerSecond"] = "1000";
        WorkerSettings["RateLimit:BurstCapacity"] = "1000";
        WorkerSettings["RateLimit:ConcurrentLimitPause"] = "00:00:00.500";
        WorkerSettings["RateLimit:DailyHardPause"] = "00:30:00"; // would stall the test if misclassified

        FakeOrgSettings["FakeSalesforce:Latency"] = "00:00:00.250";
        FakeOrgSettings["FakeSalesforce:LongRunningThreshold"] = "00:00:00.200";
        FakeOrgSettings["FakeSalesforce:ConcurrentLongRunningLimit"] = "1";
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
        // Another integration holds the org's only long-running slot, one call after another
        using var otherIntegration = new CancellationTokenSource(TimeSpan.FromMinutes(1));
        long ownRejections = 0;
        var occupying = Task.Run(async () =>
        {
            while (!otherIntegration.IsCancellationRequested)
            {
                using var res = await app.Salesforce.GetAsync($"{SalesforceRestClient.DataPath}/sobjects/Account/describe");
                if (res.StatusCode == HttpStatusCode.Forbidden)
                {
                    Interlocked.Increment(ref ownRejections);
                }
            }
        });

        var ids = Enumerable.Range(0, 40).Select(_ => SyncFixture.NewAccountId()).ToArray();
        foreach (var id in ids)
        {
            app.Publish(id, 1, new JsonObject { ["name"] = Any.String("Concurrent ") }, new JsonObject { ["tier"] = "Gold" });
        }

        app.Flush();

        // The org turns the worker away. More than one rejection beyond the other integration's own: at most one of
        // its calls is between being rejected and being counted.
        await Eventually.ValueAsync(async () => await RejectionsAsync() - Interlocked.Read(ref ownRejections),
            workerRejections => workerRejections > 1, "the worker to be rejected with REQUEST_LIMIT_EXCEEDED");
        await otherIntegration.CancelAsync();
        await occupying;

        // Short pause per rejection (ConcurrentPerOrgLongTxn), not the 30-minute daily pause, so everything lands
        await Eventually.ValueAsync(DoneAsync, done => done == ids.Length,
            "all events to sync despite REQUEST_LIMIT_EXCEEDED", TimeSpan.FromMinutes(2));

        var stats = await app.GetStatsAsync();
        output.WriteLine($"fake org: {stats}, of which the other integration's: {ownRejections}");
        (stats.GetProperty("concurrencyRejections").GetInt64() - ownRejections).Should().BePositive(
            "Salesforce should reject some of the worker's requests for exceeding the concurrent long-running limit");
        stats.GetProperty("recordsFailed").GetInt64().Should().Be(0);

        // Rate-limited attempts are handed back without spending a retry
        await using var conn = await app.Db.OpenConnectionAsync();
        var maxRetry = await conn.ExecuteScalarAsync<int>(
            "select max(retry_count) from sf_account_inbox where entity_id = any(@ids)", new { ids });
        maxRetry.Should().Be(0, "rate-limited attempts are handed back without spending a retry");

        async Task<long> RejectionsAsync()
        {
            return (await app.GetStatsAsync()).GetProperty("concurrencyRejections").GetInt64();
        }

        async Task<long> DoneAsync()
        {
            await using var c = await app.Db.OpenConnectionAsync();
            return await c.ExecuteScalarAsync<long>(
                "select count(*) from sf_account_inbox where status = 'done' and entity_id = any(@ids)", new { ids });
        }
    }
}