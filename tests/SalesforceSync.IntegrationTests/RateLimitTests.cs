using System.Text.Json.Nodes;
using Aspire.Hosting;
using Dapper;
using Xunit.Abstractions;

namespace SalesforceSync.IntegrationTests;

/// <summary>
/// A tiny daily allocation shared by 3 pods: SalesforceLimits:DailyApiAllocation = 60 and
/// RateLimit:DailyBudgetShare = 0.5 give this integration 30 calls per rolling 24 hours.
/// </summary>
public sealed class DailyBudgetFixture : AppHostFixture
{
    public const int Pods = 3;
    public const int Budget = 30;

    protected override void Configure(IDistributedApplicationTestingBuilder builder)
    {
        Project(builder, "salesforce-sync")
            .WithReplicas(Pods)
            .WithEnvironment("SalesforceLimits__DailyApiAllocation", "60")
            .WithEnvironment("RateLimit__DailyBudgetShare", "0.5")
            // Normally pods slow down at 80% and stop at 95% of the budget (unit-tested); turned off here to
            // test the hard cap itself
            .WithEnvironment("RateLimit__DailySoftRatio", "2")
            .WithEnvironment("RateLimit__DailyHardRatio", "2")
            .WithEnvironment("RateLimit__MaxRequestsPerSecond", "1000")
            .WithEnvironment("RateLimit__BurstCapacity", "1000")
            .WithEnvironment("Sync__BatchSize", "1") // 3 calls per account, so the budget runs out mid-way
            .WithEnvironment("Sync__IdleDelay", "00:00:00.050");
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
    public async Task All_pods_together_stop_at_the_integrations_share_of_the_daily_allocation()
    {
        var ids = Enumerable.Range(0, 30).Select(_ => AppHostFixture.NewAccountId()).ToArray();
        foreach (var id in ids)
            app.Publish(id, 1, new JsonObject { ["Name"] = $"Budget {id}" }, new JsonObject { ["Tier__c"] = "Gold" });
        app.Flush();

        // The budget is used up...
        await Eventually.ValueAsync(OwnCallsAsync, used => used >= DailyBudgetFixture.Budget,
            "the daily budget to be used up", TimeSpan.FromMinutes(1));

        // ...and stays used up: no pod makes another call
        var stats = await app.GetStatsAsync();
        await Task.Delay(TimeSpan.FromSeconds(3));
        var later = await app.GetStatsAsync();
        output.WriteLine($"fake org: {later}");

        var apiCalls = later.GetProperty("apiCalls").GetInt64();
        Assert.Equal(DailyBudgetFixture.Budget, await OwnCallsAsync());
        Assert.Equal(DailyBudgetFixture.Budget, apiCalls);
        Assert.Equal(stats.GetProperty("apiCalls").GetInt64(), apiCalls);

        // Work that didn't fit is deferred, not failed: still pending, retries not spent, nothing dead-lettered
        await using var conn = await app.Db.OpenConnectionAsync();
        var rows = (await conn.QueryAsync<(string Status, int RetryCount, DateTime NextAttempt)>(
            "select status, retry_count, next_attempt_at from sf_inbox where account_ext_id = any(@ids)",
            new { ids })).ToList();
        var pending = rows.Where(r => r.Status == "pending").ToList();
        output.WriteLine($"done={rows.Count(r => r.Status == "done")} pending={pending.Count}");
        Assert.NotEmpty(pending);
        Assert.DoesNotContain(rows, r => r.Status == "dead");
        Assert.All(pending, r => Assert.Equal(0, r.RetryCount));
        Assert.All(pending, r => Assert.True(r.NextAttempt > DateTime.UtcNow.AddSeconds(30),
            $"expected the event to be deferred until the budget frees up, next attempt {r.NextAttempt:O}"));
    }

    private async Task<long> OwnCallsAsync()
    {
        await using var conn = await app.Db.OpenConnectionAsync();
        return await conn.ExecuteScalarAsync<long>("select coalesce(sum(calls), 0) from sf_api_usage");
    }
}

/// <summary>
/// The mock org allows only 2 concurrent long-running requests (calls of 200 ms+ count as long-running here,
/// 20 s in Salesforce) while 3 pods x 4 workers send 250 ms calls.
/// </summary>
public sealed class ConcurrentLimitFixture : AppHostFixture
{
    protected override void Configure(IDistributedApplicationTestingBuilder builder)
    {
        Project(builder, "salesforce-sync")
            .WithReplicas(3)
            .WithEnvironment("Sync__Workers", "4")
            .WithEnvironment("Sync__MaxConcurrentRequests", "12")
            .WithEnvironment("Sync__BatchSize", "2")
            .WithEnvironment("Sync__IdleDelay", "00:00:00.050")
            .WithEnvironment("RateLimit__MaxRequestsPerSecond", "1000")
            .WithEnvironment("RateLimit__BurstCapacity", "1000")
            .WithEnvironment("RateLimit__ConcurrentLimitPause", "00:00:00.500")
            .WithEnvironment("RateLimit__DailyHardPause", "00:30:00"); // would stall the test if misclassified

        Project(builder, "fake-salesforce")
            .WithEnvironment("FakeSalesforce__Latency", "00:00:00.250")
            .WithEnvironment("FakeSalesforce__LongRunningThreshold", "00:00:00.200")
            .WithEnvironment("FakeSalesforce__ConcurrentLongRunningLimit", "2");
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
        var ids = Enumerable.Range(0, 40).Select(_ => AppHostFixture.NewAccountId()).ToArray();
        foreach (var id in ids)
            app.Publish(id, 1, new JsonObject { ["Name"] = $"Concurrent {id}" }, new JsonObject { ["Tier__c"] = "Gold" });
        app.Flush();

        // Short pause per rejection (ConcurrentPerOrgLongTxn), not the 30-minute daily pause, so everything lands
        await Eventually.ValueAsync(DoneAsync, done => done == ids.Length,
            "all events to sync despite REQUEST_LIMIT_EXCEEDED", TimeSpan.FromMinutes(2));

        var stats = await app.GetStatsAsync();
        output.WriteLine($"fake org: {stats}");
        Assert.True(stats.GetProperty("concurrencyRejections").GetInt64() > 0,
            "expected Salesforce to reject some requests for exceeding the concurrent long-running limit");
        Assert.Equal(0, stats.GetProperty("graphsFailed").GetInt64());

        // Rate-limited attempts are handed back without spending a retry
        await using var conn = await app.Db.OpenConnectionAsync();
        var maxRetry = await conn.ExecuteScalarAsync<int>(
            "select max(retry_count) from sf_inbox where account_ext_id = any(@ids)", new { ids });
        Assert.Equal(1, maxRetry);

        async Task<long> DoneAsync()
        {
            await using var c = await app.Db.OpenConnectionAsync();
            return await c.ExecuteScalarAsync<long>(
                "select count(*) from sf_inbox where status = 'done' and account_ext_id = any(@ids)", new { ids });
        }
    }
}
