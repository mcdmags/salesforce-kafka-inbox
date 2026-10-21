using System.Text.Json.Nodes;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Caching.StackExchangeRedis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Caching.Hybrid;
using Microsoft.Extensions.Options;
using SalesforceSync.Infrastructure.Salesforce;

namespace SalesforceSync.IntegrationTests;

/// <summary>The pods' record type Ids in Redis: what one pod read from the org, any other pod (a new one too) can use.</summary>
[Collection(MultiPodCollection.Name)]
public sealed class SharedRecordTypeCacheTests(MultiPodFixture app)
{
    [Fact]
    public async Task Pods_share_their_record_types_through_Redis_so_a_new_pod_starts_warm()
    {
        var acct = SyncFixture.NewAccountId();
        await app.PublishAsync(acct, 1, new JsonObject { ["name"] = Any.String("Shared ") }, new JsonObject { ["tier"] = "Gold" });
        (await app.WaitForInboxSettledAsync(acct, version: 1)).Should().Be((1L, "done"));

        var services = app.RunningPods[0].Services;
        var redis = services.GetRequiredService<IDistributedCache>();
        redis.Should().BeAssignableTo<RedisCache>("ConnectionStrings:redis is set");

        // A fresh HybridCache over the same Redis stands in for a new pod: its memory layer is empty, so what it finds
        // comes from Redis, and the org is not asked
        var queriesBefore = (await app.GetStatsAsync()).GetProperty("recordTypeQueries").GetInt64();
        var newPodServices = new ServiceCollection();
        newPodServices.AddSingleton(redis);
        newPodServices.AddHybridCache();
        var newPod = new RecordTypeCache(newPodServices.BuildServiceProvider().GetRequiredService<HybridCache>(),
            services.GetRequiredService<ISalesforceRestClient>(), services.GetRequiredService<IOptions<SalesforceOptions>>());

        (await newPod.IdOfAsync("Account", "Business", CancellationToken.None)).Should().NotBeNull();

        (await app.GetStatsAsync()).GetProperty("recordTypeQueries").GetInt64().Should().Be(queriesBefore);
    }
}
