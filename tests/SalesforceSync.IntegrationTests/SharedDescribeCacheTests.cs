using System.Text.Json.Nodes;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Caching.StackExchangeRedis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using SalesforceSync.Salesforce;

namespace SalesforceSync.IntegrationTests;

/// <summary>The pods' Describe results in Redis: what one pod described, any other pod (a new one too) can use.</summary>
[Collection(MultiPodCollection.Name)]
public sealed class SharedDescribeCacheTests(MultiPodFixture app)
{
    [Fact]
    public async Task Pods_share_their_describes_through_Redis_so_a_new_pod_starts_warm()
    {
        var acct = SyncFixture.NewAccountId();
        await app.PublishAsync(acct, 1, new JsonObject { ["name"] = Any.String("Shared ") }, new JsonObject { ["tier"] = "Gold" });
        (await app.WaitForInboxSettledAsync(acct, expectedRows: 1)).Should().Equal((1L, "done"));

        var services = app.RunningPods[0].Services;
        var redis = services.GetRequiredService<IDistributedCache>();
        redis.Should().BeAssignableTo<RedisCache>("ConnectionStrings:redis is set");

        // A pod that has described nothing itself: everything it needs is in Redis
        var newPod = new SalesforceSchemaCache(TimeProvider.System, redis, services.GetRequiredService<IOptions<SalesforceOptions>>(),
            NullLogger<SalesforceSchemaCache>.Instance);
        foreach (var sobject in new[] { "Account", "Profile__c" })
        {
            var schema = await newPod.GetAsync(sobject, TimeSpan.FromMinutes(15),
                (_, _) => throw new InvalidOperationException($"{sobject} was described again"), CancellationToken.None);
            schema.Fields.Should().ContainKey("External_Id__c");
            schema.RecordTypes.Should().ContainKey(sobject == "Account" ? "Business" : "Standard");
        }
    }
}