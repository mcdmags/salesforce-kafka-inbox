using Dapper;

namespace SalesforceSync.IntegrationTests;

/// <summary>
/// The same sync, scaled out like a Kubernetes deployment: several worker pods, each running several worker loops,
/// all sharing one Postgres inbox, one Kafka consumer group and one Salesforce org.
/// <para>
/// Salesforce itself does nothing to stop an out-of-order write, so any race lands in the mock org's write
/// history. The rate limiter is opened up and batches are kept small so workers contend for the same accounts
/// as much as possible.
/// </para>
/// </summary>
public sealed class MultiPodFixture : SyncFixture
{
    public const int PodCount = 3;
    public const int WorkersPerPod = 8;

    public MultiPodFixture() : base(PodCount)
    {
        WorkerSettings["Sync:Workers"] = WorkersPerPod.ToString();
        WorkerSettings["Sync:MaxConcurrentRequests"] = (PodCount * WorkersPerPod).ToString();
        WorkerSettings["Sync:BatchSize"] = "10";
        WorkerSettings["Sync:IdleDelay"] = "00:00:00.050";
        // Short lease: a claim lost to a dropped connection (chaos test) is reclaimed quickly
        WorkerSettings["Sync:Lease"] = "00:00:20";
        // Measure ordering, not the throttle
        WorkerSettings["RateLimit:MaxRequestsPerSecond"] = "1000000";
        WorkerSettings["RateLimit:BurstCapacity"] = "1000000";

        FakeOrgSettings["FakeSalesforce:Latency"] = "00:00:00.010";
    }

    protected override async Task OnStartedAsync()
    {
        await using var conn = await Db.OpenConnectionAsync();
        await conn.ExecuteAsync(await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "claim-audit.sql")));
    }
}

[CollectionDefinition(Name)]
public sealed class MultiPodCollection : ICollectionFixture<MultiPodFixture>
{
    public const string Name = "MultiPod";
}
