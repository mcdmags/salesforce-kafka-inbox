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
        WorkerSettings["Sync:BatchSize"] = "10";
        WorkerSettings["Sync:IdleDelay"] = "00:00:00.050";
        // Short lease: a claim lost to a dropped connection (chaos test) is reclaimed quickly
        WorkerSettings["Sync:Lease"] = "00:00:20";
        // A lease must outlast the HTTP timeout (startup validation), and the timeout the wait for a token
        WorkerSettings["SalesforceLimits:RequestTimeout"] = "00:00:15";
        WorkerSettings["RateLimit:MaxWait"] = "00:00:05";
        // The chaos test drops every connection every 200 ms, so under load several attempts in a row can be killed.
        // More, shorter retries (at most about 12 s in all, well inside the lease) let a unit of work outlast that.
        // With the defaults (5 retries) a batch whose outcomes can't be recorded keeps its lease until it expires,
        // and the takeover after expiry shows up in the claim audit as an overlap.
        WorkerSettings["Postgres:Retry:MaxRetryCount"] = "15";
        WorkerSettings["Postgres:Retry:MaxRetryDelay"] = "00:00:01";
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