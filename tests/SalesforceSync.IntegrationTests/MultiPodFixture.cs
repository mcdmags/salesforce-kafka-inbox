using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;
using Dapper;

namespace SalesforceSync.IntegrationTests;

/// <summary>
/// The same app, scaled out like a Kubernetes deployment: several worker processes ("pods"), each running
/// several worker loops, all sharing one Postgres inbox and one Salesforce org.
/// <para>
/// Salesforce itself does nothing to stop an out-of-order write, so any race lands in the mock org's write
/// history. Batches are kept small so workers contend for the same accounts as much as possible.
/// </para>
/// </summary>
public sealed class MultiPodFixture : AppHostFixture
{
    public const int Pods = 3;
    public const int WorkersPerPod = 8;

    protected override void Configure(IDistributedApplicationTestingBuilder builder)
    {
        Project(builder, "salesforce-sync")
            .WithReplicas(Pods)
            .WithEnvironment("Sync__Workers", WorkersPerPod.ToString())
            .WithEnvironment("Sync__BatchSize", "10")
            .WithEnvironment("Sync__IdleDelay", "00:00:00.050")
            // Short lease: a claim lost to a dropped connection (chaos test) is reclaimed quickly
            .WithEnvironment("Sync__Lease", "00:00:20");

        Project(builder, "fake-salesforce")
            .WithEnvironment("FakeSalesforce__Latency", "00:00:00.010");
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
