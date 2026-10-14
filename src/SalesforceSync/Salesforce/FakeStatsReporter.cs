using SalesforceSync.FakeSalesforce;

namespace SalesforceSync.Salesforce;

/// <summary>When running against the fake org, logs its counters periodically (visible in the Aspire dashboard).</summary>
public sealed class FakeStatsReporter(FakeSalesforceStore store, ILogger<FakeStatsReporter> log) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(15));
        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                log.LogInformation("Fake Salesforce: {Stats}", store.GetStats());
            }
        }
        catch (OperationCanceledException)
        {
            // shutting down
        }
    }
}