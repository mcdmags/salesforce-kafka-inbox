namespace SalesforceSync.FakeSalesforce.Server;

/// <summary>Logs the fake org's counters periodically (visible in the Aspire dashboard).</summary>
public sealed class StatsLogger(FakeSalesforceStore store, ILogger<StatsLogger> log) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(15));
        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken))
                log.LogInformation("Fake Salesforce: {Stats}", store.GetStats());
        }
        catch (OperationCanceledException)
        {
            // shutting down
        }
    }
}
