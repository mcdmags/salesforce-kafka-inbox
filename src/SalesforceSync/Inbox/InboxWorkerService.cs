using Microsoft.Extensions.Options;

namespace SalesforceSync.Inbox;

/// <summary>Runs N parallel worker loops; each iteration gets a fresh scope (fresh typed HttpClient).</summary>
public sealed class InboxWorkerService(
    IServiceScopeFactory scopes,
    IOptions<SyncOptions> options,
    ILogger<InboxWorkerService> log) : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var loops = Enumerable.Range(0, Math.Max(1, options.Value.Workers))
            .Select(i => RunLoopAsync(i, stoppingToken));
        return Task.WhenAll(loops);
    }

    private async Task RunLoopAsync(int loop, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            var processed = 0;
            try
            {
                await using var scope = scopes.CreateAsyncScope();
                var worker = scope.ServiceProvider.GetRequiredService<InboxWorker>();
                processed = await worker.RunOnceAsync(ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Worker loop {Loop} iteration failed", loop);
            }

            if (processed == 0)
            {
                var jitter = TimeSpan.FromMilliseconds(Random.Shared.Next(0, 500));
                try { await Task.Delay(options.Value.IdleDelay + jitter, ct); }
                catch (OperationCanceledException) { break; }
            }
        }
    }
}
