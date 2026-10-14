using Microsoft.Extensions.Options;
using SalesforceSync.Sync;

namespace SalesforceSync.Inbox;

/// <summary>
/// Runs N parallel worker loops. Each iteration gets a fresh scope (fresh typed HttpClient) and runs one batch of
/// every entity type (accounts, relationships, ...), starting at a different type per loop so none waits behind another.
/// </summary>
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
        for (var iteration = 0; !ct.IsCancellationRequested; iteration++)
        {
            var processed = 0;
            try
            {
                processed = await SyncOnceAsync(scopes, loop + iteration, ct);
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

    /// <summary>
    /// One pass: a batch of every registered entity type, in a fresh scope (fresh typed HttpClient), starting at
    /// type <paramref name="rotation"/> mod N so loops don't all queue behind the same type. Also used by the
    /// benchmarks, so they drive exactly what the worker runs.
    /// </summary>
    /// <returns>Inbox rows processed across all types (0 = nothing to do).</returns>
    public static async Task<int> SyncOnceAsync(IServiceScopeFactory scopes, int rotation, CancellationToken ct)
    {
        await using var scope = scopes.CreateAsyncScope();
        var syncs = scope.ServiceProvider.GetServices<ISalesforceEntitySync>().ToList();
        var processed = 0;
        for (var i = 0; i < syncs.Count; i++)
        {
            processed += await syncs[(rotation + i) % syncs.Count].SyncBatchAsync(ct);
        }

        return processed;
    }
}