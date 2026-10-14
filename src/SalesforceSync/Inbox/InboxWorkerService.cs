using Microsoft.Extensions.Options;
using SalesforceSync.Sync;

namespace SalesforceSync.Inbox;

/// <summary>
/// The one worker loop of the process: one batch of every entity type (accounts, relationships, ...) per pass, in a
/// fresh scope (fresh typed HttpClient), one after the other. Nothing runs in parallel, and nothing keeps a second
/// worker (another loop or pod) off the same rows: run exactly one.
/// </summary>
public sealed class InboxWorkerService(
    IServiceScopeFactory scopes,
    IOptions<SyncOptions> options,
    ILogger<InboxWorkerService> log) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var processed = 0;
            try
            {
                processed = await SyncOnceAsync(scopes, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Worker iteration failed");
            }

            if (processed == 0)
            {
                try
                {
                    await Task.Delay(options.Value.IdleDelay, stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }
    }

    /// <summary>
    /// One pass: a batch of every registered entity type, in a fresh scope (fresh typed HttpClient). Also used by the
    /// benchmarks, so they drive exactly what the worker runs.
    /// </summary>
    /// <returns>Inbox rows processed across all types (0 = nothing to do).</returns>
    public static async Task<int> SyncOnceAsync(IServiceScopeFactory scopes, CancellationToken ct)
    {
        await using var scope = scopes.CreateAsyncScope();
        var processed = 0;
        foreach (var sync in scope.ServiceProvider.GetServices<ISalesforceEntitySync>())
        {
            processed += await sync.SyncBatchAsync(ct);
        }

        return processed;
    }
}