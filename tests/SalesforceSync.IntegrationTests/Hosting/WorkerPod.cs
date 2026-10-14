using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace SalesforceSync.IntegrationTests.Hosting;

/// <summary>
/// One worker "pod": its own <see cref="IHost"/> in the test process, wired by the worker's own
/// <see cref="SyncServiceCollectionExtensions.AddSalesforceSyncWorker"/> (the same call Program.cs makes), so it has
/// its own Postgres pool, Kafka consumer (in the shared consumer group), token cache, HTTP stack and worker loops.
/// Several pods against the same containers behave like replicas of a Kubernetes deployment.
/// </summary>
public sealed class WorkerPod : IAsyncDisposable
{
    private readonly IHost _host;

    private WorkerPod(string name, IHost host)
    {
        Name = name;
        _host = host;
    }

    public string Name { get; }

    public IServiceProvider Services => _host.Services;

    /// <summary>
    /// Builds and starts the pod. Configuration: the worker's appsettings.json, then <paramref name="settings"/>
    /// (connection strings, Salesforce endpoint, per-pod Sync settings). Nothing comes from the environment, so a
    /// developer's ConnectionStrings__* variables can't redirect the tests.
    /// </summary>
    /// <remarks>Returns once the hosted services have started: the inbox schema is in place, the loops are running.</remarks>
    public static async Task<WorkerPod> StartAsync(string name, IReadOnlyDictionary<string, string?> settings, CancellationToken ct)
    {
        var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings
        {
            DisableDefaults = true,
            ContentRootPath = AppContext.BaseDirectory
        });
        builder.Configuration
            .AddJsonFile(Path.Combine(AppContext.BaseDirectory, "worker.appsettings.json"), optional: false)
            .AddInMemoryCollection(settings);
        builder.Logging
            .AddConfiguration(builder.Configuration.GetSection("Logging"))
            .AddSimpleConsole(o => o.IncludeScopes = false);

        builder.AddSalesforceSyncWorker();

        var host = builder.Build();
        try
        {
            await host.StartAsync(ct);
        }
        catch
        {
            host.Dispose();
            throw;
        }
        return new WorkerPod(name, host);
    }

    public async ValueTask DisposeAsync()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        try
        {
            await _host.StopAsync(timeout.Token);
        }
        finally
        {
            _host.Dispose();
        }
    }
}