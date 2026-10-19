using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace SalesforceSync.IntegrationTests.Hosting;

/// <summary>
/// One worker "pod": its own <see cref="IHost"/> in the test process, wired by the worker's own
/// <see cref="SyncServiceCollectionExtensions.AddSalesforceSyncWorker"/> (the call Program.cs makes). It has its own
/// Postgres pool, Kafka consumer (in the shared consumer group), token cache, HTTP stack and worker loops.
/// Several pods on the same containers behave like replicas of a Kubernetes deployment.
/// </summary>
public sealed class WorkerPod : IAsyncDisposable
{
    private readonly IHost _host;

    private WorkerPod(IHost host)
    {
        _host = host;
    }

    public IServiceProvider Services => _host.Services;

    /// <summary>The deploy's migrate step (<see cref="SyncServiceCollectionExtensions.MigrateAsync"/>) with a pod's settings.</summary>
    public static Task MigrateAsync(IReadOnlyDictionary<string, string?> settings)
    {
        return CreateBuilder(settings).MigrateAsync();
    }

    private static HostApplicationBuilder CreateBuilder(IReadOnlyDictionary<string, string?> settings)
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
        return builder;
    }

    /// <summary>
    /// Builds and starts the pod. Configuration: the worker's appsettings.json, then <paramref name="settings"/>
    /// (connection strings, Salesforce endpoint, per-pod Sync settings). Nothing comes from the environment, so
    /// ConnectionStrings__* variables on a developer machine can't redirect the tests.
    /// </summary>
    /// <remarks>Returns once the hosted services have started and the loops are running. The schema must be migrated (<see cref="MigrateAsync"/>).</remarks>
    public static async Task<WorkerPod> StartAsync(IReadOnlyDictionary<string, string?> settings, CancellationToken ct)
    {
        var builder = CreateBuilder(settings);

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
        return new WorkerPod(host);
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