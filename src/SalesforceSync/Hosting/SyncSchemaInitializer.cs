using Microsoft.EntityFrameworkCore;
using SalesforceSync.Data;

namespace SalesforceSync.Hosting;

/// <summary>
/// Runs before the workers start (hosted services start in registration order): migrates the database
/// (<see cref="SyncSchema.MigrateAsync"/>).
/// </summary>
public sealed class SyncSchemaInitializer(IDbContextFactory<SyncDbContext> contexts, ILogger<SyncSchemaInitializer> log) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await SyncSchema.MigrateAsync(contexts, cancellationToken);
        log.LogInformation("Database schema is up to date");
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}