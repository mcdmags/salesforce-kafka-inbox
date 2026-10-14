using Microsoft.EntityFrameworkCore;
using SalesforceSync.Data;

namespace SalesforceSync.Inbox;

/// <summary>
/// Runs before the workers start (hosted services start in registration order): creates the shared tables and the
/// inbox table of every <see cref="InboxRow"/> type in the model.
/// </summary>
public sealed class InboxSchemaInitializer(IDbContextFactory<SyncDbContext> contexts, ILogger<InboxSchemaInitializer> log) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        string[] tables;
        using (var db = contexts.CreateDbContext())
        {
            tables = [.. db.InboxTables];
        }

        await InboxSchema.EnsureCreatedAsync(contexts, tables, cancellationToken);
        log.LogInformation("Inbox schema is up to date ({Tables})", string.Join(", ", tables));
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}