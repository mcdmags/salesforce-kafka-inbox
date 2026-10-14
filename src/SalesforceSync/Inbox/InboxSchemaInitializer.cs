using Npgsql;
using SalesforceSync.Data;
using SalesforceSync.Sync;

namespace SalesforceSync.Inbox;

/// <summary>
/// Runs before the workers start (hosted services start in registration order): creates the shared tables and one
/// inbox table per registered <see cref="ISalesforceEntitySync"/>.
/// </summary>
public sealed class InboxSchemaInitializer(NpgsqlDataSource db, PostgresExecutionStrategy pg, IServiceScopeFactory scopes,
    ILogger<InboxSchemaInitializer> log) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        string[] tables;
        using (var scope = scopes.CreateScope())
        {
            tables = [.. scope.ServiceProvider.GetServices<ISalesforceEntitySync>().Select(s => s.InboxTable)];
        }

        await InboxSchema.EnsureCreatedAsync(db, tables, cancellationToken, pg);
        log.LogInformation("Inbox schema is up to date ({Tables})", string.Join(", ", tables));
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}