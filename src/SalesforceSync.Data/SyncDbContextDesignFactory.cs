using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace SalesforceSync.Data;

/// <summary>
/// Context for <c>dotnet ef</c> (adding a migration only reads the model), so the tool needn't start the worker host
/// with its Postgres, Kafka and Salesforce settings. Commands that connect (<c>database update</c>) take
/// <c>--connection</c>.
/// </summary>
public sealed class SyncDbContextDesignFactory : IDesignTimeDbContextFactory<SyncDbContext>
{
    public SyncDbContext CreateDbContext(string[] args)
    {
        return new SyncDbContext(new DbContextOptionsBuilder<SyncDbContext>().UseNpgsql("Host=localhost").Options);
    }
}