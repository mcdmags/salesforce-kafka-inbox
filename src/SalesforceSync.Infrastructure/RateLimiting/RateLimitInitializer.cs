namespace SalesforceSync.Infrastructure.RateLimiting;

/// <summary>
/// Applies the configured Salesforce limits to the shared rate-limit row. Registered after
/// <see cref="Data.SyncSchemaInitializer"/> and before the workers (hosted services start in registration order).
/// </summary>
public sealed class RateLimitInitializer(SalesforceRateLimiter limiter) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken)
    {
        return limiter.ApplyConfigurationAsync(cancellationToken);
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}