using SalesforceSync.RateLimiting;

namespace SalesforceSync.Hosting;

/// <summary>
/// Applies the configured Salesforce limits to the shared rate-limit row. Registered before the
/// workers (hosted services start in registration order). The schema must already be migrated (<c>--migrate</c>).
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