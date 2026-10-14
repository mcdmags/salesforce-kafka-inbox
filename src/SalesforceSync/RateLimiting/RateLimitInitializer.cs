namespace SalesforceSync.RateLimiting;

/// <summary>
/// Applies the configured Salesforce limits to the shared rate-limit row. Registered after
/// <see cref="Inbox.InboxSchemaInitializer"/> and before the workers (hosted services start in registration order).
/// </summary>
public sealed class RateLimitInitializer(SalesforceRateLimiter limiter) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken) => limiter.ApplyConfigurationAsync(cancellationToken);

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
