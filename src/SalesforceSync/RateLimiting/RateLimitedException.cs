namespace SalesforceSync.RateLimiting;

public sealed class RateLimitedException(TimeSpan retryAfter)
    : Exception($"Salesforce rate limited; retry after {retryAfter}")
{
    public TimeSpan RetryAfter { get; } = retryAfter;
}
