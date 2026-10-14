namespace SalesforceSync.RateLimiting;

/// <param name="mayHaveBeenProcessed">
/// False (the normal case): the request was not sent, or Salesforce itself refused it. True: something in front of
/// Salesforce answered 503, so a write may have gone through all the same.
/// </param>
public sealed class RateLimitedException(TimeSpan retryAfter, bool mayHaveBeenProcessed = false)
    : Exception($"Salesforce rate limited; retry after {retryAfter}")
{
    public TimeSpan RetryAfter { get; } = retryAfter;

    public bool MayHaveBeenProcessed { get; } = mayHaveBeenProcessed;
}
