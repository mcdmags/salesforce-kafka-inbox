namespace SalesforceSync.Infrastructure.RateLimiting;

/// <summary>
/// What a Salesforce call needs from the rate limiter shared by every worker in every pod (its state is in Postgres,
/// see <c>SalesforceRateLimiter</c> in the Data project): permission to call, a pause when Salesforce asks for one,
/// and the limits Salesforce reports back.
/// </summary>
public interface ISalesforceRateLimiter
{
    /// <summary>Waits until a call may be made, and counts it against the daily budget.</summary>
    Task AcquireAsync(CancellationToken ct);

    /// <summary>Pauses every call, in every process, for <paramref name="duration"/>.</summary>
    Task PauseAsync(TimeSpan duration, CancellationToken ct);

    /// <summary>Adjusts the bucket's speed and the budget from the Sforce-Limit-Info a response carried.</summary>
    Task ReportLimitsAsync(SalesforceLimitInfo info, CancellationToken ct);
}
