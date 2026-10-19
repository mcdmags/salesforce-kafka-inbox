namespace SalesforceSync.Infrastructure.RateLimiting;

/// <summary>
/// What a Salesforce call needs from this process's rate limiter (see <c>SalesforceRateLimiter</c>): permission to
/// call (not while paused), a pause when Salesforce asks for one, and the limits Salesforce reports back. State is in
/// memory: Salesforce has no per-second limit and reports the daily one in every response, so pods have nothing to
/// agree on.
/// </summary>
public interface ISalesforceRateLimiter
{
    /// <summary>Whether a pause is in force now: no batch starts, and no call is made, until it ends.</summary>
    bool IsPaused { get; }

    /// <summary>Waits until a call may be made: until a pause ends.</summary>
    Task AcquireAsync(CancellationToken ct);

    /// <summary>Pauses every call of this process for <paramref name="duration"/>, unless it is paused for longer already.</summary>
    void Pause(TimeSpan duration);

    /// <summary>Pauses near exhaustion of the daily allocation, from the Sforce-Limit-Info a response carried.</summary>
    void ReportLimits(SalesforceLimitInfo info);
}
