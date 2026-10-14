using System.Net;
using SalesforceSync.RateLimiting;

namespace SalesforceSync.Salesforce;

/// <summary>Salesforce answered with an error status. <see cref="IsSalesforceError"/>: the body is Salesforce's own error list.</summary>
public sealed class SalesforceResponseException(HttpStatusCode status, string body)
    : HttpRequestException($"Salesforce {(int)status}: {body}", null, status)
{
    /// <summary>
    /// True when Salesforce itself produced the response (<c>[{ "errorCode": ..., "message": ... }]</c>), as opposed to a
    /// gateway or load balancer in front of it, which can't know whether the request was processed.
    /// </summary>
    public bool IsSalesforceError { get; } = body.Contains("\"errorCode\"", StringComparison.Ordinal);
}

/// <summary>
/// A write was sent (or may have been) and its result never arrived, so Salesforce may still apply it, possibly later.
/// The entities in it must not be written again until it has either landed or been abandoned.
/// </summary>
public sealed class UnknownOutcomeException(Exception inner)
    : Exception($"The outcome of the Salesforce write is unknown: {inner.Message}", inner)
{
    /// <summary>Stored as <c>last_error_code</c> on the held rows.</summary>
    public const string Code = "UNKNOWN_OUTCOME";
}

/// <summary>What a failed write call says about whether Salesforce applied it.</summary>
public static class WriteOutcome
{
    /// <summary>
    /// False only when the failure proves the write was not applied; everything else is unknown, the safe default.
    /// <list type="bullet">
    /// <item>Not applied: a failure before the request left (<see cref="MarkNotSent"/>), Salesforce's own rejection (any 4xx, or a 5xx carrying a Salesforce error body), a rate limit
    /// (no token before sending, 429, REQUEST_LIMIT_EXCEEDED, a 503 from Salesforce), or a connection that was never
    /// established (refused, DNS, TLS handshake).</item>
    /// <item>Unknown: a cancellation or timeout (the lease cutoff, the HTTP timeout, shutdown), a connection lost once
    /// the request may have left, 502/503/504 from something in front of Salesforce, or a response that can't be read.</item>
    /// </list>
    /// </summary>
    public static bool IsUnknown(Exception ex) => !IsMarkedNotSent(ex) && ex switch
    {
        RateLimitedException limited => limited.MayHaveBeenProcessed,
        SalesforceResponseException r => (int)r.StatusCode! >= 500 && !r.IsSalesforceError,
        HttpRequestException { StatusCode: { } status } => (int)status >= 500,
        HttpRequestException
        {
            HttpRequestError: HttpRequestError.ConnectionError or HttpRequestError.NameResolutionError
                or HttpRequestError.SecureConnectionError or HttpRequestError.ProxyTunnelError
        } => false,
        ArgumentException => false, // refused by the client before anything was sent (too many collections or records)
        _ => true
    };

    private const string NotSentKey = "SalesforceSync.NotSent";

    /// <summary>
    /// Marks a failure that happened in the client's own pipeline before the request left (waiting for a rate-limit
    /// token, fetching the OAuth token), so it is never taken for a write that may have been applied. The exception
    /// keeps its type; rethrow it.
    /// </summary>
    public static void MarkNotSent(Exception ex) => ex.Data[NotSentKey] = true;

    private static bool IsMarkedNotSent(Exception? ex)
    {
        for (; ex is not null; ex = ex.InnerException)
            if (ex.Data.Contains(NotSentKey)) return true;
        return false;
    }
}
