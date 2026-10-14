using System.Net;

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