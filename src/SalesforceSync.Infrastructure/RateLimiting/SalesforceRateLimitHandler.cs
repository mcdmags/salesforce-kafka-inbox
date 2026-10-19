using System.Diagnostics;
using System.Net;
using Microsoft.Extensions.Options;
using SalesforceSync.Infrastructure.Salesforce;

namespace SalesforceSync.Infrastructure.RateLimiting;

/// <summary>
/// Every Salesforce HTTP call waits for the process's pause to end. The Sforce-Limit-Info header on each
/// response can start a pause near the daily limit; limit errors pause the process's workers.
/// </summary>
public sealed class SalesforceRateLimitHandler(
    ISalesforceRateLimiter limiter,
    IOptions<RateLimitOptions> options,
    IOptions<SalesforceLimitsOptions> limits,
    ILogger<SalesforceRateLimitHandler> log) : DelegatingHandler
{
    public const string LimitInfoHeader = "Sforce-Limit-Info";

    /// <summary>
    /// Pause after a 429/503 that doesn't say how long to wait. Salesforce documents 503 (server down for maintenance
    /// or overloaded) and REQUEST_LIMIT_EXCEEDED as 403; 429 isn't in its REST status codes, handled in case something
    /// in front of it sends one.
    /// </summary>
    private static readonly TimeSpan DefaultRetryAfter = TimeSpan.FromSeconds(30);

    public enum LimitExceeded { ConcurrentLongRunning, TotalRequests }

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
    {
        try
        {
            await limiter.AcquireAsync(ct);
        }
        catch (Exception ex)
        {
            // Nothing has been sent yet: a pause outlasting MaxWait or a cancelled wait is not a write whose outcome
            // is unknown
            WriteOutcome.MarkNotSent(ex);
            throw;
        }
        var started = Stopwatch.GetTimestamp();
        var res = await base.SendAsync(request, ct);
        try
        {
            var elapsed = Stopwatch.GetElapsedTime(started);
            if (elapsed >= limits.Value.LongRunningThreshold)
            {
                log.LogWarning(
                    "Salesforce call {Method} {Path} took {Elapsed}; calls of {Threshold} or longer count toward the org's " +
                    "limit of {Limit} concurrent long-running requests. Consider a smaller Sync:BatchSize.",
                    request.Method, request.RequestUri?.AbsolutePath, elapsed, limits.Value.LongRunningThreshold,
                    limits.Value.ConcurrentLongRunningLimit);
            }

            ReportLimits(res);

            if (res.StatusCode is HttpStatusCode.ServiceUnavailable or HttpStatusCode.TooManyRequests)
            {
                var retryAfter = RetryAfter(res) ?? DefaultRetryAfter;
                limiter.Pause(retryAfter);
                // A 503 that isn't Salesforce's own (a gateway in front of it) doesn't say whether the request was
                // processed: everyone pauses just the same, but a write must be treated as possibly applied
                var processed = res.StatusCode == HttpStatusCode.ServiceUnavailable &&
                                !new SalesforceResponseException(res.StatusCode, await res.Content.ReadAsStringAsync(ct)).IsSalesforceError;
                throw new RateLimitedException(retryAfter, mayHaveBeenProcessed: processed);
            }

            if (res.StatusCode == HttpStatusCode.Forbidden && Classify(await res.Content.ReadAsStringAsync(ct)) is { } exceeded)
            {
                var pause = exceeded == LimitExceeded.ConcurrentLongRunning
                    ? options.Value.ConcurrentLimitPause
                    : options.Value.DailyHardPause;
                log.LogWarning("Salesforce REQUEST_LIMIT_EXCEEDED ({Limit}); pausing this pod's workers for {Pause}", exceeded, pause);
                limiter.Pause(pause);
                throw new RateLimitedException(pause);
            }

            return res;
        }
        catch
        {
            // The caller never sees the response, so it can't dispose it
            res.Dispose();
            throw;
        }
    }

    /// <summary>Retry-After as a delay, whether sent as seconds or as an HTTP date.</summary>
    private static TimeSpan? RetryAfter(HttpResponseMessage res)
    {
        return res.Headers.RetryAfter switch
        {
            { Delta: { } delta } => delta,
            { Date: { } date } when date > DateTimeOffset.UtcNow => date - DateTimeOffset.UtcNow,
            _ => null
        };
    }

    /// <summary>
    /// REQUEST_LIMIT_EXCEEDED covers two limits that clear very differently: too many concurrent long-running
    /// requests ("ConcurrentPerOrgLongTxn Limit exceeded", clears when one finishes) and the org's total requests
    /// per rolling 24 hours ("TotalRequests Limit exceeded", clears gradually). Unknown variants are treated as the
    /// latter, the safer assumption.
    /// </summary>
    public static LimitExceeded? Classify(string body)
    {
        return !body.Contains("REQUEST_LIMIT_EXCEEDED", StringComparison.Ordinal)
            ? null
            : body.Contains("ConcurrentPerOrgLongTxn", StringComparison.OrdinalIgnoreCase)
            ? LimitExceeded.ConcurrentLongRunning
            : LimitExceeded.TotalRequests;
    }

    private void ReportLimits(HttpResponseMessage res)
    {
        if (!res.Headers.TryGetValues(LimitInfoHeader, out var values))
        {
            return;
        }

        if (SalesforceLimitInfo.Parse(string.Join("; ", values)) is { } info)
        {
            limiter.ReportLimits(info);
        }
    }
}
