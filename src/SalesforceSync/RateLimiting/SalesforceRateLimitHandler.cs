using System.Diagnostics;
using System.Net;
using Microsoft.Extensions.Options;

namespace SalesforceSync.RateLimiting;

/// <summary>
/// Every Salesforce HTTP call takes a token from the shared bucket. The Sforce-Limit-Info header on each
/// response feeds back into the bucket's speed; limit errors pause all workers.
/// </summary>
public sealed class SalesforceRateLimitHandler(
    SalesforceRateLimiter limiter,
    IOptions<RateLimitOptions> options,
    IOptions<SalesforceLimitsOptions> limits,
    ILogger<SalesforceRateLimitHandler> log) : DelegatingHandler
{
    public const string LimitInfoHeader = "Sforce-Limit-Info";

    /// <summary>Pause after a 429/503 that doesn't say how long to wait.</summary>
    private static readonly TimeSpan DefaultRetryAfter = TimeSpan.FromSeconds(30);

    public enum LimitExceeded { ConcurrentLongRunning, TotalRequests }

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
    {
        await limiter.AcquireAsync(1, ct);
        var started = Stopwatch.GetTimestamp();
        var res = await base.SendAsync(request, ct);
        try
        {
            var elapsed = Stopwatch.GetElapsedTime(started);
            if (elapsed >= limits.Value.LongRunningThreshold)
                log.LogWarning(
                    "Salesforce call {Method} {Path} took {Elapsed}; calls of {Threshold} or longer count toward the org's " +
                    "limit of {Limit} concurrent long-running requests. Consider a smaller Sync:BatchSize.",
                    request.Method, request.RequestUri?.AbsolutePath, elapsed, limits.Value.LongRunningThreshold,
                    limits.Value.EffectiveConcurrentLongRunningLimit);

            await ReportLimitsAsync(res, ct);

            if (res.StatusCode is HttpStatusCode.ServiceUnavailable or HttpStatusCode.TooManyRequests)
            {
                var retryAfter = RetryAfter(res) ?? DefaultRetryAfter;
                await limiter.PauseAsync(retryAfter, ct);
                throw new RateLimitedException(retryAfter);
            }

            if (res.StatusCode == HttpStatusCode.Forbidden && Classify(await res.Content.ReadAsStringAsync(ct)) is { } exceeded)
            {
                var pause = exceeded == LimitExceeded.ConcurrentLongRunning
                    ? options.Value.ConcurrentLimitPause
                    : options.Value.DailyHardPause;
                log.LogWarning("Salesforce REQUEST_LIMIT_EXCEEDED ({Limit}); pausing all workers for {Pause}", exceeded, pause);
                await limiter.PauseAsync(pause, ct);
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
    private static TimeSpan? RetryAfter(HttpResponseMessage res) => res.Headers.RetryAfter switch
    {
        { Delta: { } delta } => delta,
        { Date: { } date } when date > DateTimeOffset.UtcNow => date - DateTimeOffset.UtcNow,
        _ => null
    };

    /// <summary>
    /// REQUEST_LIMIT_EXCEEDED covers two limits that clear very differently: too many concurrent long-running
    /// requests ("ConcurrentPerOrgLongTxn Limit exceeded", clears when one finishes) and the org's total requests
    /// per rolling 24 hours ("TotalRequests Limit exceeded", clears gradually). Unknown variants are treated as the
    /// latter, the safer assumption.
    /// </summary>
    public static LimitExceeded? Classify(string body)
    {
        if (!body.Contains("REQUEST_LIMIT_EXCEEDED", StringComparison.Ordinal)) return null;
        return body.Contains("ConcurrentPerOrgLongTxn", StringComparison.OrdinalIgnoreCase)
            ? LimitExceeded.ConcurrentLongRunning
            : LimitExceeded.TotalRequests;
    }

    private async Task ReportLimitsAsync(HttpResponseMessage res, CancellationToken ct)
    {
        if (!res.Headers.TryGetValues(LimitInfoHeader, out var values)) return;

        var info = SalesforceLimitInfo.Parse(string.Join("; ", values));
        if (info is null) return;

        try
        {
            await limiter.ReportLimitsAsync(info, ct);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Limit tracking must never fail the actual Salesforce call.
            log.LogWarning(ex, "Failed to record Salesforce limits {Info}", info);
        }
    }
}
