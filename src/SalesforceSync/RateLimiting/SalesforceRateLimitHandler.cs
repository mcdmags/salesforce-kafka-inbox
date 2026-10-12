using System.Net;

namespace SalesforceSync.RateLimiting;

/// <summary>
/// Every Salesforce HTTP call takes a token from the shared bucket. The Sforce-Limit-Info header on each
/// response feeds back into the bucket's speed; limit errors pause all workers.
/// </summary>
public sealed class SalesforceRateLimitHandler(
    SalesforceRateLimiter limiter,
    ILogger<SalesforceRateLimitHandler> log) : DelegatingHandler
{
    public const string LimitInfoHeader = "Sforce-Limit-Info";

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
    {
        await limiter.AcquireAsync(1, ct);
        var res = await base.SendAsync(request, ct);

        await ReportLimitsAsync(res, ct);

        if (res.StatusCode is HttpStatusCode.ServiceUnavailable or HttpStatusCode.TooManyRequests)
        {
            var retryAfter = res.Headers.RetryAfter?.Delta ?? TimeSpan.FromSeconds(30);
            res.Dispose();
            await limiter.PauseAsync(retryAfter, ct);
            throw new RateLimitedException(retryAfter);
        }

        if (res.StatusCode == HttpStatusCode.Forbidden)
        {
            var body = await res.Content.ReadAsStringAsync(ct);
            if (body.Contains("REQUEST_LIMIT_EXCEEDED", StringComparison.Ordinal))
            {
                res.Dispose();
                var pause = TimeSpan.FromMinutes(5);
                await limiter.PauseAsync(pause, ct);
                throw new RateLimitedException(pause);
            }
        }

        return res;
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
