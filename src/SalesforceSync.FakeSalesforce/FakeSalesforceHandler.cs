using System.Net;
using System.Text;
using System.Text.Json.Nodes;
using System.Web;

namespace SalesforceSync.FakeSalesforce;

/// <summary>
/// HttpMessageHandler that answers Salesforce REST calls from a <see cref="FakeSalesforceStore"/>.
/// Plug it in as the primary handler so the real client, auth and rate-limit handlers stay in the pipeline.
/// </summary>
public sealed class FakeSalesforceHandler(FakeSalesforceStore store) : HttpMessageHandler
{
    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
    {
        var options = store.Options;
        var longRunning = options.Latency > TimeSpan.Zero && options.Latency >= options.LongRunningThreshold
                          && !IsTokenRequest(request); // OAuth is not an API request
        if (!longRunning) return await HandleAsync(request, ct);

        // Salesforce refuses new requests while the org already has the maximum of long-running ones in flight
        using var slot = store.TryStartLongRunning();
        if (slot is null)
        {
            store.CountApiCall();
            return Json(request, HttpStatusCode.Forbidden, FakeSalesforceStore.Errors("REQUEST_LIMIT_EXCEEDED",
                "ConcurrentPerOrgLongTxn Limit exceeded."));
        }
        return await HandleAsync(request, ct);
    }

    private async Task<HttpResponseMessage> HandleAsync(HttpRequestMessage request, CancellationToken ct)
    {
        var options = store.Options;
        if (options.Latency > TimeSpan.Zero) await Task.Delay(options.Latency, ct);

        var uri = request.RequestUri ?? throw new InvalidOperationException("Request has no URI");
        var path = uri.AbsolutePath;

        // OAuth token (does not count toward API limits)
        if (IsTokenRequest(request))
        {
            return Json(request, HttpStatusCode.OK, new JsonObject
            {
                ["access_token"] = "fake-token",
                ["instance_url"] = $"{uri.Scheme}://{uri.Authority}",
                ["token_type"] = "Bearer"
            });
        }

        var calls = store.CountApiCall();
        HttpResponseMessage res;

        if (options.EnforceDailyLimit && calls > options.DailyApiLimit)
        {
            res = Json(request, HttpStatusCode.Forbidden,
                FakeSalesforceStore.Errors("REQUEST_LIMIT_EXCEEDED", "TotalRequests Limit exceeded."));
        }
        else
        {
            try
            {
                if (request.Method == HttpMethod.Get && path.EndsWith("/query", StringComparison.OrdinalIgnoreCase))
                {
                    var soql = HttpUtility.ParseQueryString(uri.Query)["q"]
                               ?? throw new FakeSalesforceException(400, "MALFORMED_QUERY", "Missing q");
                    res = Json(request, HttpStatusCode.OK, store.Query(soql));
                }
                else if (request.Method == HttpMethod.Post && path.EndsWith("/composite/graph", StringComparison.OrdinalIgnoreCase))
                {
                    var text = request.Content is null ? "{}" : await request.Content.ReadAsStringAsync(ct);
                    var payload = JsonNode.Parse(text)?.AsObject()
                                  ?? throw new FakeSalesforceException(400, "JSON_PARSER_ERROR", "Empty body");
                    res = Json(request, HttpStatusCode.OK, store.ExecuteGraphs(payload));
                }
                else
                {
                    res = Json(request, HttpStatusCode.NotFound,
                        FakeSalesforceStore.Errors("NOT_FOUND", $"Fake does not support {request.Method} {path}"));
                }
            }
            catch (FakeSalesforceException ex)
            {
                res = Json(request, (HttpStatusCode)ex.Status, FakeSalesforceStore.Errors(ex.ErrorCode, ex.Message));
            }
        }

        res.Headers.TryAddWithoutValidation("Sforce-Limit-Info", $"api-usage={calls}/{options.DailyApiLimit}");
        return res;
    }

    private static bool IsTokenRequest(HttpRequestMessage request)
        => request.RequestUri?.AbsolutePath.EndsWith("/services/oauth2/token", StringComparison.OrdinalIgnoreCase) == true;

    private static HttpResponseMessage Json(HttpRequestMessage request, HttpStatusCode status, JsonNode body) => new(status)
    {
        RequestMessage = request,
        Content = new StringContent(body.ToJsonString(), Encoding.UTF8, "application/json")
    };
}
