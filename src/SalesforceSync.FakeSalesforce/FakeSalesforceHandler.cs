using System.IO.Compression;
using System.Net;
using System.Text;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using System.Web;

namespace SalesforceSync.FakeSalesforce;

/// <summary>
/// HttpMessageHandler that answers Salesforce REST calls from a <see cref="FakeSalesforceStore"/>.
/// Plug it in as the primary handler so the real client, auth and rate-limit handlers stay in the pipeline.
/// </summary>
public sealed partial class FakeSalesforceHandler(FakeSalesforceStore store) : HttpMessageHandler
{
    private const int MaxCompositeSubrequests = 25;
    private const int MaxCompositeQueries = 5;

    [GeneratedRegex(@"/sobjects/(?<obj>\w+)/describe/?$", RegexOptions.IgnoreCase)]
    private static partial Regex DescribeUrl();

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
                if (request.Method == HttpMethod.Get && DescribeUrl().Match(path) is { Success: true } describe)
                {
                    var (status, body) = store.Describe(describe.Groups["obj"].Value, request.Headers.IfModifiedSince);
                    res = body is null
                        ? new HttpResponseMessage((HttpStatusCode)status) { RequestMessage = request }
                        : Json(request, (HttpStatusCode)status, body);
                    res.Content.Headers.LastModified = store.SchemaLastModified;
                }
                else if (request.Method == HttpMethod.Get && path.EndsWith("/query", StringComparison.OrdinalIgnoreCase))
                {
                    res = Json(request, HttpStatusCode.OK, store.Query(QueryOf(uri)));
                }
                else if (request.Method == HttpMethod.Post && path.EndsWith("/composite/graph", StringComparison.OrdinalIgnoreCase))
                {
                    res = Json(request, HttpStatusCode.OK, store.ExecuteGraphs(await ReadJsonAsync(request, ct)));
                }
                else if (request.Method == HttpMethod.Post && path.EndsWith("/composite", StringComparison.OrdinalIgnoreCase))
                {
                    res = Json(request, HttpStatusCode.OK, ExecuteComposite(await ReadJsonAsync(request, ct)));
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

    /// <summary>Composite: the subrequests run one after another and the whole request is one API call.</summary>
    private JsonObject ExecuteComposite(JsonObject payload)
    {
        var subrequests = payload["compositeRequest"]?.AsArray()
                          ?? throw new FakeSalesforceException(400, "JSON_PARSER_ERROR", "Missing 'compositeRequest'");
        if (subrequests.Count > MaxCompositeSubrequests)
            throw new FakeSalesforceException(400, "LIMIT_EXCEEDED", $"A maximum of {MaxCompositeSubrequests} subrequests is allowed");
        if (subrequests.Count(r => (r?["url"]?.GetValue<string>() ?? "").Contains("/query", StringComparison.OrdinalIgnoreCase)) > MaxCompositeQueries)
            throw new FakeSalesforceException(400, "LIMIT_EXCEEDED", $"A maximum of {MaxCompositeQueries} query subrequests is allowed");

        store.CountCompositeRequest();
        var responses = new JsonArray();
        foreach (var sub in subrequests)
        {
            var referenceId = sub!["referenceId"]!.GetValue<string>();
            var method = sub["method"]!.GetValue<string>();
            var url = new Uri(new Uri("https://fake"), sub["url"]!.GetValue<string>());
            try
            {
                if (!method.Equals("GET", StringComparison.OrdinalIgnoreCase)
                    || !url.AbsolutePath.EndsWith("/query", StringComparison.OrdinalIgnoreCase))
                    throw new FakeSalesforceException(400, "NOT_SUPPORTED", $"Fake composite only runs queries, not {method} {url.AbsolutePath}");
                responses.Add(FakeSalesforceStore.SubResponse(referenceId, 200, store.Query(QueryOf(url))));
            }
            catch (FakeSalesforceException ex)
            {
                responses.Add(FakeSalesforceStore.SubResponse(referenceId, ex.Status, FakeSalesforceStore.Errors(ex.ErrorCode, ex.Message)));
            }
        }
        return new JsonObject { ["compositeResponse"] = responses };
    }

    private static string QueryOf(Uri uri)
        => HttpUtility.ParseQueryString(uri.Query)["q"] ?? throw new FakeSalesforceException(400, "MALFORMED_QUERY", "Missing q");

    /// <summary>Request body as JSON; gzip bodies (Content-Encoding: gzip) are decompressed like Salesforce does.</summary>
    private static async Task<JsonObject> ReadJsonAsync(HttpRequestMessage request, CancellationToken ct)
    {
        if (request.Content is null) throw new FakeSalesforceException(400, "JSON_PARSER_ERROR", "Empty body");
        await using var stream = await request.Content.ReadAsStreamAsync(ct);
        await using var body = request.Content.Headers.ContentEncoding.Contains("gzip")
            ? new GZipStream(stream, CompressionMode.Decompress)
            : stream;
        using var buffer = new MemoryStream();
        await body.CopyToAsync(buffer, ct); // the server's request stream only allows async reads
        buffer.Position = 0;
        return JsonNode.Parse(buffer)?.AsObject() ?? throw new FakeSalesforceException(400, "JSON_PARSER_ERROR", "Empty body");
    }

    private static bool IsTokenRequest(HttpRequestMessage request)
        => request.RequestUri?.AbsolutePath.EndsWith("/services/oauth2/token", StringComparison.OrdinalIgnoreCase) == true;

    private static HttpResponseMessage Json(HttpRequestMessage request, HttpStatusCode status, JsonNode body) => new(status)
    {
        RequestMessage = request,
        Content = new StringContent(body.ToJsonString(), Encoding.UTF8, "application/json")
    };
}
