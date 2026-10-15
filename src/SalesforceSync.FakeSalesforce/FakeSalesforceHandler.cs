using System.IO.Compression;
using System.Net;
using System.Text;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using System.Web;

namespace SalesforceSync.FakeSalesforce;

/// <summary>
/// HttpMessageHandler that answers Salesforce REST calls from a <see cref="FakeSalesforceStore"/>.
/// Plug it in as the primary handler so the real client and auth handlers stay in the pipeline.
/// </summary>
public sealed partial class FakeSalesforceHandler(FakeSalesforceStore store) : HttpMessageHandler
{
    [GeneratedRegex(@"/sobjects/(?<obj>\w+)/describe/?$", RegexOptions.IgnoreCase)]
    private static partial Regex DescribeUrl();

    [GeneratedRegex(@"/composite/sobjects/(?<obj>\w+)/(?<field>\w+)/?$", RegexOptions.IgnoreCase)]
    private static partial Regex UpsertUrl();

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
    {
        var options = store.Options;
        var longRunning = options.Latency > TimeSpan.Zero && options.Latency >= options.LongRunningThreshold
                          && !IsTokenRequest(request); // OAuth is not an API request
        if (!longRunning)
        {
            return await HandleAsync(request, ct);
        }

        // Salesforce refuses new requests while the org already has the maximum of long-running ones in flight
        using var slot = store.TryStartLongRunning();
        if (slot is null)
        {
            store.CountApiCall();
            return Json(request, HttpStatusCode.Forbidden, FakeJson.Errors("REQUEST_LIMIT_EXCEEDED",
                "ConcurrentPerOrgLongTxn Limit exceeded."));
        }
        return await HandleAsync(request, ct);
    }

    private async Task<HttpResponseMessage> HandleAsync(HttpRequestMessage request, CancellationToken ct)
    {
        var options = store.Options;
        if (options.Latency > TimeSpan.Zero)
        {
            await Task.Delay(options.Latency, ct);
        }

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
                FakeJson.Errors("REQUEST_LIMIT_EXCEEDED", "TotalRequests Limit exceeded."));
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
                else
                {
                    var upsert = UpsertUrl().Match(path);
                    res = request.Method == HttpMethod.Patch && upsert.Success
                        ? Json(request, HttpStatusCode.OK, store.Upsert(upsert.Groups["obj"].Value, upsert.Groups["field"].Value,
                            path, await ReadJsonAsync(request, ct)))
                        : request.Method == HttpMethod.Get && path.EndsWith("/query", StringComparison.OrdinalIgnoreCase)
                        ? Json(request, HttpStatusCode.OK, store.Query(QueryOf(uri)))
                        : request.Method == HttpMethod.Post && path.EndsWith("/composite", StringComparison.OrdinalIgnoreCase)
                        ? Json(request, HttpStatusCode.OK, store.ExecuteComposite(await ReadJsonAsync(request, ct)))
                        : Json(request, HttpStatusCode.NotFound,
                        FakeJson.Errors("NOT_FOUND", $"Fake does not support {request.Method} {path}"));
                }
            }
            catch (FakeSalesforceException ex)
            {
                res = Json(request, (HttpStatusCode)ex.Status, FakeJson.Errors(ex.ErrorCode, ex.Message));
            }
        }

        res.Headers.TryAddWithoutValidation("Sforce-Limit-Info", $"api-usage={calls}/{options.DailyApiLimit}");
        return res;
    }

    private static string QueryOf(Uri uri)
    {
        return HttpUtility.ParseQueryString(uri.Query)["q"] ?? throw new FakeSalesforceException(400, "MALFORMED_QUERY", "Missing q");
    }

    /// <summary>Request body as JSON; gzip bodies (Content-Encoding: gzip) are decompressed like Salesforce does.</summary>
    private static async Task<JsonObject> ReadJsonAsync(HttpRequestMessage request, CancellationToken ct)
    {
        if (request.Content is null)
        {
            throw new FakeSalesforceException(400, "JSON_PARSER_ERROR", "Empty body");
        }

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
    {
        return request.RequestUri?.AbsolutePath.EndsWith("/services/oauth2/token", StringComparison.OrdinalIgnoreCase) == true;
    }

    private static HttpResponseMessage Json(HttpRequestMessage request, HttpStatusCode status, JsonNode body)
    {
        return new(status)
        {
            RequestMessage = request,
            Content = new StringContent(body.ToJsonString(), Encoding.UTF8, "application/json")
        };
    }
}