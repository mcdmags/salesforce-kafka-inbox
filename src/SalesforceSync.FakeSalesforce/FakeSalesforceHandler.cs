using System.IO.Compression;
using System.Net;
using System.Text;
using System.Text.Json.Nodes;

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
                ["access_token"] = store.AccessToken,
                ["instance_url"] = $"{uri.Scheme}://{uri.Authority}",
                ["token_type"] = "Bearer"
            });
        }

        // Refused before it reaches the API (so not an API call), as Salesforce answers a request line and headers too long
        if (UriAndHeadersBytes(request) is var size && size > FakeSalesforceStore.MaxUriAndHeadersBytes)
        {
            return Encoding.UTF8.GetByteCount(uri.PathAndQuery) > FakeSalesforceStore.MaxUriAndHeadersBytes
                ? Json(request, HttpStatusCode.RequestUriTooLong, FakeJson.Errors("URI_TOO_LONG", "The URI is too long"))
                : Json(request, HttpStatusCode.RequestHeaderFieldsTooLarge,
                    FakeJson.Errors("REQUEST_HEADER_FIELDS_TOO_LARGE", $"URI and headers are {size:N0} bytes"));
        }

        // A session that has expired, been revoked or was never opened: the client must get a new token
        if (options.RequireAccessToken && request.Headers.Authorization?.ToString() != $"Bearer {store.AccessToken}")
        {
            return Json(request, HttpStatusCode.Unauthorized, FakeJson.Errors("INVALID_SESSION_ID", "Session expired or invalid"));
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
                res = request.Method == HttpMethod.Patch && FakeSalesforceStore.UpsertTarget(path) is (var sobject, var field)
                    ? Json(request, HttpStatusCode.OK, store.Upsert(sobject, field, path, await ReadJsonAsync(request, ct)))
                    : FakeSalesforceStore.IsDelete(request.Method.Method, path)
                    ? Json(request, HttpStatusCode.OK, store.Delete(uri))
                    : request.Method == HttpMethod.Get && FakeSalesforceStore.IsQuery(path)
                    ? Json(request, HttpStatusCode.OK, store.Query(uri))
                    : FakeSalesforceStore.IngestPath(path) is var (jobId, part)
                    ? await IngestAsync(request, jobId, part, ct)
                    : Json(request, HttpStatusCode.NotFound,
                    FakeJson.Errors("NOT_FOUND", $"Fake does not support {request.Method} {path}"));
            }
            catch (FakeSalesforceException ex)
            {
                res = Json(request, (HttpStatusCode)ex.Status, FakeJson.Errors(ex.ErrorCode, ex.Message));
            }
        }

        res.Headers.TryAddWithoutValidation("Sforce-Limit-Info", $"api-usage={calls}/{options.DailyApiLimit}");
        return res;
    }

    /// <summary>Bulk API 2.0 ingest calls: create, upload (CSV), close or abort, state, failed records (CSV).</summary>
    private async Task<HttpResponseMessage> IngestAsync(HttpRequestMessage request, string? jobId, string? part, CancellationToken ct)
    {
        var method = request.Method;
        if (jobId is not null && part == "batches" && method == HttpMethod.Put)
        {
            store.UploadIngestJob(jobId, await ReadTextAsync(request, ct));
            return new HttpResponseMessage(HttpStatusCode.Created) { RequestMessage = request };
        }

        return jobId is null
            ? method == HttpMethod.Post ? Json(request, HttpStatusCode.OK, store.CreateIngestJob(await ReadJsonAsync(request, ct))) : NotSupported(request)
            : part == "failedResults" && method == HttpMethod.Get
            ? new HttpResponseMessage(HttpStatusCode.OK)
            {
                RequestMessage = request,
                Content = new StringContent(store.IngestJobFailedResults(jobId), Encoding.UTF8, "text/csv")
            }
            : part is null && method == HttpMethod.Patch ? Json(request, HttpStatusCode.OK, store.UpdateIngestJob(jobId, await ReadJsonAsync(request, ct)))
            : part is null && method == HttpMethod.Get ? Json(request, HttpStatusCode.OK, store.IngestJob(jobId))
            : NotSupported(request);
    }

    private static HttpResponseMessage NotSupported(HttpRequestMessage request)
    {
        return Json(request, HttpStatusCode.NotFound, FakeJson.Errors("NOT_FOUND", $"Fake does not support {request.Method} {request.RequestUri!.AbsolutePath}"));
    }

    /// <summary>The request's URI (path and query) and its headers as sent: "Name: value" and a line break each.</summary>
    private static int UriAndHeadersBytes(HttpRequestMessage request)
    {
        var headers = request.Headers.Concat(request.Content?.Headers ?? Enumerable.Empty<KeyValuePair<string, IEnumerable<string>>>())
            .Sum(h => Encoding.UTF8.GetByteCount($"{h.Key}: {string.Join(", ", h.Value)}\r\n"));
        return Encoding.UTF8.GetByteCount(request.RequestUri!.PathAndQuery) + headers;
    }

    /// <summary>Request body as JSON; gzip bodies (Content-Encoding: gzip) are decompressed like Salesforce does.</summary>
    private static async Task<JsonObject> ReadJsonAsync(HttpRequestMessage request, CancellationToken ct)
    {
        using var buffer = await ReadBodyAsync(request, ct);
        return JsonNode.Parse(buffer)?.AsObject() ?? throw new FakeSalesforceException(400, "JSON_PARSER_ERROR", "Empty body");
    }

    private static async Task<string> ReadTextAsync(HttpRequestMessage request, CancellationToken ct)
    {
        using var buffer = await ReadBodyAsync(request, ct);
        return Encoding.UTF8.GetString(buffer.ToArray());
    }

    private static async Task<MemoryStream> ReadBodyAsync(HttpRequestMessage request, CancellationToken ct)
    {
        if (request.Content is null)
        {
            throw new FakeSalesforceException(400, "JSON_PARSER_ERROR", "Empty body");
        }

        await using var stream = await request.Content.ReadAsStreamAsync(ct);
        await using var body = request.Content.Headers.ContentEncoding.Contains("gzip")
            ? new GZipStream(stream, CompressionMode.Decompress)
            : stream;
        var buffer = new MemoryStream();
        await body.CopyToAsync(buffer, ct); // the server's request stream only allows async reads
        buffer.Position = 0;
        return buffer;
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