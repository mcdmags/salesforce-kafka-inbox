using System.Net;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Options;

namespace SalesforceSync.Infrastructure.Salesforce;

/// <summary>
/// Salesforce REST transport, and the only class that talks to Salesforce. Every method is exactly one API call: it
/// takes requests and returns the raw results per subrequest and per record. It decides nothing (what to write, what
/// a result means); that is <c>SalesforceBatchSyncService</c>'s.
/// <para>
/// There are deliberately no single-record methods (get, update, delete or upsert one record by id): each costs one API
/// call per record, and the integration lives on a daily call budget (<c>SalesforceLimits:DailyApiAllocation</c>). The
/// client is bulk-only: up to 5 queries per call, and up to 200 records per collection with up to 5 collections per call.
/// Writing 200 records one by one would spend 200 calls where <see cref="WriteAsync"/> spends one.
/// </para>
/// </summary>
public sealed class SalesforceRestClient : ISalesforceRestClient
{
    private readonly HttpClient http;

    // The API path and version come from configuration (Salesforce:BaseApiPath, Salesforce:ApiVersion) rather than a
    // constant, so moving to another API version is a settings change. One constructor, so the typed HttpClient's
    // dependency injection has nothing to choose between (two made it throw when the pods started).
    public SalesforceRestClient(HttpClient http, IOptions<SalesforceOptions> options)
    {
        this.http = http;
        DataPath = options.Value.DataPath;
    }

    /// <summary>The path every call starts with (<see cref="SalesforceOptions.DataPath"/>).</summary>
    public string DataPath { get; }

    // The limits below are Salesforce's. They are the call-saving ceilings: the engine packs each call up to them, and the
    // client refuses anything over them before sending (a rejected call is still a call).

    /// <summary>Composite: at most 5 of its (25) subrequests may be queries or sObject Collections operations.</summary>
    public const int MaxCompositeQueries = 5;
    public const int MaxCompositeCollections = 5;

    /// <summary>
    /// sObject Collections: max records in one create, update or upsert; the ceiling of a row type's
    /// <c>IInboxRow.MaxRecordsPerWrite</c>.
    /// </summary>
    public const int MaxCollectionRecords = 200;

    /// <summary>
    /// External ids per query, at most, for its records to come back in one page: a query returns up to 2,000 records
    /// per page and each external id matches at most one record. The ceiling of a row type's
    /// <c>IInboxRow.MaxValuesPerQuery</c>.
    /// </summary>
    public const int MaxQueryValues = 2_000;

    /// <summary>
    /// A REST call's URI and headers together may be at most 16,384 bytes: over it Salesforce answers 414 URI Too Long
    /// (the URI alone) or 431 Request Header Fields Too Large (URI and headers). Salesforce Developer Limits and
    /// Allocations Quick Reference, "API Request Limits and Allocations"
    /// (https://developer.salesforce.com/docs/platform/salesforce-app-limits-cheatsheet/guide/salesforce-app-limits-platform-api.html);
    /// REST API Developer Guide, "Status Codes and Error Responses"
    /// (https://developer.salesforce.com/docs/platform/api-rest/guide/errorcodes.html).
    /// </summary>
    public const int MaxUriAndHeadersBytes = 16_384;

    /// <summary>
    /// The part of <see cref="MaxUriAndHeadersBytes"/> left to the headers this client sends: Authorization (a Bearer
    /// access token, about 100 bytes opaque, a JWT-based one a few times that), Host (the instance), User-Agent,
    /// Accept-Encoding, Content-Type, Content-Length, Content-Encoding, If-Modified-Since, and their framing. They come
    /// to well under 1 KB; 4 KB leaves room for a long token or a proxy's additions. A Composite subrequest's URL is
    /// held to the same budget: it is sent with the outer request's headers.
    /// </summary>
    public const int HeaderAllowanceBytes = 4_096;

    /// <summary>The longest URL (path and query) a call or Composite subrequest is sent with.</summary>
    public const int MaxUrlBytes = MaxUriAndHeadersBytes - HeaderAllowanceBytes;

    private static readonly JsonSerializerOptions Json = new() { PropertyNameCaseInsensitive = true };

    /// <summary>
    /// sObject Describe: the fields and the record types (<c>recordTypeInfos</c>); with
    /// <paramref name="ifModifiedSince"/>, returns null on 304 Not Modified.
    /// </summary>
    public async Task<SObjectSchema?> DescribeAsync(string sobject, DateTimeOffset? ifModifiedSince, CancellationToken ct)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, EnsureFits($"{DataPath}/sobjects/{sobject}/describe"));
        if (ifModifiedSince is { } since)
        {
            // A 304 still counts as an API call but skips the large Describe download. The schema cache (in-memory and
            // shared through Redis) is what saves the calls: it asks at most once per refresh interval, across all pods.
            request.Headers.IfModifiedSince = since;
        }

        using var res = await http.SendAsync(request, ct);
        if (res.StatusCode == HttpStatusCode.NotModified)
        {
            return null;
        }

        await EnsureSuccessAsync(res, ct);

        var body = await res.Content.ReadFromJsonAsync<DescribeBody>(Json, ct)
                   ?? throw new InvalidOperationException($"Empty describe response for {sobject}");
        return new SObjectSchema(body.Name, body.Fields, body.RecordTypeInfos);
    }

    /// <summary>
    /// Runs up to 5 queries in one Composite request (one API call). Results are keyed by reference id;
    /// a failed subrequest throws <see cref="SalesforceQueryException"/>.
    /// </summary>
    /// <exception cref="ArgumentException">A query's URL is longer than <see cref="MaxUrlBytes"/>.</exception>
    public async Task<Dictionary<string, List<JsonElement>>> QueryAsync(IReadOnlyList<CompositeQuery> queries, CancellationToken ct)
    {
        if (queries.Count is 0 or > MaxCompositeQueries)
        {
            throw new ArgumentOutOfRangeException(nameof(queries), $"1 to {MaxCompositeQueries} queries per request");
        }

        // One Composite call carries up to 5 queries: 5 reads for the price of 1 API call. allOrNone=false so one failed
        // query doesn't discard the other four answers (each subresponse carries its own status)
        var payload = new
        {
            allOrNone = false,
            compositeRequest = queries.Select(q => new
            {
                method = "GET",
                // Checked here, before anything is sent: an over-long URL is rejected by Salesforce (414/431) but the
                // rejected call is still counted against the budget
                url = EnsureFits(QueryUrl(q.Soql)),
                referenceId = q.ReferenceId
            }).ToList() // checked before anything is sent
        };

        var subresponses = await PostCompositeAsync(payload, ct);

        var results = new Dictionary<string, List<JsonElement>>(StringComparer.Ordinal);
        foreach (var sub in subresponses)
        {
            if (sub.HttpStatusCode >= 400)
            {
                throw new SalesforceQueryException(sub.ReferenceId, SalesforceResponseParser.ParseErrors(sub.ReferenceId, sub.Body));
            }

            var page = sub.Body.Deserialize<QueryPage>(Json)
                       ?? throw new InvalidOperationException($"Empty query result {sub.ReferenceId}");
            var records = page.Records;
            // Lookups by unique external id return at most one row per id, far below a query page (2,000), so this loop
            // normally never runs a second call. It follows nextRecordsUrl anyway: dropping rows silently would be worse
            // than one extra call, and the engine would then treat a record that exists as missing and create a duplicate
            for (var next = page.NextRecordsUrl; next is not null;)
            {
                var more = await http.GetFromJsonAsync<QueryPage>(EnsureFits(next), Json, ct)
                           ?? throw new InvalidOperationException("Empty query page");
                records.AddRange(more.Records);
                next = more.NextRecordsUrl;
            }
            results[sub.ReferenceId] = records;
        }
        return results;
    }

    /// <summary>
    /// Sends sObject Collections upserts and deletes in one API call: a single one as a call of its own (an upsert
    /// <c>PATCH /composite/sobjects/{SObject}/{ExternalIdField}</c>, a delete
    /// <c>DELETE /composite/sobjects?ids=...&amp;allOrNone=false</c>), 2 to 5 as the subrequests of one Composite
    /// request, in the given order. An upserted record is created or updated by its external id, a deleted one is
    /// deleted by its Id. <c>allOrNone: false</c> is sent on the Composite request and on every collection: every
    /// record succeeds or fails on its own; <c>collateSubrequests: false</c> keeps the subrequests in order.
    /// </summary>
    /// <returns>
    /// One result per collection Salesforce answered (HTTP 200 does not mean any record was saved or deleted), each
    /// upserted record's with <c>created</c> set. A single collection that fails as a whole throws; a Composite
    /// subrequest that does answers with its errors instead of per-record results.
    /// </returns>
    /// <exception cref="ArgumentException">Too many collections or records, or a URL longer than <see cref="MaxUrlBytes"/>.</exception>
    public async Task<IReadOnlyList<CollectionResult>> WriteAsync(IReadOnlyList<CollectionRequest> collections, CancellationToken ct)
    {
        if (collections.Count is 0 or > MaxCompositeCollections)
        {
            throw new ArgumentOutOfRangeException(nameof(collections), $"1 to {MaxCompositeCollections} collections per request");
        }

        if (collections.FirstOrDefault(c => c.Count is 0 or > MaxCollectionRecords) is { } tooLarge)
        {
            throw new ArgumentOutOfRangeException(nameof(collections),
                $"{tooLarge.ReferenceId}: {tooLarge.Count} records; a collection takes 1 to {MaxCollectionRecords}");
        }

        // Every URL is checked before anything is sent, so a bad request fails locally and costs no call (a Composite
        // with one over-long subrequest would otherwise spend the call and fail that subrequest)
        var urls = collections.Select(c => EnsureFits(DataPath + c.Path)).ToList();

        // A single collection goes as a call of its own: the Composite wrapper would add nothing, and a failure of the
        // whole call then surfaces as an exception instead of a subresponse. 2 to 5 collections share one Composite
        // call, which is the saving: a batch's parent and child upserts and its delete are one API call, not several.
        if (collections is [var single])
        {
            using var request = new HttpRequestMessage(single.Method, urls[0])
            {
                Content = single.Body is { } content ? JsonContent.Create(content, options: Json) : null
            };
            using var res = await http.SendAsync(request, ct);
            await EnsureSuccessAsync(res, ct);
            var body = await res.Content.ReadFromJsonAsync<JsonElement>(Json, ct);
            return body.ValueKind == JsonValueKind.Array
                ? [new CollectionResult(single.ReferenceId, [.. body.EnumerateArray().Select(SalesforceResponseParser.ParseRecordResult)], [])]
                : throw new InvalidOperationException($"{single.ReferenceId} answered without per-record results");
        }

        var payload = new
        {
            allOrNone = false,
            // Run in the order sent. From API version 49.0 Composite collates (bulkifies) unrelated subrequests by default,
            // and then "the order of execution is not guaranteed (unless there is an explicit dependency)": a Text
            // foreign key is no explicit dependency, yet the org's automation needs the parents' upserts first and the
            // delete last (REST API Developer Guide, "Composite", collateSubrequests)
            collateSubrequests = false,
            compositeRequest = collections.Select((c, i) =>
            {
                // A delete has no body: its ids are in its URL
                var subrequest = new Dictionary<string, object?>
                {
                    ["method"] = c.Method.Method,
                    ["url"] = urls[i],
                    ["referenceId"] = c.ReferenceId
                };
                if (c.Body is { } content)
                {
                    subrequest["body"] = content;
                }

                return subrequest;
            })
        };

        var subresponses = await PostCompositeAsync(payload, ct);

        // A subrequest that failed as a whole answers with an error list instead of one result per record
        return [.. subresponses.Select(sub => sub.HttpStatusCode >= 400 || sub.Body.ValueKind != JsonValueKind.Array
            ? new CollectionResult(sub.ReferenceId, null, SalesforceResponseParser.ParseErrors(sub.ReferenceId, sub.Body))
            : new CollectionResult(sub.ReferenceId, [.. sub.Body.EnumerateArray().Select(SalesforceResponseParser.ParseRecordResult)], []))];
    }

    // The one place a Composite request is posted, for queries and writes alike, so both get the same success check and
    // the same parsing of the answer.
    private async Task<List<SubResponse>> PostCompositeAsync(object payload, CancellationToken ct)
    {
        using var res = await http.PostAsJsonAsync($"{DataPath}/composite", payload, Json, ct);
        await EnsureSuccessAsync(res, ct);
        var body = await res.Content.ReadFromJsonAsync<CompositeBody>(Json, ct)
                   ?? throw new InvalidOperationException("Empty composite response");
        return body.CompositeResponse;
    }

    /// <summary>The URL a query is sent with, as a Composite subrequest or a call of its own.</summary>
    internal string QueryUrl(string soql)
    {
        return QueryUrl(DataPath, soql);
    }

    /// <summary>The URL of a query under <paramref name="dataPath"/> (<see cref="SalesforceOptions.DataPath"/>).</summary>
    internal static string QueryUrl(string dataPath, string soql)
    {
        return $"{dataPath}/query?q={Uri.EscapeDataString(soql)}";
    }

    /// <summary>
    /// Whether a URL is within <see cref="MaxUrlBytes"/>, counted in bytes as sent (UTF-8; an escaped query is ASCII,
    /// so its bytes are its characters).
    /// </summary>
    internal static bool FitsUrlLimit(string url)
    {
        return Encoding.UTF8.GetByteCount(url) <= MaxUrlBytes;
    }

    private static string EnsureFits(string url)
    {
        return FitsUrlLimit(url)
            ? url
            : throw new ArgumentException($"The URL is {Encoding.UTF8.GetByteCount(url):N0} bytes; a call allows {MaxUrlBytes:N0} " +
                $"({MaxUriAndHeadersBytes:N0} for the URI and headers together): {url[..Math.Min(url.Length, 200)]}...", nameof(url));
    }

    internal static string Soql(string sobject, IEnumerable<string> fields, string whereField, IEnumerable<string> values)
    {
        return $"SELECT {string.Join(",", fields)} FROM {sobject} WHERE {whereField} IN " +
               $"({string.Join(",", values.Select(SoqlLiteral))})";
    }

    /// <summary>A value as a SOQL string literal: quoted, with backslashes and quotes escaped.</summary>
    internal static string SoqlLiteral(string value)
    {
        return $"'{value.Replace("\\", "\\\\").Replace("'", "\\'")}'";
    }

    private static async Task EnsureSuccessAsync(HttpResponseMessage res, CancellationToken ct)
    {
        if (res.IsSuccessStatusCode)
        {
            return;
        }

        throw new SalesforceResponseException(res.StatusCode, await res.Content.ReadAsStringAsync(ct));
    }

    // ---------- DTOs ----------

    private sealed record DescribeBody(string Name, List<SalesforceField> Fields, List<SalesforceRecordType>? RecordTypeInfos);
    private sealed record CompositeBody(List<SubResponse> CompositeResponse);
    private sealed record QueryPage(List<JsonElement> Records, string? NextRecordsUrl);
    private sealed record SubResponse(string ReferenceId, int HttpStatusCode, JsonElement Body);
}
