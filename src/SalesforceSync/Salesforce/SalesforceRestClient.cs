using System.Net;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;

namespace SalesforceSync.Salesforce;

/// <summary>
/// Salesforce REST transport, and the only class that talks to Salesforce. Every method is exactly one API call: it
/// takes requests and returns the raw results per subrequest and per record. It decides nothing (what to write, what
/// a result means); that is <see cref="Sync.SalesforceBatchSyncService"/>'s.
/// </summary>
public sealed class SalesforceRestClient(HttpClient http)
{
    public const string ApiVersion = "v62.0";
    public const string DataPath = $"/services/data/{ApiVersion}";

    /// <summary>Composite: at most 5 of its (25) subrequests may be queries or sObject Collections operations.</summary>
    public const int MaxCompositeQueries = 5;
    public const int MaxCompositeCollections = 5;

    /// <summary>
    /// sObject Collections: max records in one create, update or upsert; the ceiling of a row type's
    /// <see cref="Inbox.IInboxRow.MaxRecordsPerWrite"/>.
    /// </summary>
    public const int MaxCollectionRecords = 200;

    /// <summary>
    /// External ids per query, at most, for its records to come back in one page: a query returns up to 2,000 records
    /// per page and each external id matches at most one record. The ceiling of a row type's
    /// <see cref="Inbox.IInboxRow.MaxValuesPerQuery"/>.
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

        var payload = new
        {
            allOrNone = false,
            compositeRequest = queries.Select(q => new
            {
                method = "GET",
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
                throw new SalesforceQueryException(sub.ReferenceId, ParseErrors(sub.ReferenceId, sub.Body));
            }

            var page = sub.Body.Deserialize<QueryPage>(Json)
                       ?? throw new InvalidOperationException($"Empty query result {sub.ReferenceId}");
            var records = page.Records;
            // Lookups by unique external id return at most one row per id, far below a query page (2,000), but
            // follow nextRecordsUrl anyway rather than silently dropping rows
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
    /// Sends sObject Collections upserts in one API call: a single one as a call of its own
    /// (<c>PATCH /composite/sobjects/{SObject}/{ExternalIdField}</c>), 2 to 5 as the subrequests of one Composite
    /// request, in the given order, where a later one may refer to an earlier one's results
    /// (<c>@{referenceId[i].id}</c>). Each record is created or updated by its external id. <c>allOrNone: false</c> is
    /// sent on the Composite request and on every upsert: every record succeeds or fails on its own.
    /// </summary>
    /// <returns>
    /// One result per upsert Salesforce answered (HTTP 200 does not mean any record was saved), each record's with
    /// <c>created</c> set. A single upsert that fails as a whole throws; a Composite subrequest that does answers with
    /// its errors instead of per-record results.
    /// </returns>
    public async Task<IReadOnlyList<CollectionResult>> UpsertAsync(IReadOnlyList<CollectionRequest> collections, CancellationToken ct)
    {
        if (collections.Count is 0 or > MaxCompositeCollections)
        {
            throw new ArgumentOutOfRangeException(nameof(collections), $"1 to {MaxCompositeCollections} collections per request");
        }

        if (collections.FirstOrDefault(c => c.Records.Count is 0 or > MaxCollectionRecords) is { } tooLarge)
        {
            throw new ArgumentOutOfRangeException(nameof(collections),
                $"{tooLarge.ReferenceId}: {tooLarge.Records.Count} records; a collection takes 1 to {MaxCollectionRecords}");
        }

        if (collections is [var single])
        {
            using var res = await http.PatchAsJsonAsync(single.Url, new { allOrNone = false, records = single.Records }, Json, ct);
            await EnsureSuccessAsync(res, ct);
            var body = await res.Content.ReadFromJsonAsync<JsonElement>(Json, ct);
            return body.ValueKind == JsonValueKind.Array
                ? [new CollectionResult(single.ReferenceId, [.. body.EnumerateArray().Select(ParseRecordResult)], [])]
                : throw new InvalidOperationException($"Upsert of {single.SObject} answered without per-record results");
        }

        var payload = new
        {
            allOrNone = false,
            compositeRequest = collections.Select(c => new
            {
                method = "PATCH",
                url = c.Url,
                referenceId = c.ReferenceId,
                body = new { allOrNone = false, records = c.Records }
            })
        };

        var subresponses = await PostCompositeAsync(payload, ct);

        // A subrequest that failed as a whole answers with an error list instead of one result per record
        return [.. subresponses.Select(sub => sub.HttpStatusCode >= 400 || sub.Body.ValueKind != JsonValueKind.Array
            ? new CollectionResult(sub.ReferenceId, null, ParseErrors(sub.ReferenceId, sub.Body))
            : new CollectionResult(sub.ReferenceId, [.. sub.Body.EnumerateArray().Select(ParseRecordResult)], []))];
    }

    /// <summary>Posts a Composite request and returns its subresponses, in request order.</summary>
    private async Task<List<SubResponse>> PostCompositeAsync(object payload, CancellationToken ct)
    {
        using var res = await http.PostAsJsonAsync($"{DataPath}/composite", payload, Json, ct);
        await EnsureSuccessAsync(res, ct);
        var body = await res.Content.ReadFromJsonAsync<CompositeBody>(Json, ct)
                   ?? throw new InvalidOperationException("Empty composite response");
        return body.CompositeResponse;
    }

    /// <summary>The URL a query is sent with, as a Composite subrequest or a call of its own.</summary>
    internal static string QueryUrl(string soql)
    {
        return $"{DataPath}/query?q={Uri.EscapeDataString(soql)}";
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

    private static List<SalesforceError> ParseErrors(string? referenceId, JsonElement body)
    {
        return body.ValueKind != JsonValueKind.Array
            ? [new SalesforceError(referenceId, "UNKNOWN_ERROR", body.ValueKind == JsonValueKind.Undefined ? null : body.GetRawText())]
            : [.. body.EnumerateArray().Select(e => ParseError(referenceId, e, "errorCode"))];
    }

    private static RecordResult ParseRecordResult(JsonElement record)
    {
        return new(
            record.TryGetProperty("success", out var ok) && ok.ValueKind == JsonValueKind.True,
            record.TryGetProperty("id", out var id) && id.ValueKind == JsonValueKind.String ? id.GetString() : null,
            record.TryGetProperty("errors", out var errors) && errors.ValueKind == JsonValueKind.Array
                ? errors.EnumerateArray().Select(e => ParseError(null, e, "statusCode")).ToList()
                : [],
            record.TryGetProperty("created", out var created) && created.ValueKind is JsonValueKind.True or JsonValueKind.False
                ? created.GetBoolean()
                : null);
    }

    /// <param name="codeProperty">"errorCode" in an error response, "statusCode" in a collection's per-record errors.</param>
    /// <param name="referenceId">The subrequest the error belongs to, null for a call of its own.</param>
    /// <param name="e">One error object of the response.</param>
    private static SalesforceError ParseError(string? referenceId, JsonElement e, string codeProperty)
    {
        return new(
            referenceId,
            StringProperty(e, codeProperty) ?? "UNKNOWN_ERROR",
            StringProperty(e, "message"),
            e.ValueKind == JsonValueKind.Object && e.TryGetProperty("fields", out var f) && f.ValueKind == JsonValueKind.Array
                ? f.EnumerateArray().Where(x => x.ValueKind == JsonValueKind.String).Select(x => x.GetString()!).ToList()
                : null);
    }

    private static string? StringProperty(JsonElement e, string name)
    {
        return e.ValueKind == JsonValueKind.Object && e.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.String
                ? value.GetString()
                : null;
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

/// <summary>A SOQL query to run as one subrequest of a Composite request.</summary>
public sealed record CompositeQuery(string ReferenceId, string Soql);

/// <summary>
/// An sObject Collections upsert: up to 200 records of <paramref name="SObject"/>, each
/// <c>{ "attributes": { "type": ... }, externalIdField: ..., field: value, ... }</c>, created or updated by
/// <paramref name="ExternalIdField"/>. A lookup can be set by the parent's external id through the field's
/// relationship, <c>"Account": { "ExternalId__c": "A1" }</c>.
/// </summary>
public sealed record CollectionRequest(string ReferenceId, string SObject, string ExternalIdField,
    IReadOnlyList<IReadOnlyDictionary<string, object?>> Records)
{
    public string Url => $"{SalesforceRestClient.DataPath}/composite/sobjects/{SObject}/{ExternalIdField}";
}

/// <summary>One record's result within a collection, in request order.</summary>
/// <param name="Created">Upserts only: whether the record was created (true) or updated (false).</param>
/// <param name="Success">Whether the record was saved.</param>
/// <param name="Id">The record's Id when it was saved.</param>
/// <param name="Errors">Why it wasn't saved; empty when it was.</param>
public sealed record RecordResult(bool Success, string? Id, IReadOnlyList<SalesforceError> Errors, bool? Created = null);

/// <param name="Records">Null when the subrequest failed as a whole (see <paramref name="Errors"/>).</param>
/// <param name="ReferenceId">The collection's reference id in the request.</param>
/// <param name="Errors">Why the subrequest failed as a whole; empty when it answered per record.</param>
public sealed record CollectionResult(string ReferenceId, IReadOnlyList<RecordResult>? Records, IReadOnlyList<SalesforceError> Errors);