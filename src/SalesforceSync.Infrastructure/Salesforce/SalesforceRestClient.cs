using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Options;

namespace SalesforceSync.Infrastructure.Salesforce;

/// <summary>
/// Salesforce REST transport; only class that talks to Salesforce. Each method = exactly one API call: many records
/// in, each one's raw result out. Decides nothing (what to write, what a result means): type's job (<c>SalesforceCalls</c>
/// and the type's sync).
/// <para>
/// No single-record call (get, update, delete, upsert by id) on purpose: one API call per record, and all
/// integrations share org's daily API allocation. Read ≤ 2,000 external ids, upsert/delete ≤ 200 records. 200 records
/// one by one = 200 calls vs 1 for <see cref="UpsertAsync"/>.
/// </para>
/// </summary>
public sealed class SalesforceRestClient : ISalesforceRestClient
{
    private readonly HttpClient http;

    // API path + version from config (Salesforce:BaseApiPath, Salesforce:ApiVersion), not constants: new API version =
    // settings change. One constructor: typed HttpClient DI has nothing to choose (two made it throw at pod start).
    public SalesforceRestClient(HttpClient http, IOptions<SalesforceOptions> options)
    {
        this.http = http;
        DataPath = options.Value.DataPath;
    }

    /// <summary>Path prefix of every call (<see cref="SalesforceOptions.DataPath"/>).</summary>
    public string DataPath { get; }

    // Limits below are Salesforce's call-saving ceilings: engine packs calls up to them; client refuses anything over
    // before sending (rejected call still counts).

    /// <summary>
    /// sObject Collections: max records per create/update/upsert; ceiling of a row type's
    /// <c>IInboxRow.MaxRecordsPerWrite</c>.
    /// </summary>
    public const int MaxCollectionRecords = 200;

    /// <summary>
    /// Max external ids per query for records to fit one page: page ≤ 2,000 records, each external id matches ≤ 1
    /// record. Ceiling of a row type's <c>IInboxRow.MaxValuesPerQuery</c>.
    /// </summary>
    public const int MaxQueryValues = 2_000;

    /// <summary>
    /// REST call URI + headers ≤ 16,384 bytes: over → 414 URI Too Long (URI alone) or 431 Request Header Fields Too
    /// Large (URI + headers). Salesforce Developer Limits and Allocations Quick Reference, "API Request Limits and Allocations"
    /// (https://developer.salesforce.com/docs/platform/salesforce-app-limits-cheatsheet/guide/salesforce-app-limits-platform-api.html);
    /// REST API Developer Guide, "Status Codes and Error Responses"
    /// (https://developer.salesforce.com/docs/platform/api-rest/guide/errorcodes.html).
    /// </summary>
    public const int MaxUriAndHeadersBytes = 16_384;

    /// <summary>
    /// Part of <see cref="MaxUriAndHeadersBytes"/> reserved for this client's headers: Authorization (Bearer token, ~100
    /// bytes opaque, JWT-based a few times that), Host, User-Agent, Accept-Encoding, Content-Type, Content-Length,
    /// Content-Encoding, framing. Well under 1 KB; 4 KB leaves room for a long token or proxy additions.
    /// </summary>
    public const int HeaderAllowanceBytes = 4_096;

    /// <summary>Longest URL (path + query) a call is sent with.</summary>
    public const int MaxUrlBytes = MaxUriAndHeadersBytes - HeaderAllowanceBytes;

    private static readonly JsonSerializerOptions Json = new() { PropertyNameCaseInsensitive = true };

    /// <summary>Runs one SOQL query: all rows.</summary>
    /// <exception cref="ArgumentException">Query URL longer than <see cref="MaxUrlBytes"/>.</exception>
    public Task<List<JsonElement>> SelectAsync(string soql, CancellationToken ct)
    {
        return SelectAsync<JsonElement>(soql, ct);
    }

    /// <summary>Runs one SOQL query: all rows, each read into a <typeparamref name="TRow"/>.</summary>
    /// <exception cref="ArgumentException">Query URL longer than <see cref="MaxUrlBytes"/>.</exception>
    public async Task<List<TRow>> SelectAsync<TRow>(string soql, CancellationToken ct)
    {
        // Checked before sending: Salesforce rejects over-long URL (414/431) but the call still counts against allocation
        var page = await GetPageAsync<TRow>(EnsureFits(QueryUrl(soql)), ct);
        var rows = page.Records;

        // Unique external id lookups → ≤ 1 row per id, far below page size (2,000): loop normally never makes a second
        // call. Follows nextRecordsUrl anyway: silently dropped rows are worse than an extra call (engine would treat an
        // existing record as missing → duplicate)
        for (var next = page.NextRecordsUrl; next is not null;)
        {
            var more = await GetPageAsync<TRow>(EnsureFits(next), ct);
            rows.AddRange(more.Records);
            next = more.NextRecordsUrl;
        }
        return rows;
    }

    private async Task<QueryPage<TRow>> GetPageAsync<TRow>(string url, CancellationToken ct)
    {
        using var res = await http.GetAsync(url, ct);
        await EnsureSuccessAsync(res, ct);
        return await res.Content.ReadFromJsonAsync<QueryPage<TRow>>(Json, ct) ?? throw new InvalidOperationException("Empty query result");
    }

    /// <summary>
    /// <c>PATCH /composite/sobjects/{object}/{externalIdFieldName}</c>: ≤ 200 records created/updated by external id.
    /// Field NAME in URL once per call; each record carries its own VALUE.
    /// <c>allOrNone: false</c>: each record succeeds/fails alone, so one bad record doesn't force a retry of the other
    /// 199 (and their call).
    /// </summary>
    /// <returns>One result per record (HTTP 200 ≠ any record saved), each with <c>created</c> set.</returns>
    /// <exception cref="ArgumentException">No records, > <see cref="MaxCollectionRecords"/>, or URL longer than <see cref="MaxUrlBytes"/>.</exception>
    public Task<IReadOnlyList<RecordResult>> UpsertAsync(string sobject, string externalIdFieldName, IReadOnlyList<SObjectBody> records,
        CancellationToken ct)
    {
        EnsureRecordCount(records.Count);
        var request = new HttpRequestMessage(HttpMethod.Patch, EnsureFits($"{DataPath}/composite/sobjects/{sobject}/{externalIdFieldName}"))
        {
            Content = JsonContent.Create(new { allOrNone = false, records }, options: Json)
        };
        return SendAsync(request, ct);
    }

    /// <summary>
    /// <c>DELETE /composite/sobjects?ids=...&amp;allOrNone=false</c>: ≤ 200 records, any objects, by Id (no external
    /// ids). Deleted → Recycle Bin. Ids in URL (DELETE has no body); 200 18-char Ids = URL under 4 KB, within
    /// <see cref="MaxUrlBytes"/>.
    /// </summary>
    /// <exception cref="ArgumentException">No Ids, > <see cref="MaxCollectionRecords"/>, or URL longer than <see cref="MaxUrlBytes"/>.</exception>
    public Task<IReadOnlyList<RecordResult>> DeleteAsync(IReadOnlyList<string> ids, CancellationToken ct)
    {
        EnsureRecordCount(ids.Count);
        var url = $"{DataPath}/composite/sobjects?ids={string.Join(",", ids.Select(Uri.EscapeDataString))}&allOrNone=false";
        return SendAsync(new HttpRequestMessage(HttpMethod.Delete, EnsureFits(url)), ct);
    }

    // Call answering with a result per record (both Collections calls)
    private async Task<IReadOnlyList<RecordResult>> SendAsync(HttpRequestMessage request, CancellationToken ct)
    {
        using (request)
        {
            using var res = await http.SendAsync(request, ct);
            await EnsureSuccessAsync(res, ct);

            var body = await res.Content.ReadFromJsonAsync<JsonElement>(Json, ct);
            return body.ValueKind == JsonValueKind.Array
                ? [.. body.EnumerateArray().Select(SalesforceResponseParser.ParseRecordResult)]
                : throw new InvalidOperationException("Salesforce answered without per-record results");
        }
    }

    // Call takes 1-200 records: checked before sending (rejected call still counts)
    private static void EnsureRecordCount(int count)
    {
        if (count is 0 or > MaxCollectionRecords)
        {
            throw new ArgumentOutOfRangeException(nameof(count), $"{count} records; a call takes 1 to {MaxCollectionRecords}");
        }
    }

    /// <summary>URL a query is sent with.</summary>
    internal string QueryUrl(string soql)
    {
        return QueryUrl(DataPath, soql);
    }

    /// <summary>Query URL under <paramref name="dataPath"/> (<see cref="SalesforceOptions.DataPath"/>).</summary>
    internal static string QueryUrl(string dataPath, string soql)
    {
        return $"{dataPath}/query?q={Uri.EscapeDataString(soql)}";
    }

    /// <summary>
    /// URL within <see cref="MaxUrlBytes"/>, counted in bytes as sent (UTF-8; escaped query is ASCII: bytes = chars).
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

    /// <summary>Value as SOQL string literal: quoted, backslashes and quotes escaped.</summary>
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

    private sealed record QueryPage<TRow>(List<TRow> Records, string? NextRecordsUrl);
}
