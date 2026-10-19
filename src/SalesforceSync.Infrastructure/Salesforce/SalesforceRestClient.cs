using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Options;

namespace SalesforceSync.Infrastructure.Salesforce;

/// <summary>
/// Salesforce REST transport, and the only class that talks to Salesforce. Every method is exactly one API call: it
/// takes many records and returns each one's raw result. It decides nothing (what to write, what a result means);
/// that is the type's own (<c>RecordWriter</c> for the usual way).
/// <para>
/// There is deliberately no call for one record (get, update, delete or upsert by id): each costs one API call per
/// record, and every integration shares the org's daily API allocation. A read takes up to 2,000 external ids, an
/// upsert or delete up to 200 records. Writing 200 records one by one would spend 200 calls where
/// <see cref="UpsertAsync"/> spends one.
/// </para>
/// </summary>
public sealed class SalesforceRestClient : ISalesforceRestClient
{
    private readonly HttpClient http;

    // API path and version come from configuration (Salesforce:BaseApiPath, Salesforce:ApiVersion), not a constant, so
    // moving to another API version is a settings change. One constructor, so the typed HttpClient's dependency
    // injection has nothing to choose between (two made it throw when the pods started).
    public SalesforceRestClient(HttpClient http, IOptions<SalesforceOptions> options)
    {
        this.http = http;
        DataPath = options.Value.DataPath;
    }

    /// <summary>The path every call starts with (<see cref="SalesforceOptions.DataPath"/>).</summary>
    public string DataPath { get; }

    // The limits below are Salesforce's. They are call-saving ceilings: the engine packs each call up to them, and the
    // client refuses anything over them before sending (a rejected call is still a call).

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
    /// Accept-Encoding, Content-Type, Content-Length, Content-Encoding, and their framing. They come to well under
    /// 1 KB; 4 KB leaves room for a long token or a proxy's additions.
    /// </summary>
    public const int HeaderAllowanceBytes = 4_096;

    /// <summary>The longest URL (path and query) a call is sent with.</summary>
    public const int MaxUrlBytes = MaxUriAndHeadersBytes - HeaderAllowanceBytes;

    private static readonly JsonSerializerOptions Json = new() { PropertyNameCaseInsensitive = true };

    /// <summary>Runs one SOQL query: all its rows.</summary>
    /// <exception cref="ArgumentException">The query's URL is longer than <see cref="MaxUrlBytes"/>.</exception>
    public Task<List<JsonElement>> SelectAsync(string soql, CancellationToken ct)
    {
        return SelectAsync<JsonElement>(soql, ct);
    }

    /// <summary>Runs one SOQL query: all its rows, each read into a <typeparamref name="TRow"/>.</summary>
    /// <exception cref="ArgumentException">The query's URL is longer than <see cref="MaxUrlBytes"/>.</exception>
    public async Task<List<TRow>> SelectAsync<TRow>(string soql, CancellationToken ct)
    {
        // Checked before anything is sent: Salesforce rejects an over-long URL (414/431), but the rejected call still
        // counts against the allocation
        var page = await GetPageAsync<TRow>(EnsureFits(QueryUrl(soql)), ct);
        var rows = page.Records;

        // Lookups by unique external id return at most one row per id, far below a query page (2,000), so this loop
        // normally never runs a second call. It follows nextRecordsUrl anyway: dropping rows silently would be worse than
        // one extra call, and the engine would then treat a record that exists as missing and create a duplicate
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
    /// <c>PATCH /composite/sobjects/{object}/{externalIdFieldName}</c>: up to 200 records created or updated by external
    /// id. The field NAME is in the URL, once for the call; each record carries its own VALUE of that field.
    /// <c>allOrNone: false</c>: every record succeeds or fails on its own, so one bad record doesn't cost the other 199
    /// (and the call that carried them) a retry.
    /// </summary>
    /// <returns>One result per record (HTTP 200 does not mean any record was saved), each with <c>created</c> set.</returns>
    /// <exception cref="ArgumentException">No records or more than <see cref="MaxCollectionRecords"/>, or a URL longer than <see cref="MaxUrlBytes"/>.</exception>
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
    /// <c>DELETE /composite/sobjects?ids=...&amp;allOrNone=false</c>: up to 200 records, of any objects, by Id (no
    /// external ids). Deleted records go to the Recycle Bin. Ids are in the URL (a DELETE has no body); 200 Ids of 18
    /// characters make a URL under 4 KB, within <see cref="MaxUrlBytes"/>.
    /// </summary>
    /// <exception cref="ArgumentException">No Ids or more than <see cref="MaxCollectionRecords"/>, or a URL longer than <see cref="MaxUrlBytes"/>.</exception>
    public Task<IReadOnlyList<RecordResult>> DeleteAsync(IReadOnlyList<string> ids, CancellationToken ct)
    {
        EnsureRecordCount(ids.Count);
        var url = $"{DataPath}/composite/sobjects?ids={string.Join(",", ids.Select(Uri.EscapeDataString))}&allOrNone=false";
        return SendAsync(new HttpRequestMessage(HttpMethod.Delete, EnsureFits(url)), ct);
    }

    // A call that answers with a result per record, as both Collections calls do
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

    // A call takes 1 to 200 records: checked before sending, as a rejected call is still a call
    private static void EnsureRecordCount(int count)
    {
        if (count is 0 or > MaxCollectionRecords)
        {
            throw new ArgumentOutOfRangeException(nameof(count), $"{count} records; a call takes 1 to {MaxCollectionRecords}");
        }
    }

    /// <summary>The URL a query is sent with.</summary>
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

    private sealed record QueryPage<TRow>(List<TRow> Records, string? NextRecordsUrl);
}
