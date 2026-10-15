using System.Net;
using System.Net.Http.Json;
using System.Text.Json;

namespace SalesforceSync.Salesforce;

/// <summary>
/// Salesforce REST transport, and the only class that talks to Salesforce. Every method is exactly one API call: it
/// takes requests and returns the raw results per subrequest and per record. It decides nothing (what to write, what
/// a result means); that is <see cref="Sync.SalesforceBatchUpsertService"/>'s.
/// </summary>
public sealed class SalesforceRestClient(HttpClient http)
{
    public const string ApiVersion = "v62.0";
    public const string DataPath = $"/services/data/{ApiVersion}";

    /// <summary>Composite: at most 5 of its (25) subrequests may be queries or sObject Collections operations.</summary>
    public const int MaxCompositeQueries = 5;
    public const int MaxCompositeCollections = 5;

    /// <summary>sObject Collections: max records in one create or update.</summary>
    public const int MaxCollectionRecords = 200;

    private static readonly JsonSerializerOptions Json = new() { PropertyNameCaseInsensitive = true };

    /// <summary>
    /// sObject Describe: the fields and the record types (<c>recordTypeInfos</c>); with
    /// <paramref name="ifModifiedSince"/>, returns a null schema on 304 Not Modified.
    /// </summary>
    public async Task<DescribeResult> DescribeAsync(string sobject, DateTimeOffset? ifModifiedSince, CancellationToken ct)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, $"{DataPath}/sobjects/{sobject}/describe");
        if (ifModifiedSince is { } since)
        {
            request.Headers.IfModifiedSince = since;
        }

        using var res = await http.SendAsync(request, ct);
        if (res.StatusCode == HttpStatusCode.NotModified)
        {
            return new DescribeResult(null);
        }

        await EnsureSuccessAsync(res, ct);

        var body = await res.Content.ReadFromJsonAsync<DescribeBody>(Json, ct)
                   ?? throw new InvalidOperationException($"Empty describe response for {sobject}");
        return new DescribeResult(new SObjectSchema(body.Name, body.Fields.Select(f => new SalesforceField(
            f.Name, f.Type, f.Length, f.Precision, f.Scale, f.Createable, f.Updateable, f.Nillable, f.Digits)),
            body.RecordTypeInfos));
    }

    /// <summary>
    /// Runs up to 5 queries in one Composite request (one API call). Results are keyed by reference id;
    /// a failed subrequest throws <see cref="SalesforceQueryException"/>.
    /// </summary>
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
                url = $"{DataPath}/query?q={Uri.EscapeDataString(q.Soql)}",
                referenceId = q.ReferenceId
            })
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
                var more = await http.GetFromJsonAsync<QueryPage>(next, Json, ct)
                           ?? throw new InvalidOperationException("Empty query page");
                records.AddRange(more.Records);
                next = more.NextRecordsUrl;
            }
            results[sub.ReferenceId] = records;
        }
        return results;
    }

    /// <summary>
    /// Sends up to 5 sObject Collections creates/updates as the subrequests of one Composite request (one API call),
    /// in the given order; a later one may refer to an earlier one's results (<c>@{referenceId[i].id}</c>).
    /// <c>allOrNone: false</c> is sent on the composite request and on every collection: every record succeeds or
    /// fails on its own.
    /// </summary>
    /// <returns>One result per subrequest Salesforce answered (HTTP 200 does not mean any record was saved).</returns>
    public async Task<IReadOnlyList<CollectionResult>> SendCollectionsAsync(IReadOnlyList<CollectionRequest> collections,
        CancellationToken ct)
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

        var payload = new
        {
            allOrNone = false,
            compositeRequest = collections.Select(c => new
            {
                method = c.Create ? "POST" : "PATCH",
                url = $"{DataPath}/composite/sobjects",
                referenceId = c.ReferenceId,
                body = new { allOrNone = false, records = c.Records }
            })
        };

        var subresponses = await PostCompositeAsync(payload, ct);

        return [.. subresponses.Select(sub =>
        {
            // A subrequest that failed as a whole answers with an error list instead of one result per record
            return sub.HttpStatusCode >= 400 || sub.Body.ValueKind != JsonValueKind.Array
                ? new CollectionResult(sub.ReferenceId, null, ParseErrors(sub.ReferenceId, sub.Body))
                : new CollectionResult(sub.ReferenceId, [.. sub.Body.EnumerateArray().Select(ParseRecordResult)], []);
        })];
    }

    /// <summary>
    /// sObject Collections upsert: up to 200 records of one object in one call, each created or updated by its
    /// <paramref name="externalIdField"/> (<c>PATCH /composite/sobjects/{SObject}/{ExternalIdField}</c>, each record
    /// <c>{ "attributes": { "type": ... }, externalIdField: ..., field: value, ... }</c>). Not a Composite request: one
    /// call of its own, so no references to other requests' results. <c>allOrNone: false</c>: every record succeeds or
    /// fails on its own. A lookup can be set by the parent's external id through the field's relationship,
    /// <c>"Account": { "External_Id__c": "A1" }</c>.
    /// </summary>
    /// <returns>One result per record, in request order, with <c>created</c> set.</returns>
    public async Task<IReadOnlyList<RecordResult>> UpsertAsync(string sobject, string externalIdField,
        IReadOnlyList<IReadOnlyDictionary<string, object?>> records, CancellationToken ct)
    {
        if (records.Count is 0 or > MaxCollectionRecords)
        {
            throw new ArgumentOutOfRangeException(nameof(records), $"{records.Count} records; an upsert takes 1 to {MaxCollectionRecords}");
        }

        using var res = await http.PatchAsJsonAsync($"{DataPath}/composite/sobjects/{sobject}/{externalIdField}",
            new { allOrNone = false, records }, Json, ct);
        await EnsureSuccessAsync(res, ct);
        var body = await res.Content.ReadFromJsonAsync<JsonElement>(Json, ct);
        return body.ValueKind == JsonValueKind.Array
            ? [.. body.EnumerateArray().Select(ParseRecordResult)]
            : throw new InvalidOperationException($"Upsert of {sobject} answered without per-record results");
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

    public static string Soql(string sobject, IEnumerable<string> fields, string whereField, IEnumerable<string> values)
    {
        return $"SELECT {string.Join(",", fields)} FROM {sobject} WHERE {whereField} IN " +
               $"({string.Join(",", values.Select(v => $"'{EscapeSoql(v)}'"))})";
    }

    private static string EscapeSoql(string s)
    {
        return s.Replace("\\", "\\\\").Replace("'", "\\'");
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
    private static SalesforceError ParseError(string? referenceId, JsonElement e, string codeProperty)
    {
        return new(
        referenceId,
        StringProperty(e, codeProperty) ?? "UNKNOWN_ERROR",
        StringProperty(e, "message"),
        e.ValueKind == JsonValueKind.Object && e.TryGetProperty("fields", out var f) && f.ValueKind == JsonValueKind.Array
            ? f.EnumerateArray().Where(x => x.ValueKind == JsonValueKind.String).Select(x => x.GetString()!).ToList() : null);
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

    private sealed record DescribeBody(string Name, List<DescribeField> Fields, List<SalesforceRecordType>? RecordTypeInfos);
    private sealed record DescribeField(string Name, string Type, int Length, int Precision, int Scale,
        bool Createable, bool Updateable, bool Nillable, int Digits);
    private sealed record CompositeBody(List<SubResponse> CompositeResponse);
    private sealed record QueryPage(List<JsonElement> Records, string? NextRecordsUrl);
    private sealed record SubResponse(string ReferenceId, int HttpStatusCode, JsonElement Body);
}

/// <summary>A SOQL query to run as one subrequest of a Composite request.</summary>
public sealed record CompositeQuery(string ReferenceId, string Soql);

/// <summary>
/// An sObject Collections subrequest: creates (POST) or updates (PATCH) up to 200 records, each
/// <c>{ "attributes": { "type": ... }, field: value, ... }</c>, with its <c>id</c> when updating.
/// </summary>
public sealed record CollectionRequest(string ReferenceId, bool Create, IReadOnlyList<IReadOnlyDictionary<string, object?>> Records);

/// <summary>One record's result within a collection, in request order.</summary>
/// <param name="Created">Upserts only: whether the record was created (true) or updated (false).</param>
public sealed record RecordResult(bool Success, string? Id, IReadOnlyList<SalesforceError> Errors, bool? Created = null);

/// <param name="Records">Null when the subrequest failed as a whole (see <paramref name="Errors"/>).</param>
public sealed record CollectionResult(string ReferenceId, IReadOnlyList<RecordResult>? Records, IReadOnlyList<SalesforceError> Errors);