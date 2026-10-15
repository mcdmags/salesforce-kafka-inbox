using System.Globalization;
using System.Net;
using System.Net.Http.Json;
using System.Text.Json;

namespace SalesforceSync.Salesforce;

/// <summary>A SOQL query to run as one subrequest of a Composite request.</summary>
public sealed record CompositeQuery(string ReferenceId, string Soql);

/// <summary>One Composite Graph: all of its nodes succeed or none do.</summary>
public sealed record GraphRequest(string GraphId, IReadOnlyList<GraphNode> Nodes);

public sealed record GraphNode(string Method, string Url, string ReferenceId, IReadOnlyDictionary<string, object?> Body);

public sealed record GraphResult(string GraphId, bool IsSuccessful, IReadOnlyList<SalesforceError> Errors);

/// <summary>Salesforce REST transport. Every method is exactly one API call.</summary>
public sealed class SalesforceRestClient(HttpClient http)
{
    public const string ApiVersion = "v62.0";
    public const string DataPath = $"/services/data/{ApiVersion}";

    /// <summary>Composite Graph: max graphs and nodes in one request.</summary>
    public const int MaxGraphsPerRequest = 75;
    public const int MaxNodesPerRequest = 500;

    /// <summary>Composite: max subrequests, of which at most 5 may be queries.</summary>
    public const int MaxCompositeQueries = 5;

    private static readonly JsonSerializerOptions Json = new() { PropertyNameCaseInsensitive = true };

    /// <summary>sObject Describe; with <paramref name="ifModifiedSince"/>, returns a null schema on 304 Not Modified.</summary>
    public async Task<DescribeResult> DescribeAsync(string sobject, DateTimeOffset? ifModifiedSince, CancellationToken ct)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, $"{DataPath}/sobjects/{sobject}/describe");
        if (ifModifiedSince is { } since) request.Headers.IfModifiedSince = since;

        using var res = await http.SendAsync(request, ct);
        if (res.StatusCode == HttpStatusCode.NotModified) return new DescribeResult(null);
        await EnsureSuccessAsync(res, ct);

        var body = await res.Content.ReadFromJsonAsync<DescribeBody>(Json, ct)
                   ?? throw new InvalidOperationException($"Empty describe response for {sobject}");
        return new DescribeResult(new SObjectSchema(body.Name, body.Fields.Select(f => new SalesforceField(
            f.Name, f.Type, f.Length, f.Precision, f.Scale, f.Createable, f.Updateable, f.Nillable))));
    }

    /// <summary>
    /// Runs up to 5 queries in one Composite request (one API call). Results are keyed by reference id;
    /// a failed subrequest throws <see cref="SalesforceQueryException"/>.
    /// </summary>
    public async Task<Dictionary<string, List<JsonElement>>> QueryAsync(IReadOnlyList<CompositeQuery> queries, CancellationToken ct)
    {
        if (queries.Count is 0 or > MaxCompositeQueries)
            throw new ArgumentOutOfRangeException(nameof(queries), $"1 to {MaxCompositeQueries} queries per request");

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

        using var res = await http.PostAsJsonAsync($"{DataPath}/composite", payload, Json, ct);
        await EnsureSuccessAsync(res, ct);
        var body = await res.Content.ReadFromJsonAsync<CompositeBody>(Json, ct)
                   ?? throw new InvalidOperationException("Empty composite response");

        var results = new Dictionary<string, List<JsonElement>>(StringComparer.Ordinal);
        foreach (var sub in body.CompositeResponse)
        {
            if (sub.HttpStatusCode >= 400)
                throw new SalesforceQueryException(sub.ReferenceId, ParseErrors(sub.ReferenceId, sub.Body));

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

    /// <summary>Sends up to 75 graphs / 500 nodes in one Composite Graph request and returns each graph's outcome.</summary>
    public async Task<IReadOnlyList<GraphResult>> SendGraphsAsync(IReadOnlyList<GraphRequest> graphs, CancellationToken ct)
    {
        var nodes = graphs.Sum(g => g.Nodes.Count);
        if (graphs.Count > MaxGraphsPerRequest || nodes > MaxNodesPerRequest)
            throw new ArgumentOutOfRangeException(nameof(graphs),
                $"{graphs.Count} graphs / {nodes} nodes exceed {MaxGraphsPerRequest} graphs / {MaxNodesPerRequest} nodes per request");

        var payload = new
        {
            graphs = graphs.Select(g => new
            {
                graphId = g.GraphId,
                compositeRequest = g.Nodes.Select(n => new
                {
                    method = n.Method,
                    url = DataPath + n.Url,
                    referenceId = n.ReferenceId,
                    body = n.Body
                })
            })
        };

        using var res = await http.PostAsJsonAsync($"{DataPath}/composite/graph", payload, Json, ct);
        await EnsureSuccessAsync(res, ct);
        var body = await res.Content.ReadFromJsonAsync<GraphEnvelope>(Json, ct)
                   ?? throw new InvalidOperationException("Empty composite graph response");

        return body.Graphs.Select(g => new GraphResult(g.GraphId, g.IsSuccessful, g.IsSuccessful ? [] : GraphErrors(g)))
            .ToList();
    }

    /// <summary>
    /// Errors of a failed graph. A failure without any error detail is reported as UNKNOWN_ERROR (transient), never
    /// as an empty list, which would read as "not attempted" and be retried forever without spending a retry.
    /// </summary>
    private static List<SalesforceError> GraphErrors(GraphBodyResult graph)
    {
        var errors = graph.GraphResponse.CompositeResponse
            .Where(s => s.HttpStatusCode >= 400)
            .SelectMany(s => ParseErrors(s.ReferenceId, s.Body))
            .ToList();
        if (errors.Count == 0) errors.Add(new SalesforceError(null, "UNKNOWN_ERROR", $"Graph {graph.GraphId} failed without error details"));
        return errors;
    }

    public static string Soql(string sobject, IEnumerable<string> fields, string whereField, IEnumerable<string> values)
        => $"SELECT {string.Join(",", fields)} FROM {sobject} WHERE {whereField} IN " +
           $"({string.Join(",", values.Select(v => $"'{EscapeSoql(v)}'"))})";

    private static string EscapeSoql(string s) => s.Replace("\\", "\\\\").Replace("'", "\\'");

    private static List<SalesforceError> ParseErrors(string? referenceId, JsonElement body)
    {
        if (body.ValueKind != JsonValueKind.Array)
            return [new SalesforceError(referenceId, "UNKNOWN_ERROR", body.ValueKind == JsonValueKind.Undefined ? null : body.GetRawText())];

        return body.EnumerateArray().Select(e => new SalesforceError(
                referenceId,
                e.TryGetProperty("errorCode", out var c) && c.GetString() is { } code ? code : "UNKNOWN_ERROR",
                e.TryGetProperty("message", out var m) ? m.GetString() : null))
            .ToList();
    }

    private static async Task EnsureSuccessAsync(HttpResponseMessage res, CancellationToken ct)
    {
        if (res.IsSuccessStatusCode) return;
        var body = await res.Content.ReadAsStringAsync(ct);
        throw new HttpRequestException(
            string.Create(CultureInfo.InvariantCulture, $"Salesforce {(int)res.StatusCode}: {body}"), null, res.StatusCode);
    }

    // ---------- DTOs ----------

    private sealed record DescribeBody(string Name, List<DescribeField> Fields);
    private sealed record DescribeField(string Name, string Type, int Length, int Precision, int Scale,
        bool Createable, bool Updateable, bool Nillable);
    private sealed record CompositeBody(List<SubResponse> CompositeResponse);
    private sealed record QueryPage(List<JsonElement> Records, string? NextRecordsUrl);
    private sealed record GraphEnvelope(List<GraphBodyResult> Graphs);
    private sealed record GraphBodyResult(string GraphId, bool IsSuccessful, GraphBody GraphResponse);
    private sealed record GraphBody(List<SubResponse> CompositeResponse);
    private sealed record SubResponse(string ReferenceId, int HttpStatusCode, JsonElement Body);
}

/// <summary>A query subrequest failed, e.g. INVALID_FIELD because a field was deleted after it was described.</summary>
public sealed class SalesforceQueryException(string referenceId, IReadOnlyList<SalesforceError> errors)
    : Exception($"Query {referenceId} failed: {string.Join("; ", errors)}")
{
    public string ReferenceId { get; } = referenceId;
    public IReadOnlyList<SalesforceError> Errors { get; } = errors;
}
