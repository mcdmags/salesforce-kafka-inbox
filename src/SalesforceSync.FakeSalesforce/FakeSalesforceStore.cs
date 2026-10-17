using System.Text;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using System.Web;
using static SalesforceSync.FakeSalesforce.FakeJson;

namespace SalesforceSync.FakeSalesforce;

/// <summary>
/// In-memory Salesforce org: OAuth tokens, sObject Describe, SOQL "WHERE field IN (...)" queries (in pages of 2,000),
/// and sObject Collections upserts by external id and deletes by Id, as calls of their own or as Composite subrequests
/// (per-record results, allOrNone false or true, references between subrequests), modelled on the REST API Developer
/// Guide (and, for per-record codes, the SOAP API Developer Guide). Fields follow <see cref="FakeSchema"/>: unknown and read-only fields are rejected and
/// values are stored the way Salesforce stores them. Every committed create/update is kept in a per-record write
/// history (stored values), so tests can check what reached the org and in which order. Errors can be injected per
/// record. Thread-safe (single lock), which also mimics Salesforce serializing writes to the same rows.
/// <para>
/// This class is the org's surface and its lock. The parts are their own classes: <see cref="FakeRecords"/> (data and
/// write history), <see cref="FakeSoql"/> (queries), <see cref="FakeCollectionWriter"/> (upserts),
/// <see cref="CompositeReferences"/> (references between subrequests), <see cref="FakeFaults"/> (injected failures)
/// and <see cref="FakeCounters"/> (stats).
/// </para>
/// </summary>
public sealed partial class FakeSalesforceStore
{
    public const string ExternalIdField = "ExternalId__c";

    private const int MaxCompositeSubrequests = 25;
    private const int MaxCompositeCollectionsAndQueries = 5;

    /// <summary>A call's URI and headers together, at most (a Composite subrequest's URL is held to it alone).</summary>
    public const int MaxUriAndHeadersBytes = 16_384;

    private const int MaxRecordedWrites = 200;

    private readonly object _gate = new();
    private readonly FakeRecords _records = new();
    private readonly FakeFaults _faults = new();
    private readonly FakeCounters _counters = new();
    private readonly FakeCollectionWriter _collections;
    private readonly List<JsonObject> _recordedWrites = [];
    private readonly Dictionary<string, (List<JsonObject> Rows, int Offset)> _queryLocators = new(StringComparer.Ordinal);
    private int _longRunningInFlight;
    private int _tokens;
    private long _queryCursors;

    public FakeSalesforceStore(FakeSalesforceOptions options)
    {
        Options = options;
        _collections = new FakeCollectionWriter(_records, _faults, _counters, options);
    }

    public FakeSalesforceOptions Options { get; }

    /// <summary>The access token the org issues first, and again after <see cref="Reset"/>.</summary>
    public const string FirstAccessToken = "fake-token";

    /// <summary>The access token the OAuth endpoint issues and API calls must send (<see cref="FakeSalesforceOptions.RequireAccessToken"/>).</summary>
    public string AccessToken { get; private set; } = FirstAccessToken;

    /// <summary>Ends the session (an admin revoking it, or its timeout): the current token is refused from now on, 401.</summary>
    public void ExpireAccessToken()
    {
        lock (_gate)
        {
            AccessToken = $"{FirstAccessToken}-{++_tokens}";
        }
    }

    /// <summary>When the metadata last changed, for Describe's If-Modified-Since (HTTP dates have second precision).</summary>
    public DateTimeOffset SchemaLastModified { get; } = DateTimeOffset.FromUnixTimeSeconds(DateTimeOffset.UtcNow.ToUnixTimeSeconds());

    // ---------- Limits and stats ----------

    public long CountApiCall()
    {
        return _counters.CountApiCall();
    }

    /// <summary>Takes one of the org's concurrent long-running request slots, or returns null when all are in use.</summary>
    public IDisposable? TryStartLongRunning()
    {
        if (Interlocked.Increment(ref _longRunningInFlight) <= Options.ConcurrentLongRunningLimit)
        {
            return new LongRunningSlot(this);
        }

        Interlocked.Decrement(ref _longRunningInFlight);
        _counters.CountConcurrencyRejection();
        return null;
    }

    private sealed class LongRunningSlot(FakeSalesforceStore store) : IDisposable
    {
        private int _disposed;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                Interlocked.Decrement(ref store._longRunningInFlight);
            }
        }
    }

    public FakeSalesforceStats GetStats()
    {
        lock (_gate)
        {
            return _counters.Snapshot(_records.Count);
        }
    }

    public void Reset()
    {
        lock (_gate)
        {
            _records.Clear();
            _faults.Clear();
            _recordedWrites.Clear();
            _queryLocators.Clear();
            _counters.Reset();
            AccessToken = FirstAccessToken;
        }
    }

    // ---------- Inspection and manipulation by tests ----------

    /// <summary>Returns a copy of the record with the given external id, or null.</summary>
    public JsonObject? FindByExternalId(string sobject, string externalId, string extField = ExternalIdField)
    {
        lock (_gate)
        {
            return _records.Find(sobject, extField, externalId)?.DeepClone().AsObject();
        }
    }

    public IReadOnlyList<JsonObject> All(string sobject)
    {
        lock (_gate)
        {
            return [.. _records.Table(sobject).Values.Select(r => r.DeepClone().AsObject())];
        }
    }

    /// <summary>Field values of every committed create/update of the record, oldest first, or null if the record does not exist.</summary>
    public IReadOnlyList<JsonObject>? History(string sobject, string externalId, string extField = ExternalIdField)
    {
        lock (_gate)
        {
            var id = _records.Find(sobject, extField, externalId)?["Id"];
            return id is null ? null : _records.HistoryOf(id.GetValue<string>()).Select(w => w.DeepClone().AsObject()).ToList();
        }
    }

    /// <summary>
    /// The write requests, oldest first (the last <see cref="MaxRecordedWrites"/>), as received: Composite requests
    /// that contained writes (<c>{ "allOrNone", "compositeRequest": [...] }</c>), upserts as
    /// <c>{ "method": "PATCH", "url", "body": { "allOrNone", "records" } }</c> and deletes as
    /// <c>{ "method": "DELETE", "url" }</c> (the Ids in the URL).
    /// </summary>
    public IReadOnlyList<JsonObject> WriteRequests
    {
        get
        {
            lock (_gate)
            {
                return [.. _recordedWrites.Select(r => r.DeepClone().AsObject())];
            }
        }
    }

    /// <summary>Changes a record outside the sync (a user editing it in Salesforce). Returns false if it does not exist.</summary>
    public bool Edit(string sobject, string externalId, JsonObject fields, string extField = ExternalIdField)
    {
        lock (_gate)
        {
            var record = _records.Find(sobject, extField, externalId);
            if (record is null)
            {
                return false;
            }

            var stored = new JsonObject();
            foreach (var (k, v) in fields)
            {
                var field = FakeSchema.Find(sobject, k) ?? throw new FakeSalesforceException(400, "INVALID_FIELD", k);
                var (value, code, message) = FakeSchema.Store(field, v);
                if (code is not null)
                {
                    throw new FakeSalesforceException(400, code, message!);
                }

                record[field.Name] = value?.DeepClone();
                stored[field.Name] = value;
            }
            record["LastModifiedDate"] = FakeSchema.FormatDateTime(DateTimeOffset.UtcNow);
            _records.HistoryOf(record["Id"]!.GetValue<string>()).Add(stored);
            return true;
        }
    }

    /// <summary>
    /// Makes create/update of the record with this external id fail with <paramref name="code"/> the next
    /// <paramref name="times"/> times (a validation rule, a locked row, ...).
    /// </summary>
    public void InjectFault(string sobject, string externalId, string code, string message, int times)
    {
        lock (_gate)
        {
            _faults.Inject(sobject, externalId, code, message, times);
        }
    }

    /// <summary>
    /// The next <paramref name="times"/> requests that write the record with this external id are applied, but their
    /// response never reaches the client (<see cref="FakeResponseLostException"/>): the timeout or dropped connection
    /// after which a client can't know whether its write went through.
    /// </summary>
    public void LoseResponsesFor(string sobject, string externalId, int times)
    {
        lock (_gate)
        {
            _faults.LoseResponsesFor(sobject, externalId, times);
        }
    }

    // ---------- Describe ----------

    /// <returns>(200, describe) or (304, null) if the metadata hasn't changed since <paramref name="ifModifiedSince"/>.</returns>
    public (int Status, JsonNode? Body) Describe(string sobject, DateTimeOffset? ifModifiedSince)
    {
        if (!FakeSchema.Objects.ContainsKey(sobject))
        {
            throw new FakeSalesforceException(404, "NOT_FOUND", $"The requested resource does not exist: {sobject}");
        }

        lock (_gate)
        {
            _counters.Describes++;
            if (ifModifiedSince >= SchemaLastModified)
            {
                _counters.DescribesNotModified++;
                return (304, null);
            }
        }
        return (200, FakeSchema.Describe(sobject));
    }

    // ---------- Query ----------

    /// <summary>Records per query page, at most (REST API Developer Guide, "Query": up to 2,000 at a time).</summary>
    public const int QueryPageSize = 2_000;

    /// <summary>
    /// A query (<c>GET .../query?q=</c>) or its next page (<c>GET .../query/{locator}</c>, its <c>nextRecordsUrl</c>):
    /// up to <see cref="QueryPageSize"/> rows, <c>totalSize</c> (the whole query's), <c>done</c>, and
    /// <c>nextRecordsUrl</c> while rows are left. Each row has <c>attributes</c> (<c>type</c>, <c>url</c>) and the
    /// fields asked for. Deleted records (in the Recycle Bin) are not returned, as Query filters them (queryAll would
    /// return them; it isn't modelled).
    /// </summary>
    public JsonObject Query(Uri url)
    {
        var match = QueryUrl().Match(url.AbsolutePath);
        var dataPath = match.Groups["base"].Value;
        lock (_gate)
        {
            if (match.Groups["locator"].Success)
            {
                return _queryLocators.Remove(match.Groups["locator"].Value, out var rest)
                    ? Page(dataPath, rest.Rows, rest.Offset)
                    : throw new FakeSalesforceException(400, "INVALID_QUERY_LOCATOR", "invalid query locator");
            }

            var query = FakeSoql.Parse(QueryOf(url));
            _counters.Queries++;
            var rows = new List<JsonObject>();
            foreach (var record in _records.Table(query.SObject).Values)
            {
                var key = Str(record[query.WhereField]);
                if (key is null || !query.Values.Contains(key))
                {
                    continue;
                }

                var row = new JsonObject
                {
                    ["attributes"] = new JsonObject
                    {
                        ["type"] = query.SObject,
                        ["url"] = $"{dataPath}/sobjects/{query.SObject}/{Str(record["Id"])}"
                    }
                };
                foreach (var f in query.Fields)
                {
                    var actual = record.FirstOrDefault(kv => string.Equals(kv.Key, f, StringComparison.OrdinalIgnoreCase));
                    row[actual.Key is null ? f : actual.Key] = actual.Value?.DeepClone();
                }
                rows.Add(row);
            }

            return Page(dataPath, rows, 0);
        }
    }

    /// <summary>The page of <paramref name="rows"/> from <paramref name="offset"/>; the rest is kept under a new locator.</summary>
    private JsonObject Page(string dataPath, List<JsonObject> rows, int offset)
    {
        var page = new JsonObject
        {
            ["totalSize"] = rows.Count,
            ["done"] = offset + QueryPageSize >= rows.Count
        };
        if (offset + QueryPageSize < rows.Count)
        {
            // Like Salesforce's: a cursor id and the offset of the next page's first row
            var locator = $"01gFAKE{++_queryCursors:D8}-{offset + QueryPageSize}";
            _queryLocators[locator] = (rows, offset + QueryPageSize);
            page["nextRecordsUrl"] = $"{dataPath}/query/{locator}";
        }

        page["records"] = new JsonArray([.. rows.Skip(offset).Take(QueryPageSize)]);
        return page;
    }

    /// <summary>Whether a call or subrequest is a query or a query's next page (<c>GET .../query</c>, <c>.../query/{locator}</c>).</summary>
    public static bool IsQuery(string path)
    {
        return QueryUrl().IsMatch(path);
    }

    [GeneratedRegex(@"^(?<base>/services/data/v\d+\.\d+)/query(?:/(?<locator>[^/]+))?/?$", RegexOptions.IgnoreCase)]
    private static partial Regex QueryUrl();

    // ---------- Composite ----------

    /// <summary>
    /// Composite: the subrequests run one after another and the whole request is one API call. Supported subrequests:
    /// SOQL queries (GET .../query), sObject Collections upserts (PATCH .../composite/sobjects/{SObject}/{ExternalIdField},
    /// as <see cref="Upsert"/>) and deletes (DELETE .../composite/sobjects?ids=, as <see cref="Delete"/>). A later subrequest can use an earlier one's result as
    /// <c>@{referenceId.field}</c> or <c>@{referenceId[i].field}</c> (case-sensitive); a reference id may hold only
    /// letters, digits and underscores (from API version 52.0 another fails the whole request, 400 JSON_PARSER_ERROR).
    /// Subrequests always run in the order sent, as with collateSubrequests false: Salesforce may reorder unrelated
    /// subrequests when it is true (the default), which the fake doesn't model. allOrNone true on the Composite request
    /// is refused, not modelled (the sync never sends it); a collection's own allOrNone is.
    /// (REST API Developer Guide, "Composite".)
    /// </summary>
    public JsonObject ExecuteComposite(JsonObject payload)
    {
        var subrequests = payload["compositeRequest"]?.AsArray()
                          ?? throw new FakeSalesforceException(400, "JSON_PARSER_ERROR", "Missing 'compositeRequest'");
        if (subrequests.Count > MaxCompositeSubrequests)
        {
            throw new FakeSalesforceException(400, "LIMIT_EXCEEDED", $"A maximum of {MaxCompositeSubrequests} subrequests is allowed");
        }

        if (subrequests.Count(r => IsQuery(r) || IsCollection(r)) > MaxCompositeCollectionsAndQueries)
        {
            throw new FakeSalesforceException(400, "LIMIT_EXCEEDED",
                $"A maximum of {MaxCompositeCollectionsAndQueries} query and sObject Collections subrequests is allowed");
        }

        if (subrequests.Select(r => r?["referenceId"]?.GetValue<string>() ?? "").FirstOrDefault(id => !ReferenceId().IsMatch(id)) is { } invalid)
        {
            throw new FakeSalesforceException(400, "JSON_PARSER_ERROR", $"Provided referenceId ('{invalid}') must start with a letter or " +
                "a number, and it can contain only letters, numbers and underscores ('_').");
        }

        if (FakeCollectionWriter.IsTrue(payload["allOrNone"]))
        {
            throw new FakeSalesforceException(400, "NOT_SUPPORTED", "The fake does not model a Composite request with allOrNone true");
        }

        var responses = new JsonArray();
        var results = new Dictionary<string, JsonNode?>(StringComparer.Ordinal); // referenceId -> response body
        bool lost;

        lock (_gate)
        {
            _faults.LoseResponse = false;
            _counters.CompositeRequests++;
            if (subrequests.Any(IsCollection))
            {
                RecordWriteRequest(payload);
            }

            foreach (var sub in subrequests)
            {
                var referenceId = sub!["referenceId"]!.GetValue<string>();
                var (status, body) = ExecuteSubrequest(sub.AsObject(), results);
                responses.Add(SubResponse(referenceId, status, body));
                results[referenceId] = body;
            }

            lost = _faults.LoseResponse;
        }

        // Everything above is committed; only the answer goes missing
        return lost ? throw new FakeResponseLostException() : new JsonObject { ["compositeResponse"] = responses };
    }

    [GeneratedRegex("^[A-Za-z0-9][A-Za-z0-9_]*$")]
    private static partial Regex ReferenceId();

    // ---------- sObject Collections upsert ----------

    /// <summary>
    /// sObject Collections upsert (<c>PATCH /composite/sobjects/{SObject}/{ExternalIdField}</c>): one API call of its own,
    /// up to 200 records of that object, each updated if a record has its external id and created otherwise. Answers
    /// <c>[{ "id", "success", "errors", "created" }]</c> in request order.
    /// </summary>
    public JsonArray Upsert(string sobject, string externalIdField, string path, JsonObject payload)
    {
        var field = ExternalIdFieldOf(sobject, externalIdField);

        JsonArray results;
        bool lost;
        lock (_gate)
        {
            _faults.LoseResponse = false;
            _counters.UpsertRequests++;
            RecordWriteRequest(new JsonObject { ["method"] = "PATCH", ["url"] = path, ["body"] = payload.DeepClone() });
            results = _collections.Upsert(sobject, field, payload);
            lost = _faults.LoseResponse;
        }

        return lost ? throw new FakeResponseLostException() : results;
    }

    /// <summary>
    /// sObject Collections delete (<c>DELETE /composite/sobjects?ids=...&amp;allOrNone=false</c>): one API call of its own,
    /// up to 200 records of any objects by Id. Answers 200 <c>[{ "id", "success", "errors" }]</c> in request order,
    /// with allOrNone true or false (REST API Developer Guide, "Delete Records Using sObject Collections").
    /// </summary>
    public JsonArray Delete(Uri url)
    {
        var (ids, allOrNone) = DeleteIdsOf(url);
        JsonArray results;
        bool lost;
        lock (_gate)
        {
            _faults.LoseResponse = false;
            RecordWriteRequest(new JsonObject { ["method"] = "DELETE", ["url"] = url.PathAndQuery });
            results = DeleteLocked(ids, allOrNone);
            lost = _faults.LoseResponse;
        }

        return lost ? throw new FakeResponseLostException() : results;
    }

    /// <summary>Whether a call or subrequest is a Collections delete (<c>DELETE .../composite/sobjects?ids=</c>).</summary>
    public static bool IsDelete(string method, string path)
    {
        return method.Equals("DELETE", StringComparison.OrdinalIgnoreCase)
               && path.TrimEnd('/').EndsWith("/composite/sobjects", StringComparison.OrdinalIgnoreCase);
    }

    private JsonArray DeleteLocked(IReadOnlyList<string> ids, bool allOrNone)
    {
        _counters.DeleteRequests++;
        return _collections.Delete(ids, allOrNone);
    }

    /// <summary>The Ids of a delete (<c>ids</c>, comma-separated) and its <c>allOrNone</c> (false when absent).</summary>
    private static (List<string> Ids, bool AllOrNone) DeleteIdsOf(Uri url)
    {
        var query = HttpUtility.ParseQueryString(url.Query);
        return ([.. (query["ids"] ?? "").Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)],
            FakeCollectionWriter.IsTrue(query["allOrNone"]));
    }

    /// <summary>The SOQL of a query call or subrequest (its <c>q</c> parameter).</summary>
    internal static string QueryOf(Uri url)
    {
        return HttpUtility.ParseQueryString(url.Query)["q"] ?? throw new FakeSalesforceException(400, "MALFORMED_QUERY", "Missing q");
    }

    /// <summary>The object and external id field of an upsert's path (<c>.../composite/sobjects/{SObject}/{ExternalIdField}</c>), or null.</summary>
    public static (string SObject, string ExternalIdField)? UpsertTarget(string path)
    {
        var match = UpsertUrl().Match(path);
        return match.Success ? (match.Groups["obj"].Value, match.Groups["field"].Value) : null;
    }

    [GeneratedRegex(@"/composite/sobjects/(?<obj>\w+)/(?<field>\w+)/?$", RegexOptions.IgnoreCase)]
    private static partial Regex UpsertUrl();

    /// <summary>The external id field's name as the org spells it; an unknown object or field is a 404, as Salesforce answers.</summary>
    private static string ExternalIdFieldOf(string sobject, string externalIdField)
    {
        return !FakeSchema.Objects.ContainsKey(sobject)
            ? throw new FakeSalesforceException(404, "NOT_FOUND", $"The requested resource does not exist: {sobject}")
            : FakeSchema.Find(sobject, externalIdField) is { Type: "string" } field
            ? field.Name
            : throw new FakeSalesforceException(404, "NOT_FOUND",
                $"Provided external ID field does not exist or is not accessible: {externalIdField}");
    }

    private void RecordWriteRequest(JsonObject payload)
    {
        _counters.WriteRequests++;
        _recordedWrites.Add(payload.DeepClone().AsObject());
        if (_recordedWrites.Count > MaxRecordedWrites)
        {
            _recordedWrites.RemoveAt(0);
        }
    }

    private static bool IsQuery(JsonNode? sub)
    {
        return IsQuery(new Uri(new Uri("https://fake"), sub?["url"]?.GetValue<string>() ?? "/").AbsolutePath);
    }

    private static bool IsCollection(JsonNode? sub)
    {
        return (sub?["url"]?.GetValue<string>() ?? "").Contains("/composite/sobjects", StringComparison.OrdinalIgnoreCase);
    }

    private (int Status, JsonNode? Body) ExecuteSubrequest(JsonObject sub, Dictionary<string, JsonNode?> results)
    {
        var method = sub["method"]!.GetValue<string>().ToUpperInvariant();
        var url = new Uri(new Uri("https://fake"), sub["url"]!.GetValue<string>());
        try
        {
            if (Encoding.UTF8.GetByteCount(url.PathAndQuery) > MaxUriAndHeadersBytes)
            {
                throw new FakeSalesforceException(414, "URI_TOO_LONG", $"The URI exceeds the {MaxUriAndHeadersBytes:N0}-byte limit");
            }

            if (method == "GET" && IsQuery(url.AbsolutePath))
            {
                return (200, Query(url));
            }

            if (method == "PATCH" && UpsertTarget(url.AbsolutePath) is (var sobject, var externalIdField))
            {
                var field = ExternalIdFieldOf(sobject, externalIdField);
                var body = sub["body"] as JsonObject ?? throw new FakeSalesforceException(400, "JSON_PARSER_ERROR", "Missing body");
                body = body.DeepClone().AsObject();
                if (CompositeReferences.Resolve(body, results) is { } unresolved)
                {
                    // As the guide shows a reference without a value answered ("Composite", illegal reference ids)
                    return (400, Errors("PROCESSING_HALTED",
                        $"Invalid reference specified. No value for {unresolved} found in {unresolved.Split('.', '[')[0]}."));
                }

                return (200, _collections.Upsert(sobject, field, body));
            }

            return IsDelete(method, url.AbsolutePath) && DeleteIdsOf(url) is var (ids, allOrNone)
                ? (200, DeleteLocked(ids, allOrNone))
                : throw new FakeSalesforceException(400, "NOT_SUPPORTED", $"Fake composite does not support {method} {url.AbsolutePath}");
        }
        catch (FakeSalesforceException ex)
        {
            return (ex.Status, Errors(ex.ErrorCode, ex.Message));
        }
    }
}