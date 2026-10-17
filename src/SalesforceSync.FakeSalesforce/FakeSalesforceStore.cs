using System.Text;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using System.Web;
using static SalesforceSync.FakeSalesforce.FakeJson;

namespace SalesforceSync.FakeSalesforce;

/// <summary>
/// In-memory Salesforce org: sObject Describe, SOQL "WHERE field IN (...)" queries, and sObject Collections upserts
/// by external id, as calls of their own or as Composite subrequests (per-record results with allOrNone false,
/// references between subrequests). Fields follow <see cref="FakeSchema"/>: unknown and read-only fields are rejected and
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
    private int _longRunningInFlight;

    public FakeSalesforceStore(FakeSalesforceOptions options)
    {
        Options = options;
        _collections = new FakeCollectionWriter(_records, _faults, _counters);
    }

    public FakeSalesforceOptions Options { get; }

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
            _counters.Reset();
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
    /// that contained writes (<c>{ "allOrNone", "compositeRequest": [...] }</c>), and upserts as
    /// <c>{ "method": "PATCH", "url", "body": { "allOrNone", "records" } }</c>.
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

    public JsonObject Query(string soql)
    {
        var query = FakeSoql.Parse(soql);

        var rows = new JsonArray();
        lock (_gate)
        {
            _counters.Queries++;
            foreach (var record in _records.Table(query.SObject).Values)
            {
                var key = Str(record[query.WhereField]);
                if (key is null || !query.Values.Contains(key))
                {
                    continue;
                }

                var row = new JsonObject { ["attributes"] = new JsonObject { ["type"] = query.SObject } };
                foreach (var f in query.Fields)
                {
                    var actual = record.FirstOrDefault(kv => string.Equals(kv.Key, f, StringComparison.OrdinalIgnoreCase));
                    row[actual.Key is null ? f : actual.Key] = actual.Value?.DeepClone();
                }
                rows.Add(row);
            }
        }

        return new JsonObject
        {
            ["totalSize"] = rows.Count,
            ["done"] = true,
            ["records"] = rows
        };
    }

    // ---------- Composite ----------

    /// <summary>
    /// Composite: the subrequests run one after another and the whole request is one API call. Supported subrequests:
    /// SOQL queries (GET .../query) and sObject Collections upserts (PATCH .../composite/sobjects/{SObject}/{ExternalIdField},
    /// as <see cref="Upsert"/>). A later subrequest can use an earlier one's result as
    /// <c>@{referenceId.field}</c> or <c>@{referenceId[i].field}</c> (case-sensitive). Every record succeeds or fails
    /// on its own (allOrNone false: the sync never sends true, so that mode is not modelled).
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
            : throw new FakeSalesforceException(404, "NOT_FOUND", $"Provided external ID field does not exist or is not accessible: {externalIdField}");
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
        return (sub?["url"]?.GetValue<string>() ?? "").Contains("/query", StringComparison.OrdinalIgnoreCase);
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

            if (method == "GET" && IsQuery(sub))
            {
                return (200, Query(QueryOf(url)));
            }

            if (method == "PATCH" && UpsertTarget(url.AbsolutePath) is (var sobject, var externalIdField))
            {
                var field = ExternalIdFieldOf(sobject, externalIdField);
                var body = sub["body"] as JsonObject ?? throw new FakeSalesforceException(400, "JSON_PARSER_ERROR", "Missing body");
                body = body.DeepClone().AsObject();
                if (CompositeReferences.Resolve(body, results) is { } unresolved)
                {
                    // Closest to what Composite answers for a reference it can't resolve; verify against a real org
                    return (400, Errors("PROCESSING_HALTED", $"Invalid reference specified. No value for {unresolved} found."));
                }

                return (200, _collections.Upsert(sobject, field, body));
            }

            throw new FakeSalesforceException(400, "NOT_SUPPORTED", $"Fake composite does not support {method} {url.AbsolutePath}");
        }
        catch (FakeSalesforceException ex)
        {
            return (ex.Status, Errors(ex.ErrorCode, ex.Message));
        }
    }
}