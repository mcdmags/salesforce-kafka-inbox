using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;

namespace SalesforceSync.FakeSalesforce;

/// <summary>
/// In-memory Salesforce org: sObject Describe, SOQL "WHERE field IN (...)" queries, and Composite Graph execution
/// with per-graph rollback. Fields follow <see cref="FakeSchema"/>: unknown and read-only fields are rejected and
/// values are stored the way Salesforce stores them. Every committed create/update is kept in a per-record write
/// history (stored values), so tests can check what reached the org and in which order. Errors can be injected per
/// record. Thread-safe (single lock), which also mimics Salesforce serializing writes to the same rows.
/// </summary>
public sealed partial class FakeSalesforceStore(FakeSalesforceOptions options)
{
    public const string ExternalIdField = "External_Id__c";

    private readonly object _gate = new();
    private readonly Dictionary<string, Dictionary<string, JsonObject>> _tables = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, List<JsonObject>> _history = new(StringComparer.Ordinal); // record Id -> writes

    private long _idCounter;
    private readonly Dictionary<(string, string), Fault> _faults = new();
    private readonly Dictionary<(string, string), int> _lostResponses = new(); // (sobject, external id) -> times left
    private bool _loseResponse;

    private long _apiCalls, _queries, _graphRequests, _graphsSucceeded, _graphsFailed, _graphsHalted, _creates, _updates,
        _concurrencyRejections, _describes, _describesNotModified, _compositeRequests;
    private int _longRunningInFlight;

    public FakeSalesforceOptions Options { get; } = options;

    /// <summary>When the metadata last changed, for Describe's If-Modified-Since (HTTP dates have second precision).</summary>
    public DateTimeOffset SchemaLastModified { get; } = DateTimeOffset.FromUnixTimeSeconds(DateTimeOffset.UtcNow.ToUnixTimeSeconds());

    private sealed class Fault(string code, string message, int times)
    {
        public string Code { get; } = code;
        public string Message { get; } = message;
        public int Remaining { get; set; } = times;
    }

    [GeneratedRegex(@"^/services/data/v[\d.]+/sobjects/(?<obj>\w+)(?:/(?<id>[^/?]+))?/?$", RegexOptions.IgnoreCase)]
    private static partial Regex SObjectUrl();

    [GeneratedRegex(@"^\s*SELECT\s+(?<fields>.+?)\s+FROM\s+(?<obj>\w+)\s+WHERE\s+(?<field>\w+)\s+IN\s*\((?<values>.*)\)\s*$",
        RegexOptions.IgnoreCase | RegexOptions.Singleline)]
    private static partial Regex Soql();

    [GeneratedRegex(@"'((?:\\.|[^'\\])*)'")]
    private static partial Regex QuotedValue();

    [GeneratedRegex(@"^@\{(?<ref>[^.}]+)\.id\}$")]
    private static partial Regex Reference();

    public long CountApiCall() => Interlocked.Increment(ref _apiCalls);

    /// <summary>Takes one of the org's concurrent long-running request slots, or returns null when all are in use.</summary>
    public IDisposable? TryStartLongRunning()
    {
        if (Interlocked.Increment(ref _longRunningInFlight) <= Options.ConcurrentLongRunningLimit)
            return new LongRunningSlot(this);
        Interlocked.Decrement(ref _longRunningInFlight);
        Interlocked.Increment(ref _concurrencyRejections);
        return null;
    }

    private sealed class LongRunningSlot(FakeSalesforceStore store) : IDisposable
    {
        private int _disposed;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0) Interlocked.Decrement(ref store._longRunningInFlight);
        }
    }

    public void Reset()
    {
        lock (_gate)
        {
            _tables.Clear();
            _history.Clear();
            _faults.Clear();
            _lostResponses.Clear();
            _idCounter = 0;
            _apiCalls = _queries = _graphRequests = _graphsSucceeded = _graphsFailed = _graphsHalted = _creates = _updates = 0;
            _describes = _describesNotModified = _compositeRequests = 0;
            Interlocked.Exchange(ref _concurrencyRejections, 0);
        }
    }

    public FakeSalesforceStats GetStats()
    {
        lock (_gate)
        {
            return new FakeSalesforceStats(
                Interlocked.Read(ref _apiCalls), _queries, _graphRequests, _graphsSucceeded, _graphsFailed, _graphsHalted,
                _creates, _updates, Interlocked.Read(ref _concurrencyRejections), _describes, _describesNotModified,
                _compositeRequests, _tables.Values.Sum(t => t.Count));
        }
    }

    /// <summary>Returns a copy of the record with the given external id, or null.</summary>
    public JsonObject? FindByExternalId(string sobject, string externalId, string extField = ExternalIdField)
    {
        lock (_gate)
        {
            return Table(sobject).Values
                .FirstOrDefault(r => Str(r[extField]) == externalId)
                ?.DeepClone().AsObject();
        }
    }

    /// <summary>
    /// Field values of every committed create/update of the record, oldest first (rolled-back graphs excluded),
    /// or null if the record does not exist.
    /// </summary>
    public IReadOnlyList<JsonObject>? History(string sobject, string externalId, string extField = ExternalIdField)
    {
        lock (_gate)
        {
            var id = Table(sobject).Values.FirstOrDefault(r => Str(r[extField]) == externalId)?["Id"];
            return id is null ? null : _history[id.GetValue<string>()].Select(w => w.DeepClone().AsObject()).ToList();
        }
    }

    /// <summary>Changes a record outside the sync (a user editing it in Salesforce). Returns false if it does not exist.</summary>
    public bool Edit(string sobject, string externalId, JsonObject fields, string extField = ExternalIdField)
    {
        lock (_gate)
        {
            var record = Table(sobject).Values.FirstOrDefault(r => Str(r[extField]) == externalId);
            if (record is null) return false;
            var stored = new JsonObject();
            foreach (var (k, v) in fields)
            {
                var field = FakeSchema.Find(sobject, k) ?? throw new FakeSalesforceException(400, "INVALID_FIELD", k);
                var (value, code, message) = FakeSchema.Store(field, v);
                if (code is not null) throw new FakeSalesforceException(400, code, message!);
                record[field.Name] = value?.DeepClone();
                stored[field.Name] = value;
            }
            record["LastModifiedDate"] = FakeSchema.FormatDateTime(DateTimeOffset.UtcNow);
            _history[record["Id"]!.GetValue<string>()].Add(stored);
            return true;
        }
    }

    /// <summary>
    /// Makes create/update of the record with this external id fail with <paramref name="code"/> the next
    /// <paramref name="times"/> times (a validation rule, a locked row, ...).
    /// </summary>
    public void InjectFault(string sobject, string externalId, string code, string message, int times)
    {
        lock (_gate) _faults[(sobject.ToLowerInvariant(), externalId)] = new Fault(code, message, times);
    }

    /// <summary>
    /// The next <paramref name="times"/> requests that write the record with this external id are applied, but their
    /// response never reaches the client (<see cref="FakeResponseLostException"/>): the timeout or dropped connection
    /// after which a client can't know whether its write went through.
    /// </summary>
    public void LoseResponsesFor(string sobject, string externalId, int times)
    {
        lock (_gate) _lostResponses[(sobject.ToLowerInvariant(), externalId)] = times;
    }

    /// <summary>Called for every record written (under the lock): marks the current request's response as lost if asked to.</summary>
    private void NoteWrite(string sobject, string? externalId)
    {
        if (externalId is null || !_lostResponses.TryGetValue((sobject.ToLowerInvariant(), externalId), out var left) || left <= 0) return;
        _lostResponses[(sobject.ToLowerInvariant(), externalId)] = left - 1;
        _loseResponse = true;
    }

    // ---------- Describe ----------

    /// <returns>(200, describe) or (304, null) if the metadata hasn't changed since <paramref name="ifModifiedSince"/>.</returns>
    public (int Status, JsonNode? Body) Describe(string sobject, DateTimeOffset? ifModifiedSince)
    {
        if (!FakeSchema.Objects.ContainsKey(sobject))
            throw new FakeSalesforceException(404, "NOT_FOUND", $"The requested resource does not exist: {sobject}");

        lock (_gate)
        {
            _describes++;
            if (ifModifiedSince >= SchemaLastModified)
            {
                _describesNotModified++;
                return (304, null);
            }
        }
        return (200, FakeSchema.Describe(sobject));
    }

    public void CountCompositeRequest()
    {
        lock (_gate) _compositeRequests++;
    }

    public IReadOnlyList<JsonObject> All(string sobject)
    {
        lock (_gate)
        {
            return Table(sobject).Values.Select(r => r.DeepClone().AsObject()).ToList();
        }
    }

    // ---------- Query ----------

    public JsonObject Query(string soql)
    {
        var m = Soql().Match(soql);
        if (!m.Success)
            throw new FakeSalesforceException(400, "MALFORMED_QUERY", $"Fake only supports SELECT ... WHERE x IN (...): {soql}");

        var fields = m.Groups["fields"].Value.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        var sobject = m.Groups["obj"].Value;
        var whereField = m.Groups["field"].Value;
        if (!FakeSchema.Objects.ContainsKey(sobject))
            throw new FakeSalesforceException(400, "INVALID_TYPE", $"sObject type '{sobject}' is not supported.");
        foreach (var f in fields.Append(whereField))
            if (FakeSchema.Find(sobject, f) is null)
                throw new FakeSalesforceException(400, "INVALID_FIELD", $"No such column '{f}' on entity '{sobject}'.");
        var values = QuotedValue().Matches(m.Groups["values"].Value)
            .Select(v => v.Groups[1].Value.Replace("\\'", "'").Replace("\\\\", "\\"))
            .ToHashSet(StringComparer.Ordinal);

        var records = new JsonArray();
        lock (_gate)
        {
            _queries++;
            foreach (var r in Table(sobject).Values)
            {
                var key = Str(r[whereField]);
                if (key is null || !values.Contains(key)) continue;

                var row = new JsonObject { ["attributes"] = new JsonObject { ["type"] = sobject } };
                foreach (var f in fields)
                {
                    var actual = r.FirstOrDefault(kv => string.Equals(kv.Key, f, StringComparison.OrdinalIgnoreCase));
                    row[actual.Key is null ? f : actual.Key] = actual.Value?.DeepClone();
                }
                records.Add(row);
            }
        }

        return new JsonObject
        {
            ["totalSize"] = records.Count,
            ["done"] = true,
            ["records"] = records
        };
    }

    // ---------- Composite Graph ----------

    public JsonObject ExecuteGraphs(JsonObject payload)
    {
        var graphs = payload["graphs"]?.AsArray()
                     ?? throw new FakeSalesforceException(400, "JSON_PARSER_ERROR", "Missing 'graphs'");

        if (graphs.Count > 75)
            throw new FakeSalesforceException(400, "LIMIT_EXCEEDED", "A maximum of 75 graphs is allowed per request");

        var nodeCount = graphs.Sum(g => g?["compositeRequest"]?.AsArray().Count ?? 0);
        if (nodeCount > 500)
            throw new FakeSalesforceException(400, "LIMIT_EXCEEDED", "A maximum of 500 nodes is allowed per request");

        var results = new JsonArray();
        var failures = 0;
        bool lost;

        lock (_gate)
        {
            _loseResponse = false;
            _graphRequests++;
            foreach (var g in graphs)
            {
                var graph = g!.AsObject();
                var halted = failures > Options.MaxGraphFailuresPerRequest;
                if (halted) _graphsHalted++;
                var result = ExecuteGraph(graph, halted);
                if (!result["isSuccessful"]!.GetValue<bool>()) failures++;
                results.Add(result);
            }
            lost = _loseResponse;
        }

        // Everything above is committed; only the answer goes missing
        if (lost) throw new FakeResponseLostException();
        return new JsonObject { ["graphs"] = results };
    }

    private JsonObject ExecuteGraph(JsonObject graph, bool halted)
    {
        var graphId = graph["graphId"]!.GetValue<string>();
        var nodes = graph["compositeRequest"]!.AsArray();
        var responses = new JsonArray();
        var undo = new List<Action>();
        var refs = new Dictionary<string, string>(StringComparer.Ordinal);
        var failed = halted;

        foreach (var n in nodes)
        {
            var node = n!.AsObject();
            var refId = node["referenceId"]!.GetValue<string>();

            if (failed)
            {
                responses.Add(SubResponse(refId, 400, Errors("PROCESSING_HALTED", "Graph processing halted")));
                continue;
            }

            var (status, body) = ApplyNode(node, refs, undo);
            responses.Add(SubResponse(refId, status, body));
            if (status >= 400) failed = true;
            else if (body?["id"] is JsonNode id) refs[refId] = id.GetValue<string>();
        }

        if (failed)
        {
            for (var i = undo.Count - 1; i >= 0; i--) undo[i]();
            _graphsFailed++;
        }
        else
        {
            _graphsSucceeded++;
        }

        return new JsonObject
        {
            ["graphId"] = graphId,
            ["graphResponse"] = new JsonObject { ["compositeResponse"] = responses },
            ["isSuccessful"] = !failed
        };
    }

    private (int Status, JsonNode? Body) ApplyNode(JsonObject node, Dictionary<string, string> refs, List<Action> undo)
    {
        var method = node["method"]!.GetValue<string>().ToUpperInvariant();
        var url = node["url"]!.GetValue<string>();
        var body = node["body"] is JsonObject b ? b.DeepClone().AsObject() : new JsonObject();

        // Resolve "@{ref.id}" values
        foreach (var key in body.Select(kv => kv.Key).ToList())
        {
            var s = Str(body[key]);
            if (s is null) continue;
            var rm = Reference().Match(s);
            if (!rm.Success) continue;
            if (!refs.TryGetValue(rm.Groups["ref"].Value, out var resolved))
                return (400, Errors("INVALID_REFERENCE", $"Unresolved reference {s}"));
            body[key] = resolved;
        }

        var m = SObjectUrl().Match(url);
        if (!m.Success) return (404, Errors("NOT_FOUND", $"Fake does not support {url}"));

        var sobject = m.Groups["obj"].Value;
        return (method, m.Groups["id"].Success) switch
        {
            ("POST", false) => Create(sobject, body, undo),
            ("PATCH", true) => Update(sobject, m.Groups["id"].Value, body, undo),
            _ => (405, Errors("METHOD_NOT_ALLOWED", $"{method} {url}"))
        };
    }

    private (int, JsonNode?) Create(string sobject, JsonObject body, List<Action> undo)
    {
        var table = Table(sobject);
        var extId = Str(body[ExternalIdField]);
        if (TakeFault(sobject, extId) is { } fault) return fault;
        var (stored, error) = StoredValues(sobject, body, creating: true);
        if (stored is null) return error;
        if (extId is not null && table.Values.Any(r => Str(r[ExternalIdField]) == extId))
            return (400, Errors("DUPLICATE_VALUE", $"duplicate value found: {ExternalIdField} duplicates value {extId}"));

        NoteWrite(sobject, extId);
        var id = NewId(sobject);
        var record = stored.DeepClone().AsObject();
        record["Id"] = id;
        var now = FakeSchema.FormatDateTime(DateTimeOffset.UtcNow);
        record["CreatedDate"] = now;
        record["LastModifiedDate"] = now;
        table[id] = record;
        _history[id] = [stored];
        _creates++;
        undo.Add(() =>
        {
            table.Remove(id);
            _history.Remove(id);
            _creates--;
        });

        return (201, new JsonObject { ["id"] = id, ["success"] = true, ["errors"] = new JsonArray() });
    }

    private (int, JsonNode?) Update(string sobject, string id, JsonObject body, List<Action> undo)
    {
        var table = Table(sobject);
        if (!table.TryGetValue(id, out var existing))
            return (404, Errors("NOT_FOUND", $"{sobject} {id} not found"));

        body.Remove("Id");
        if (TakeFault(sobject, Str(existing[ExternalIdField])) is { } fault) return fault;
        var (stored, error) = StoredValues(sobject, body, creating: false);
        if (stored is null) return error;

        NoteWrite(sobject, Str(existing[ExternalIdField]));
        var before = existing.DeepClone().AsObject();
        foreach (var (k, v) in stored) existing[k] = v?.DeepClone();
        existing["LastModifiedDate"] = FakeSchema.FormatDateTime(DateTimeOffset.UtcNow);
        var history = _history[id];
        history.Add(stored);
        _updates++;
        undo.Add(() =>
        {
            table[id] = before;
            history.RemoveAt(history.Count - 1);
            _updates--;
        });

        return (204, null);
    }

    // ---------- Field rules ----------

    /// <summary>The values Salesforce would store for the body, or the error it would return.</summary>
    private static (JsonObject? Stored, (int, JsonNode?) Error) StoredValues(string sobject, JsonObject body, bool creating)
    {
        var stored = new JsonObject();
        foreach (var (name, value) in body)
        {
            if (FakeSchema.Find(sobject, name) is not { } field)
                return (null, (400, Errors("INVALID_FIELD", $"No such column '{name}' on sobject of type {sobject}")));
            if (creating ? !field.Createable : !field.Updateable)
                return (null, (400, Errors("INVALID_FIELD_FOR_INSERT_UPDATE",
                    $"Unable to create/update fields: {field.Name}. Please check the security settings of this field.")));

            var (v, code, message) = FakeSchema.Store(field, value);
            if (code is not null) return (null, (400, Errors(code, message!)));
            stored[field.Name] = v;
        }
        return (stored, default);
    }

    private (int, JsonNode?)? TakeFault(string sobject, string? externalId)
    {
        if (externalId is null || !_faults.TryGetValue((sobject.ToLowerInvariant(), externalId), out var fault) || fault.Remaining <= 0)
            return null;
        fault.Remaining--;
        return (400, Errors(fault.Code, fault.Message));
    }

    // ---------- Helpers ----------

    private Dictionary<string, JsonObject> Table(string sobject)
    {
        if (!_tables.TryGetValue(sobject, out var t))
            _tables[sobject] = t = new Dictionary<string, JsonObject>(StringComparer.Ordinal);
        return t;
    }

    private string NewId(string sobject)
    {
        var prefix = sobject.ToLowerInvariant() switch { "account" => "001", "profile__c" => "a00", _ => "a01" };
        return $"{prefix}FAKE{++_idCounter:D11}";
    }

    internal static JsonObject SubResponse(string referenceId, int status, JsonNode? body) => new()
    {
        ["body"] = body,
        ["httpHeaders"] = new JsonObject(),
        ["httpStatusCode"] = status,
        ["referenceId"] = referenceId
    };

    internal static JsonArray Errors(string code, string message) =>
        new JsonArray(new JsonObject { ["errorCode"] = code, ["message"] = message, ["fields"] = new JsonArray() });

    internal static string? Str(JsonNode? node) =>
        node is JsonValue v && v.GetValueKind() == JsonValueKind.String ? v.GetValue<string>() : null;
}
