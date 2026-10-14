using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;

namespace SalesforceSync.FakeSalesforce;

/// <summary>
/// In-memory Salesforce org: sObject Describe, SOQL "WHERE field IN (...)" queries, and Composite requests with
/// sObject Collections create/update subrequests (per-record results, allOrNone both ways, references between
/// subrequests). Fields follow <see cref="FakeSchema"/>: unknown and read-only fields are rejected and
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

    private const int MaxRecordedWrites = 200;
    private readonly List<JsonObject> _recordedWrites = [];

    private long _apiCalls, _queries, _writeRequests, _recordsSucceeded, _recordsFailed, _recordsRolledBack, _creates, _updates,
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

    [GeneratedRegex(@"^@\{(?<path>[^}]+)\}$")]
    private static partial Regex Reference();

    [GeneratedRegex(@"(?<name>[A-Za-z0-9_]+)(?:\[(?<index>\d+)\])*")]
    private static partial Regex ReferencePart();

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
            _recordedWrites.Clear();
            _apiCalls = _queries = _writeRequests = _recordsSucceeded = _recordsFailed = _recordsRolledBack = _creates = _updates = 0;
            _describes = _describesNotModified = _compositeRequests = 0;
            Interlocked.Exchange(ref _concurrencyRejections, 0);
        }
    }

    public FakeSalesforceStats GetStats()
    {
        lock (_gate)
        {
            return new FakeSalesforceStats(
                Interlocked.Read(ref _apiCalls), _queries, _writeRequests, _recordsSucceeded, _recordsFailed, _recordsRolledBack,
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
    /// Field values of every committed create/update of the record, oldest first (rolled-back writes excluded),
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

    // ---------- Composite + sObject Collections ----------

    public const int MaxCompositeSubrequests = 25;
    public const int MaxCompositeCollectionsAndQueries = 5;
    public const int MaxCollectionRecords = 200;

    private const string RolledBack = "ALL_OR_NONE_OPERATION_ROLLED_BACK";
    private const string RolledBackMessage = "Record rolled back because not all records were valid and the request was using AllOrNone header";

    /// <summary>
    /// Composite: the subrequests run one after another and the whole request is one API call. Supported subrequests:
    /// SOQL queries (GET .../query) and sObject Collections create (POST .../composite/sobjects) and update (PATCH
    /// .../composite/sobjects, each record with its id). A later subrequest can use an earlier one's result as
    /// <c>@{referenceId.field}</c> or <c>@{referenceId[i].field}</c> (case-sensitive).
    /// <para>
    /// allOrNone: on a collection, false lets each record succeed or fail on its own; true rolls the collection back
    /// if any record fails (the others report ALL_OR_NONE_OPERATION_ROLLED_BACK). On the composite request, true
    /// overrides the subrequests: any failure rolls back everything and halts the subrequests after it.
    /// </para>
    /// </summary>
    public JsonObject ExecuteComposite(JsonObject payload)
    {
        var subrequests = payload["compositeRequest"]?.AsArray()
                          ?? throw new FakeSalesforceException(400, "JSON_PARSER_ERROR", "Missing 'compositeRequest'");
        if (subrequests.Count > MaxCompositeSubrequests)
            throw new FakeSalesforceException(400, "LIMIT_EXCEEDED", $"A maximum of {MaxCompositeSubrequests} subrequests is allowed");
        if (subrequests.Count(r => IsQuery(r) || IsCollection(r)) > MaxCompositeCollectionsAndQueries)
            throw new FakeSalesforceException(400, "LIMIT_EXCEEDED",
                $"A maximum of {MaxCompositeCollectionsAndQueries} query and sObject Collections subrequests is allowed");

        var allOrNone = payload["allOrNone"]?.GetValue<bool>() ?? false;
        var writes = subrequests.Any(IsCollection);
        var responses = new JsonArray();
        var results = new Dictionary<string, JsonNode?>(StringComparer.Ordinal); // referenceId -> response body
        var undo = new List<Action>();
        var failed = false;
        bool lost;

        lock (_gate)
        {
            _loseResponse = false;
            _compositeRequests++;
            if (writes)
            {
                _writeRequests++;
                _recordedWrites.Add(payload.DeepClone().AsObject());
                if (_recordedWrites.Count > MaxRecordedWrites) _recordedWrites.RemoveAt(0);
            }

            foreach (var sub in subrequests)
            {
                var referenceId = sub!["referenceId"]!.GetValue<string>();
                if (allOrNone && failed)
                {
                    responses.Add(SubResponse(referenceId, 400, Errors("PROCESSING_HALTED",
                        "The transaction was rolled back since another operation in the same transaction failed.")));
                    continue;
                }

                var (status, body, anyFailed) = ExecuteSubrequest(sub.AsObject(), results, undo);
                responses.Add(SubResponse(referenceId, status, body));
                results[referenceId] = body;
                failed |= anyFailed;
            }

            if (allOrNone && failed)
            {
                for (var i = undo.Count - 1; i >= 0; i--) undo[i]();
                // Records that had succeeded are reported as rolled back; the ones that failed keep their own error
                foreach (var response in responses)
                    if (response!["httpStatusCode"]!.GetValue<int>() < 400 && response["body"] is JsonArray records)
                        foreach (var record in records.Where(r => r?["success"]?.GetValue<bool>() == true))
                            MarkRolledBack(record!.AsObject());
            }
            lost = _loseResponse && !(allOrNone && failed);
        }

        // Everything above is committed; only the answer goes missing
        if (lost) throw new FakeResponseLostException();
        return new JsonObject { ["compositeResponse"] = responses };
    }

    /// <summary>The composite requests that contained writes, oldest first (the last <see cref="MaxRecordedWrites"/>), as received.</summary>
    public IReadOnlyList<JsonObject> WriteRequests
    {
        get
        {
            lock (_gate) return _recordedWrites.Select(r => r.DeepClone().AsObject()).ToList();
        }
    }

    private static bool IsQuery(JsonNode? sub)
        => (sub?["url"]?.GetValue<string>() ?? "").Contains("/query", StringComparison.OrdinalIgnoreCase);

    private static bool IsCollection(JsonNode? sub)
        => (sub?["url"]?.GetValue<string>() ?? "").TrimEnd('/').EndsWith("/composite/sobjects", StringComparison.OrdinalIgnoreCase);

    private (int Status, JsonNode? Body, bool Failed) ExecuteSubrequest(JsonObject sub, Dictionary<string, JsonNode?> results, List<Action> undo)
    {
        var method = sub["method"]!.GetValue<string>().ToUpperInvariant();
        var url = new Uri(new Uri("https://fake"), sub["url"]!.GetValue<string>());
        try
        {
            if (method == "GET" && IsQuery(sub))
                return (200, Query(System.Web.HttpUtility.ParseQueryString(url.Query)["q"]
                                   ?? throw new FakeSalesforceException(400, "MALFORMED_QUERY", "Missing q")), false);

            if (IsCollection(sub) && method is "POST" or "PATCH")
            {
                var body = sub["body"] as JsonObject ?? throw new FakeSalesforceException(400, "JSON_PARSER_ERROR", "Missing body");
                body = body.DeepClone().AsObject();
                if (ResolveReferences(body, results) is { } unresolved)
                    // Closest to what Composite answers for a reference it can't resolve; verify against a real org
                    return (400, Errors("PROCESSING_HALTED", $"Invalid reference specified. No value for {unresolved} found."), true);
                var records = ExecuteCollection(method == "POST", body, undo);
                return (200, records, records.Any(r => r!["success"]!.GetValue<bool>() == false));
            }

            throw new FakeSalesforceException(400, "NOT_SUPPORTED", $"Fake composite does not support {method} {url.AbsolutePath}");
        }
        catch (FakeSalesforceException ex)
        {
            return (ex.Status, Errors(ex.ErrorCode, ex.Message), true);
        }
    }

    /// <summary>Replaces every "@{ref.path}" string in the body; returns the first reference that has no value, or null.</summary>
    private static string? ResolveReferences(JsonNode node, Dictionary<string, JsonNode?> results)
    {
        switch (node)
        {
            case JsonObject obj:
                foreach (var key in obj.Select(kv => kv.Key).ToList())
                {
                    if (Str(obj[key]) is { } text && Reference().Match(text) is { Success: true } m)
                    {
                        if (Lookup(m.Groups["path"].Value, results) is not { } value) return m.Groups["path"].Value;
                        obj[key] = value.DeepClone();
                    }
                    else if (obj[key] is JsonObject or JsonArray && ResolveReferences(obj[key]!, results) is { } unresolved)
                    {
                        return unresolved;
                    }
                }
                return null;
            case JsonArray array:
                foreach (var item in array)
                    if (item is not null && ResolveReferences(item, results) is { } unresolved) return unresolved;
                return null;
            default:
                return null;
        }
    }

    /// <summary>ref.field, ref[0].field, ref.items[2].Id ...: a failed record has no id, so a reference to it has no value.</summary>
    private static JsonNode? Lookup(string path, Dictionary<string, JsonNode?> results)
    {
        var parts = ReferencePart().Matches(path);
        if (parts.Count == 0 || !results.TryGetValue(parts[0].Groups["name"].Value, out var current)) return null;
        for (var i = 0; i < parts.Count && current is not null; i++)
        {
            if (i > 0) current = current is JsonObject o && o.TryGetPropertyValue(parts[i].Groups["name"].Value, out var next) ? next : null;
            foreach (Capture index in parts[i].Groups["index"].Captures)
                current = current is JsonArray a && int.Parse(index.Value) < a.Count ? a[int.Parse(index.Value)] : null;
        }
        return current;
    }

    /// <summary>sObject Collections create (POST) or update (PATCH): one result per record, in request order.</summary>
    private JsonArray ExecuteCollection(bool create, JsonObject body, List<Action> outerUndo)
    {
        var records = body["records"]?.AsArray() ?? throw new FakeSalesforceException(400, "JSON_PARSER_ERROR", "Missing 'records'");
        if (records.Count > MaxCollectionRecords)
            throw new FakeSalesforceException(400, "LIMIT_EXCEEDED", $"A maximum of {MaxCollectionRecords} records is allowed per request");
        var allOrNone = body["allOrNone"]?.GetValue<bool>() ?? false;

        var results = new JsonArray();
        var undo = new List<Action>();
        foreach (var r in records)
        {
            var record = r!.DeepClone().AsObject();
            var sobject = record["attributes"]?["type"]?.GetValue<string>();
            var id = Str(record["id"]) ?? Str(record["Id"]);
            record.Remove("attributes");
            record.Remove("id");
            record.Remove("Id");

            (int Status, JsonNode? Body) outcome;
            if (sobject is null || !FakeSchema.Objects.ContainsKey(sobject))
                outcome = (400, Errors("INVALID_TYPE", $"sObject type '{sobject}' is not supported."));
            else if (!create && id is null)
                outcome = (400, Errors("MISSING_ARGUMENT", "Id not specified in an update call"));
            else if (create && id is not null)
                outcome = (400, Errors("INVALID_FIELD_FOR_INSERT_UPDATE", "cannot specify Id in an insert call", "Id"));
            else if (ResolveRelationships(sobject, record) is { } missing)
                outcome = (400, missing);
            else
                outcome = create ? Create(sobject, record, undo) : Update(sobject, id!, record, undo);

            var ok = outcome.Status < 400;
            results.Add(new JsonObject
            {
                ["id"] = ok ? (outcome.Body?["id"]?.GetValue<string>() ?? id) : null,
                ["success"] = ok,
                ["errors"] = ok
                    ? new JsonArray()
                    : new JsonArray(outcome.Body!.AsArray().Select(e => (JsonNode)new JsonObject
                    {
                        ["statusCode"] = e!["errorCode"]!.GetValue<string>(),
                        ["message"] = e["message"]?.GetValue<string>(),
                        ["fields"] = e["fields"]?.DeepClone() ?? new JsonArray()
                    }).ToArray())
            });
        }

        var failed = results.Count(r => r!["success"]!.GetValue<bool>() == false);
        if (allOrNone && failed > 0)
        {
            for (var i = undo.Count - 1; i >= 0; i--) undo[i]();
            foreach (var result in results.Where(r => r!["success"]!.GetValue<bool>())) MarkRolledBack(result!.AsObject());
        }
        else
        {
            outerUndo.AddRange(undo);
            _recordsSucceeded += results.Count - failed;
            outerUndo.Add(() => _recordsSucceeded -= results.Count - failed);
        }
        _recordsFailed += failed;
        return results;
    }

    private void MarkRolledBack(JsonObject result)
    {
        result["success"] = false;
        result["id"] = null;
        result["errors"] = new JsonArray(new JsonObject { ["statusCode"] = RolledBack, ["message"] = RolledBackMessage, ["fields"] = new JsonArray() });
        _recordsRolledBack++;
    }

    /// <summary>
    /// A lookup given as a relationship reference, <c>"Account__r": { "External_Id__c": "ACC-1" }</c>, becomes the
    /// target's Id in the lookup field. Returns the error Salesforce gives when no record has that external id.
    /// </summary>
    private JsonNode? ResolveRelationships(string sobject, JsonObject record)
    {
        foreach (var (name, value) in record.ToList())
        {
            if (value is not JsonObject reference) continue;
            var field = FakeSchema.Objects[sobject].FirstOrDefault(f => name.Equals(f.RelationshipName, StringComparison.OrdinalIgnoreCase));
            if (field?.ReferenceTo is null)
                return Errors("INVALID_FIELD", $"No such relationship '{name}' on {sobject}", name);

            var (externalField, externalValue) = reference.Where(kv => kv.Key != "attributes").Select(kv => (kv.Key, Str(kv.Value))).FirstOrDefault();
            var target = externalField is null ? null : Table(field.ReferenceTo).Values.FirstOrDefault(r => Str(r[externalField]) == externalValue);
            if (target is null)
                return Errors("INVALID_FIELD",
                    $"Foreign key external ID: {externalValue} not found for field {externalField} in entity {field.ReferenceTo}");
            record.Remove(name);
            record[field.Name] = target["Id"]!.GetValue<string>();
        }
        return null;
    }

    /// <summary>A lookup must point at a record that exists (and is of the right object).</summary>
    private JsonNode? MissingLookupTarget(string sobject, JsonObject stored)
    {
        foreach (var (name, value) in stored)
        {
            if (Str(value) is not { } id || FakeSchema.Find(sobject, name) is not { Type: "reference", ReferenceTo: { } target }) continue;
            if (!Table(target).ContainsKey(id))
                return Errors("INVALID_CROSS_REFERENCE_KEY", $"invalid cross reference id: {id}", name);
        }
        return null;
    }

    private (int, JsonNode?) Create(string sobject, JsonObject body, List<Action> undo)
    {
        var table = Table(sobject);
        var extId = Str(body[ExternalIdField]);
        if (TakeFault(sobject, extId) is { } fault) return fault;
        var (stored, error) = StoredValues(sobject, body, creating: true);
        if (stored is null) return error;
        if (MissingLookupTarget(sobject, stored) is { } missingTarget) return (400, missingTarget);
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
            return (400, Errors("INVALID_CROSS_REFERENCE_KEY", $"invalid cross reference id: {id}", "Id"));

        body.Remove("Id");
        if (TakeFault(sobject, Str(existing[ExternalIdField])) is { } fault) return fault;
        var (stored, error) = StoredValues(sobject, body, creating: false);
        if (stored is null) return error;
        if (MissingLookupTarget(sobject, stored) is { } missingTarget) return (400, missingTarget);

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

    internal static JsonArray Errors(string code, string message, params string[] fields) =>
        new JsonArray(new JsonObject
        {
            ["errorCode"] = code, ["message"] = message, ["fields"] = new JsonArray(fields.Select(f => (JsonNode)f).ToArray())
        });

    internal static string? Str(JsonNode? node) =>
        node is JsonValue v && v.GetValueKind() == JsonValueKind.String ? v.GetValue<string>() : null;
}
