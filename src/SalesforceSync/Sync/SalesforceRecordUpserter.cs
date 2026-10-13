using System.Text.Json;
using Microsoft.Extensions.Options;
using SalesforceSync.Salesforce;

namespace SalesforceSync.Sync;

/// <summary>
/// The Salesforce half of <see cref="SalesforceBatchUpsertService"/>, for any sObjects: upserts a batch of units in
/// two API calls.
/// <list type="number">
/// <item>One Composite request runs the strategy's queries (chunked, at most 5) to read the current values of every
/// field the units set and the Ids their lookups need.</item>
/// <item>One Composite Graph request writes each unit as one graph (all-or-nothing): new records are created, existing
/// ones get only the fields whose normalized value differs.</item>
/// </list>
/// Values are normalized with the fields' Describe metadata (cached) before they are compared and sent. A value
/// Salesforce would reject (unknown or read-only field, too long, wrong type) fails only its own unit, before any
/// call. Each graph's result is classified per unit: saved, not attempted, transient or permanent failure.
/// </summary>
public sealed class SalesforceRecordUpserter(
    SalesforceRestClient api,
    SalesforceSchemaCache schemas,
    SalesforceErrors errors,
    WriteEchoTracker echoTracker,
    IOptions<SalesforceOptions> options,
    ILogger<SalesforceRecordUpserter> log)
{
    private const int MaxValuesPerQuery = 100;

    /// <param name="units">At most 75 (one graph each), keyed by entity key.</param>
    public async Task<List<UpsertResult>> UpsertAsync(IReadOnlyList<(string Key, UpsertUnit Unit)> units,
        IReadOnlyList<SoqlQuery> queries, CancellationToken ct)
    {
        if (units.Count > SalesforceRestClient.MaxGraphsPerRequest)
            throw new ArgumentOutOfRangeException(nameof(units), $"At most {SalesforceRestClient.MaxGraphsPerRequest} units per call (one graph each)");

        var results = new List<UpsertResult>(units.Count);
        if (units.Count == 0) return results;

        // 1. Field metadata of every object involved (cached per process, revalidated with If-Modified-Since)
        var sobjects = units.SelectMany(u => u.Unit.Records)
            .SelectMany(r => (r.Lookups ?? []).Select(l => l.SObject).Append(r.SObject))
            .Concat(queries.Select(q => q.SObject))
            .Distinct(StringComparer.OrdinalIgnoreCase);
        var schema = new Dictionary<string, SObjectSchema>(StringComparer.OrdinalIgnoreCase);
        foreach (var sobject in sobjects) schema[sobject] = await SchemaAsync(sobject, ct);

        // 2. Normalize; a unit Salesforce would reject fails on its own without reaching the query
        var prepared = new List<PreparedUnit>(units.Count);
        foreach (var (key, unit) in units)
        {
            var problems = new List<SalesforceError>();
            var records = unit.Records.Select(r => Prepare(r, schema[r.SObject], schema, problems)).ToList();
            if (problems.Count > 0) results.Add(new(key, SyncOutcome.PermanentFailure, problems));
            else prepared.Add(new PreparedUnit(key, records));
        }
        if (prepared.Count == 0) return results;

        // 3. Read: one Composite call with the strategy's queries
        var current = await ReadAsync(prepared, queries, schema, ct);

        // 4. Diff + resolve lookups
        var graphs = new Dictionary<string, (GraphRequest Graph, PreparedUnit Unit, List<WriteEcho> Echoes)>(StringComparer.Ordinal);
        for (var i = 0; i < prepared.Count; i++)
        {
            var unit = prepared[i];
            var problems = new List<SalesforceError>();
            var missing = new List<SalesforceError>();
            var echoes = new List<WriteEcho>();
            var nodes = BuildNodes(unit, current, echoes, problems, missing);

            if (problems.Count > 0) results.Add(new(unit.Key, SyncOutcome.PermanentFailure, problems));
            else if (missing.Count > 0) results.Add(new(unit.Key, SyncOutcome.TransientFailure, missing));
            else if (nodes.Count == 0) results.Add(new(unit.Key, SyncOutcome.Unchanged, []));
            else graphs[$"g{i}"] = (new GraphRequest($"g{i}", nodes), unit, echoes);
        }
        if (graphs.Count == 0) return results;

        // 5. Write: one Composite Graph call; each graph reports its own outcome
        var graphResults = await api.SendGraphsAsync(graphs.Values.Select(g => g.Graph).ToList(), ct);
        var written = new List<WriteEcho>();
        foreach (var r in graphResults)
        {
            if (!graphs.Remove(r.GraphId, out var sent)) continue;
            if (r.IsSuccessful)
            {
                written.AddRange(sent.Echoes);
                results.Add(new(sent.Unit.Key, SyncOutcome.Saved, []));
            }
            else
            {
                // Permanent errors first, so the one that decided the outcome is reported
                var ordered = r.Errors.OrderByDescending(e => errors.IsPermanent(e.Code)).ToList();
                results.Add(new(sent.Unit.Key, errors.Classify(ordered), ordered));
            }
        }
        echoTracker.Record(written);

        // A graph missing from the response was never processed
        foreach (var (_, unit, _) in graphs.Values)
            results.Add(new(unit.Key, SyncOutcome.NotAttempted, []));

        return results;
    }

    private Task<SObjectSchema> SchemaAsync(string sobject, CancellationToken ct)
        => schemas.GetAsync(sobject, options.Value.SchemaRefreshInterval,
            (since, token) => api.DescribeAsync(sobject, since, token), ct);

    // ---------- Normalize ----------

    private static PreparedRecord Prepare(SObjectRecord record, SObjectSchema schema,
        IReadOnlyDictionary<string, SObjectSchema> all, List<SalesforceError> problems)
    {
        var fields = new Dictionary<SalesforceField, object?>();
        foreach (var (name, value) in record.Fields)
        {
            if (schema.Find(name) is not { } field)
            {
                problems.Add(new(record.ReferenceId, "INVALID_FIELD", $"No such column '{name}' on {schema.Name}"));
                continue;
            }
            if (FieldValues.TryNormalize(field, value, out var n, out var error)) fields[field] = n;
            else problems.Add(new(record.ReferenceId, error.Code, $"{field.Name}: {error.Message}"));
        }

        var lookups = new List<(SalesforceField Field, SObjectLookup Lookup)>();
        foreach (var lookup in record.Lookups ?? [])
        {
            if (schema.Find(lookup.Field) is { Type: "reference" } field && all[lookup.SObject].Find(lookup.ExternalIdField) is not null)
                lookups.Add((field, lookup));
            else
                problems.Add(new(record.ReferenceId, "INVALID_FIELD",
                    $"{schema.Name}.{lookup.Field} is not a lookup, or {lookup.SObject}.{lookup.ExternalIdField} doesn't exist"));
        }

        if (schema.Find(record.ExternalIdField) is null)
            problems.Add(new(record.ReferenceId, "INVALID_FIELD", $"No such column '{record.ExternalIdField}' on {schema.Name}"));

        return new PreparedRecord(record, schema, fields, lookups);
    }

    // ---------- Read ----------

    private async Task<CurrentRecords> ReadAsync(List<PreparedUnit> units, IReadOnlyList<SoqlQuery> queries,
        IReadOnlyDictionary<string, SObjectSchema> schema, CancellationToken ct)
    {
        var records = units.SelectMany(u => u.Records).ToList();
        var lookups = records.SelectMany(r => r.Lookups.Select(l => l.Lookup)).ToList();

        var composite = new List<CompositeQuery>();
        var objectOf = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var query in queries.Where(q => q.Values.Count > 0))
        {
            var target = schema[query.SObject];
            // Every field the diff compares, the external ids records and lookups are matched by, and the Id
            var fields = records.Where(r => r.Schema == target)
                .SelectMany(r => r.Fields.Keys.Select(f => f.Name)
                    .Concat(r.Lookups.Select(l => l.Field.Name))
                    .Append(r.Record.ExternalIdField))
                .Concat(lookups.Where(l => target.Name.Equals(l.SObject, StringComparison.OrdinalIgnoreCase)).Select(l => l.ExternalIdField))
                .Append(query.WhereField)
                .Append("Id")
                .Select(f => target.Find(f)?.Name ?? f)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();

            foreach (var chunk in query.Values.Distinct(StringComparer.Ordinal).Chunk(MaxValuesPerQuery))
            {
                var referenceId = $"q{composite.Count}";
                objectOf[referenceId] = target.Name;
                composite.Add(new CompositeQuery(referenceId, SalesforceRestClient.Soql(target.Name, fields, query.WhereField, chunk)));
            }
        }

        if (composite.Count > SalesforceRestClient.MaxCompositeQueries)
            throw new InvalidOperationException(
                $"The strategy's queries need {composite.Count} SOQL queries; a Composite request allows {SalesforceRestClient.MaxCompositeQueries}. Use a smaller Sync:BatchSize.");
        if (composite.Count == 0) return new CurrentRecords([]);

        try
        {
            var rows = await api.QueryAsync(composite, ct);
            return new CurrentRecords(rows.SelectMany(q => q.Value.Select(row => (objectOf[q.Key], row.EnumerateObject()
                .Where(p => p.Name != "attributes")
                .ToDictionary(p => p.Name, p => p.Value, StringComparer.OrdinalIgnoreCase)))));
        }
        catch (SalesforceQueryException ex) when (ex.Errors.Any(e => e.Code == "INVALID_FIELD"))
        {
            // The cached describe is out of date (a field was removed or renamed): describe again next time
            foreach (var sobject in schema.Keys) schemas.Invalidate(sobject);
            throw;
        }
    }

    /// <summary>Rows read from Salesforce, indexed by every text field that was selected (external ids, Id).</summary>
    private sealed class CurrentRecords
    {
        private readonly Dictionary<(string SObject, string Field, string Value), Dictionary<string, JsonElement>> _index = new();

        public CurrentRecords(IEnumerable<(string SObject, Dictionary<string, JsonElement> Row)> rows)
        {
            foreach (var (sobject, row) in rows)
            {
                var sobjectKey = sobject.ToLowerInvariant();
                foreach (var (field, value) in row)
                    if (value.ValueKind == JsonValueKind.String)
                        _index.TryAdd((sobjectKey, field.ToLowerInvariant(), value.GetString()!), row);
            }
        }

        public Dictionary<string, JsonElement>? Find(string sobject, string field, string value)
            => _index.GetValueOrDefault((sobject.ToLowerInvariant(), field.ToLowerInvariant(), value));
    }

    // ---------- Diff ----------

    private List<GraphNode> BuildNodes(PreparedUnit unit, CurrentRecords current, List<WriteEcho> echoes,
        List<SalesforceError> problems, List<SalesforceError> missing)
    {
        var nodes = new List<GraphNode>();
        // Records of this unit by (sobject, external id): their Id if they exist, else a reference to the node creating them
        var inUnit = new Dictionary<(string, string), string>();

        foreach (var record in unit.Records)
        {
            var r = record.Record;
            var existing = current.Find(r.SObject, r.ExternalIdField, r.ExternalId);
            var changes = Changes(record, existing, echoes, problems);

            foreach (var (field, lookup) in record.Lookups)
            {
                var targetId = inUnit.GetValueOrDefault((lookup.SObject.ToLowerInvariant(), lookup.ExternalId));
                if (targetId is null && current.Find(lookup.SObject, lookup.ExternalIdField, lookup.ExternalId) is { } target)
                    targetId = FieldValues.To18CharId(target["Id"].GetString()!);
                if (targetId is null)
                {
                    missing.Add(new(r.ReferenceId, SalesforceErrors.DependencyNotSynced,
                        $"{record.Schema.Name}.{field.Name}: {lookup.SObject} {lookup.ExternalIdField}={lookup.ExternalId} is not in Salesforce yet"));
                    continue;
                }

                var linked = existing is not null && existing.TryGetValue(field.Name, out var cur) && cur.ValueKind == JsonValueKind.String
                    ? FieldValues.To18CharId(cur.GetString()!)
                    : null;
                if (linked == targetId) continue;
                if (!(existing is null ? field.Createable : field.Updateable))
                {
                    problems.Add(new(r.ReferenceId, "INVALID_FIELD_FOR_INSERT_UPDATE",
                        $"{record.Schema.Name}.{field.Name} is not {(existing is null ? "createable" : "updateable")}"));
                    continue;
                }
                changes[field.Name] = targetId;
            }

            if (existing is null)
            {
                changes[record.Schema.Find(r.ExternalIdField)!.Name] = r.ExternalId;
                nodes.Add(new GraphNode("POST", $"/sobjects/{record.Schema.Name}", r.ReferenceId, changes));
                inUnit[(r.SObject.ToLowerInvariant(), r.ExternalId)] = $"@{{{r.ReferenceId}.id}}";
            }
            else
            {
                var id = existing["Id"].GetString()!;
                inUnit[(r.SObject.ToLowerInvariant(), r.ExternalId)] = FieldValues.To18CharId(id)!;
                if (changes.Count > 0)
                    nodes.Add(new GraphNode("PATCH", $"/sobjects/{record.Schema.Name}/{id}", r.ReferenceId, changes));
            }
        }
        return nodes;
    }

    /// <summary>
    /// Fields to send: all of them for a new record, otherwise only those whose normalized value differs from
    /// Salesforce. A field Salesforce won't let us set (not createable / updateable) is a problem only if it must
    /// actually be written.
    /// </summary>
    private Dictionary<string, object?> Changes(PreparedRecord record, Dictionary<string, JsonElement>? existing,
        List<WriteEcho> echoes, List<SalesforceError> problems)
    {
        var changes = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (var (field, value) in record.Fields)
        {
            JsonElement now = default;
            if (existing is not null && existing.TryGetValue(field.Name, out now) && FieldValues.AreEqual(field, value, now))
                continue;

            if (!(existing is null ? field.Createable : field.Updateable))
            {
                problems.Add(new(record.Record.ReferenceId, "INVALID_FIELD_FOR_INSERT_UPDATE",
                    $"{record.Schema.Name}.{field.Name} is not {(existing is null ? "createable" : "updateable")}"));
                continue;
            }

            var wire = FieldValues.ToWire(value);
            changes[field.Name] = wire;
            var echo = new WriteEcho(record.Schema.Name, record.Record.ExternalId, field.Name, JsonSerializer.Serialize(wire));
            var stored = now.ValueKind == JsonValueKind.Undefined ? "null" : now.GetRawText();
            if (existing is not null && echoTracker.IsFlapping(echo, stored))
                log.LogWarning(
                    "{Object}.{Field} on {ExternalId} keeps differing after it is written: sent {Sent}, Salesforce stores {Stored}. " +
                    "FieldValues is missing a normalization rule for '{Type}' fields, so every event rewrites this field.",
                    record.Schema.Name, field.Name, record.Record.ExternalId, echo.Value, stored, field.Type);
            echoes.Add(echo);
        }
        return changes;
    }

    private sealed record PreparedRecord(
        SObjectRecord Record,
        SObjectSchema Schema,
        Dictionary<SalesforceField, object?> Fields,
        List<(SalesforceField Field, SObjectLookup Lookup)> Lookups);

    private sealed record PreparedUnit(string Key, List<PreparedRecord> Records);
}
