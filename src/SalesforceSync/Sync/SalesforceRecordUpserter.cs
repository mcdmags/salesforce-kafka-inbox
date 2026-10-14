using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Options;
using SalesforceSync.Inbox;
using SalesforceSync.Inbox.Payloads;
using SalesforceSync.Salesforce;

namespace SalesforceSync.Sync;

/// <summary>
/// The Salesforce half of <see cref="SalesforceBatchUpsertService"/>, for any sObjects: upserts a batch of units in
/// two API calls.
/// <list type="number">
/// <item>One Composite request runs the strategy's queries (chunked, at most 5) to read the current values of every
/// field the units set and the Ids their lookups need.</item>
/// <item>One Composite request writes every record with sObject Collections subrequests, one per object and verb:
/// new records are created (POST), existing ones get only the fields whose normalized value differs (PATCH by Id).
/// Parents go first; a child of a parent created in the same request points at it by reference
/// (<see cref="SalesforceOptions.ParentLink"/>). More than 5 collections are split over further requests, an entity's
/// records always staying together.</item>
/// </list>
/// Whether a record is created or updated is decided by what the read found; the event's type (inserted / updated) is
/// checked against it (<see cref="SyncOptions.EventTypeMismatch"/>). Values are normalized with the fields' Describe
/// metadata (cached) before they are compared and sent. A value Salesforce would reject (unknown or read-only field,
/// too long, wrong type) fails only its own unit, before any call. Salesforce answers per record; a unit is saved
/// only if all of its records are, otherwise it is not attempted, a transient or a permanent failure. If the write
/// call itself fails without proving that nothing was applied, <see cref="UnknownOutcomeException"/> is thrown.
/// </summary>
public sealed partial class SalesforceRecordUpserter(
    SalesforceRestClient api,
    SalesforceSchemaCache schemas,
    SalesforceErrors errors,
    WriteEchoTracker echoTracker,
    IOptions<SalesforceOptions> options,
    IOptions<SyncOptions> syncOptions,
    ILogger<SalesforceRecordUpserter> log)
{
    private const int MaxValuesPerQuery = 100;

    /// <param name="units">At most <see cref="SalesforceRestClient.MaxEntitiesPerBatch"/>, keyed by entity key.</param>
    public async Task<List<UpsertResult>> UpsertAsync(IReadOnlyList<(string Key, UpsertUnit Unit)> units,
        IReadOnlyList<SoqlQuery> queries, CancellationToken ct)
    {
        if (units.Count > SalesforceRestClient.MaxEntitiesPerBatch)
            throw new ArgumentOutOfRangeException(nameof(units), $"At most {SalesforceRestClient.MaxEntitiesPerBatch} units per call");

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
            else prepared.Add(new PreparedUnit(key, records, unit.EventType));
        }
        if (prepared.Count == 0) return results;

        // 3. Read: one Composite call with the strategy's queries
        var current = await ReadAsync(prepared, queries, schema, ct);

        // 4. Diff + resolve lookups: what to create, what to update, what to leave alone
        var planned = new List<PlannedUnit>();
        var mismatches = 0;
        foreach (var unit in prepared)
        {
            if (TypeMismatch(unit, current) is { } mismatch)
            {
                mismatches++;
                if (syncOptions.Value.EventTypeMismatch == EventTypeMismatch.DeadLetter)
                {
                    results.Add(new(unit.Key, SyncOutcome.PermanentFailure, [mismatch]));
                    continue;
                }
                log.LogDebug("{Key}: {Mismatch}; going by Salesforce", unit.Key, mismatch.Message);
            }

            var problems = new List<SalesforceError>();
            var missing = new List<SalesforceError>();
            var ops = Plan(unit, current, schema, problems, missing);

            if (problems.Count > 0) results.Add(new(unit.Key, SyncOutcome.PermanentFailure, problems));
            else if (missing.Count > 0) results.Add(new(unit.Key, SyncOutcome.TransientFailure, missing));
            else if (ops.Count == 0) results.Add(new(unit.Key, SyncOutcome.Unchanged, []));
            else planned.Add(new PlannedUnit(unit.Key, ops));
        }
        if (mismatches > 0)
            log.LogInformation(
                "{Count} of {Total} events had a type that disagrees with Salesforce (inserted but the record exists, or updated but it does not): {Action}",
                mismatches, prepared.Count,
                syncOptions.Value.EventTypeMismatch == EventTypeMismatch.DeadLetter ? "dead-lettered" : "reconciled with Salesforce");

        // 5. Write: normally one Composite call. Each call's records are final once it returns, so a later call that
        // fails outright only costs its own entities a retry (the earlier ones then read back as unchanged).
        var written = new List<WriteEcho>();
        foreach (var call in Pack(planned, results))
        {
            IReadOnlyList<CollectionResult> answers;
            try
            {
                answers = await api.SendCollectionsAsync(call.Requests(), options.Value.AllOrNone, ct);
            }
            catch (Exception ex) when (WriteOutcome.IsUnknown(ex))
            {
                // Everything before this point only read from Salesforce, so its failures are safe to retry. From here
                // on Salesforce may have the write, and may still apply it after we stop waiting.
                throw new UnknownOutcomeException(ex);
            }
            results.AddRange(Outcomes(call, answers, written));
        }
        echoTracker.Record(written);

        return results;
    }

    /// <summary>
    /// The event's type against what Salesforce has for the unit's main record: "inserted" for a record that exists
    /// (a replay, or it was created another way) or "updated" for one that doesn't (its insert was lost or
    /// dead-lettered, or the record was deleted in Salesforce).
    /// </summary>
    private static SalesforceError? TypeMismatch(PreparedUnit unit, CurrentRecords current)
    {
        if (unit.EventType is not { } type) return null;
        var main = unit.Records[0].Record;
        var exists = current.Find(main.SObject, main.ExternalIdField, main.ExternalId) is not null;
        if (exists == (type == EventType.Updated)) return null;
        return new SalesforceError(main.ReferenceId, SalesforceErrors.EventTypeMismatch, exists
            ? $"the event says inserted, but {main.SObject} {main.ExternalIdField}={main.ExternalId} is already in Salesforce"
            : $"the event says updated, but {main.SObject} {main.ExternalIdField}={main.ExternalId} is not in Salesforce");
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

    /// <summary>
    /// The unit's writes: a create for each record Salesforce doesn't have, an update for each one that differs, nothing
    /// for the rest. A lookup becomes the target's Id when the target exists, and a link to the target's create when
    /// the target is an earlier record of this unit that is new.
    /// </summary>
    private List<Op> Plan(PreparedUnit unit, CurrentRecords current, IReadOnlyDictionary<string, SObjectSchema> schema,
        List<SalesforceError> problems, List<SalesforceError> missing)
    {
        var ops = new List<Op>();
        // Records of this unit by (sobject, external id): their Id if they exist, else the op creating them
        var inUnit = new Dictionary<(string, string), (string? Id, Op? Create, int Depth)>();

        foreach (var record in unit.Records)
        {
            var r = record.Record;
            var existing = current.Find(r.SObject, r.ExternalIdField, r.ExternalId);
            var echoes = new List<WriteEcho>();
            var changes = Changes(record, existing, echoes, problems);
            var links = new List<ParentLink>();
            var depth = 0;

            foreach (var (field, lookup) in record.Lookups)
            {
                Op? parent = null;
                string? targetId = null;
                if (inUnit.TryGetValue((lookup.SObject.ToLowerInvariant(), lookup.ExternalId), out var sibling))
                {
                    (targetId, parent) = (sibling.Id, sibling.Create);
                    depth = Math.Max(depth, sibling.Depth + 1);
                }
                else if (current.Find(lookup.SObject, lookup.ExternalIdField, lookup.ExternalId) is { } target)
                {
                    targetId = FieldValues.To18CharId(target["Id"].GetString()!);
                }
                else
                {
                    missing.Add(new(r.ReferenceId, SalesforceErrors.DependencyNotSynced,
                        $"{record.Schema.Name}.{field.Name}: {lookup.SObject} {lookup.ExternalIdField}={lookup.ExternalId} is not in Salesforce yet"));
                    continue;
                }

                var linked = existing is not null && existing.TryGetValue(field.Name, out var cur) && cur.ValueKind == JsonValueKind.String
                    ? FieldValues.To18CharId(cur.GetString()!)
                    : null;
                if (parent is null && linked == targetId) continue;
                if (!(existing is null ? field.Createable : field.Updateable))
                {
                    problems.Add(new(r.ReferenceId, "INVALID_FIELD_FOR_INSERT_UPDATE",
                        $"{record.Schema.Name}.{field.Name} is not {(existing is null ? "createable" : "updateable")}"));
                    continue;
                }

                if (parent is null) changes[field.Name] = targetId;
                else
                    // By external id only if the lookup can be addressed that way; a reference always can
                    links.Add(new ParentLink(field, parent, lookup.ExternalId,
                        options.Value.ParentLink == Salesforce.ParentLink.ExternalId && field.RelationshipName is not null
                            ? schema[lookup.SObject].Find(lookup.ExternalIdField)!.Name
                            : null));
            }

            var lookupFields = record.Lookups.Where(l => changes.ContainsKey(l.Field.Name)).Select(l => l.Field.Name)
                .Concat(links.SelectMany(l => new[] { l.Field.Name, l.Field.RelationshipName ?? l.Field.Name }))
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            Op? op = null;
            if (existing is null)
            {
                changes[record.Schema.Find(r.ExternalIdField)!.Name] = r.ExternalId;
                op = new Op(record, null, changes, links, lookupFields, echoes, depth);
            }
            else if (changes.Count > 0 || links.Count > 0)
            {
                op = new Op(record, existing["Id"].GetString()!, changes, links, lookupFields, echoes, depth);
            }
            if (op is not null) ops.Add(op);

            inUnit[(r.SObject.ToLowerInvariant(), r.ExternalId)] = existing is null
                ? (null, op, depth)
                : (FieldValues.To18CharId(existing["Id"].GetString()!), null, depth);
        }
        return ops;
    }

    // ---------- Pack ----------

    /// <summary>
    /// Distributes the units' writes over Composite requests. Records are grouped into one collection per level,
    /// object and verb; children that point at their parent by reference get a collection of their own, because one
    /// reference without a value (its parent failed) fails that whole subrequest, and children with a literal Id
    /// should not go down with it. A request takes 5 collections of 200 records; a unit that fits none of the requests
    /// so far starts another, so an entity's records (and the references between them) share a request.
    /// </summary>
    private static List<Call> Pack(List<PlannedUnit> units, List<UpsertResult> results)
    {
        var calls = new List<Call>();
        foreach (var unit in units)
        {
            var groups = unit.Ops.GroupBy(o => o.Group).ToDictionary(g => g.Key, g => g.Count());
            if (groups.Count > SalesforceRestClient.MaxCompositeCollections || groups.Values.Any(n => n > SalesforceRestClient.MaxCollectionRecords))
            {
                results.Add(new(unit.Key, SyncOutcome.PermanentFailure, [new SalesforceError(null, "LIMIT_EXCEEDED",
                    $"The unit needs {groups.Count} collections; a Composite request takes {SalesforceRestClient.MaxCompositeCollections} " +
                    $"of {SalesforceRestClient.MaxCollectionRecords} records")]));
                continue;
            }

            // First request with room, so units needing the same collections end up together
            var call = calls.FirstOrDefault(c => c.Fits(groups));
            if (call is null) calls.Add(call = new Call());
            call.Add(unit);
        }
        return calls;
    }

    /// <summary>One Composite request: its units and their ops by collection.</summary>
    private sealed partial class Call
    {
        private readonly Dictionary<Group, List<Op>> _groups = [];

        public List<PlannedUnit> Units { get; } = [];

        /// <summary>The collections in sending order, set by <see cref="Requests"/>.</summary>
        public List<(string ReferenceId, List<Op> Ops)> Collections { get; } = [];

        public bool Fits(Dictionary<Group, int> groups)
            => _groups.Keys.Union(groups.Keys).Count() <= SalesforceRestClient.MaxCompositeCollections
               && groups.All(g => (_groups.GetValueOrDefault(g.Key)?.Count ?? 0) + g.Value <= SalesforceRestClient.MaxCollectionRecords);

        public void Add(PlannedUnit unit)
        {
            Units.Add(unit);
            foreach (var op in unit.Ops)
            {
                if (!_groups.TryGetValue(op.Group, out var ops)) _groups[op.Group] = ops = [];
                ops.Add(op);
            }
        }

        /// <summary>
        /// Parents before children (a reference can only point back), creates before updates. Each record learns its
        /// collection and position there, which is how its children refer to it.
        /// </summary>
        public List<CollectionRequest> Requests()
        {
            var requests = new List<CollectionRequest>();
            foreach (var (group, ops) in _groups.OrderBy(g => g.Key.Depth).ThenByDescending(g => g.Key.Create)
                         .ThenBy(g => g.Key.SObject, StringComparer.Ordinal).ThenBy(g => g.Key.ByReference))
            {
                // Reference ids take letters, digits and underscores only
                var referenceId = NotReferenceIdCharacter().Replace(
                    $"{group.SObject}_{(group.Create ? "create" : "update")}{(group.ByReference ? "_ref" : "")}", "_");
                if (Collections.Any(c => c.ReferenceId == referenceId)) referenceId += $"_{group.Depth}";

                for (var i = 0; i < ops.Count; i++) (ops[i].Collection, ops[i].Index) = (referenceId, i);
                Collections.Add((referenceId, ops));
                requests.Add(new CollectionRequest(referenceId, group.Create, ops.Select(o => o.Body()).ToList()));
            }
            return requests;
        }

        [GeneratedRegex("[^A-Za-z0-9_]")]
        private static partial Regex NotReferenceIdCharacter();
    }

    // ---------- Outcomes ----------

    private enum OpKind
    {
        Saved,
        /// <summary>Not saved because of a record of another unit (rolled back, or its subrequest went down).</summary>
        NotAttempted,
        /// <summary>Not saved because its parent in the same unit wasn't; the parent's result says what to do.</summary>
        ParentNotSaved,
        Transient,
        Permanent
    }

    /// <summary>
    /// Turns Salesforce's per-record results into one outcome per unit: saved only if every record is; permanent if
    /// any record's own error is; else transient if any is; else not attempted.
    /// </summary>
    private List<UpsertResult> Outcomes(Call call, IReadOnlyList<CollectionResult> answers, List<WriteEcho> written)
    {
        // Parents were sent first, so a child is classified after its parent
        foreach (var (referenceId, ops) in call.Collections)
        {
            var answer = answers.FirstOrDefault(a => a.ReferenceId == referenceId);
            var wholeError = answer switch
            {
                null => [],   // not in the response: never processed
                { Records: null } => answer.Errors,
                { Records: var records } when records.Count != ops.Count =>
                    [new SalesforceError(referenceId, "UNKNOWN_ERROR", $"{records.Count} results for {ops.Count} records")],
                _ => (IReadOnlyList<SalesforceError>?)null
            };
            // A reference to a parent that wasn't saved has no value, which fails every record of the subrequest
            var brokenReference = ops.Any(o => o.Links.Any(l => l.ExternalIdField is null && l.Parent.Kind != OpKind.Saved));

            for (var i = 0; i < ops.Count; i++)
            {
                var op = ops[i];
                var result = wholeError is null ? answer!.Records![i] : null;
                if (result is { Success: true })
                {
                    op.Kind = OpKind.Saved;
                    written.AddRange(op.Echoes);
                    continue;
                }

                op.Errors = (result?.Errors ?? wholeError!).Select(e => Translate(op, e)).ToList();
                if (result is not null && op.Errors.Count == 0)
                    op.Errors.Add(new SalesforceError(op.ReferenceId, "UNKNOWN_ERROR", "The record failed without error details"));

                var real = op.Errors.Where(e => !SalesforceErrors.NotAttemptedCodes.Contains(e.Code)).ToList();
                // A subrequest that failed as a whole says nothing about any one record's data, so it never
                // dead-letters: the records are retried, and by then a child finds its parent by the read
                op.Kind = op.Links.Any(l => l.Parent.Kind != OpKind.Saved) ? OpKind.ParentNotSaved
                    : real.Count == 0 || (result is null && brokenReference) ? OpKind.NotAttempted
                    : result is not null && real.Any(e => errors.IsPermanent(e.Code) && !IsStaleUpdate(op, e)) ? OpKind.Permanent
                    : OpKind.Transient;
            }
        }

        // Records failed, but none says why (every one of them reports being rolled back because of another): handing
        // the units straight back would repeat the same request forever, so they spend a retry and back off instead.
        var allOps = call.Units.SelectMany(u => u.Ops).ToList();
        var causeUnknown = allOps.Any(o => o.Kind != OpKind.Saved) && !allOps.Any(o => o.Kind is OpKind.Transient or OpKind.Permanent);

        var results = new List<UpsertResult>(call.Units.Count);
        foreach (var unit in call.Units)
        {
            var worst = unit.Ops.Max(o => o.Kind);
            // Most significant first: the record that decided the outcome, and within it the permanent error
            var unitErrors = unit.Ops.Where(o => o.Kind != OpKind.ParentNotSaved).OrderByDescending(o => o.Kind)
                .SelectMany(o => o.Errors.OrderByDescending(e => errors.IsPermanent(e.Code)))
                .ToList();
            var outcome = worst switch
            {
                OpKind.Saved => SyncOutcome.Saved,
                OpKind.Permanent => SyncOutcome.PermanentFailure,
                OpKind.Transient => SyncOutcome.TransientFailure,
                _ => SyncOutcome.NotAttempted
            };
            if (outcome == SyncOutcome.NotAttempted && causeUnknown)
            {
                outcome = SyncOutcome.TransientFailure;
                unitErrors.Insert(0, new SalesforceError(null, "UNKNOWN_ERROR",
                    "Records of the request failed without any of them reporting the cause"));
            }
            results.Add(new(unit.Key, outcome, unitErrors));
        }
        return results;
    }

    /// <summary>
    /// Salesforce rejecting a lookup because its target isn't there (the target was deleted after the read, or the
    /// external id it is addressed by doesn't exist) is the same situation as the read not finding it: the target is
    /// expected to arrive through its own sync, so the record waits for it rather than being dead-lettered.
    /// </summary>
    private static SalesforceError Translate(Op op, SalesforceError e)
    {
        var targetMissing =
            e.Message?.Contains("Foreign key external ID", StringComparison.OrdinalIgnoreCase) == true
            || (e.Code is "INVALID_CROSS_REFERENCE_KEY" or "ENTITY_IS_DELETED"
                && op.LookupFields.Count > 0
                && (e.Fields is not { Count: > 0 } || e.Fields.Any(op.LookupFields.Contains)));
        return targetMissing
            ? new SalesforceError(op.ReferenceId, SalesforceErrors.DependencyNotSynced, $"{e.Code}: {e.Message}", e.Fields)
            : e with { ReferenceId = op.ReferenceId };
    }

    /// <summary>The record was deleted between the read and the update: the next read finds it gone and creates it.</summary>
    private static bool IsStaleUpdate(Op op, SalesforceError e)
        => !op.Create && e.Code is "INVALID_CROSS_REFERENCE_KEY" or "ENTITY_IS_DELETED";

    /// <summary>
    /// Fields to send: every non-empty one for a new record, otherwise only those whose normalized value differs from
    /// Salesforce. A field Salesforce won't let us set (not createable / updateable) is a problem only if it must
    /// actually be written.
    /// </summary>
    private Dictionary<string, object?> Changes(PreparedRecord record, Dictionary<string, JsonElement>? existing,
        List<WriteEcho> echoes, List<SalesforceError> problems)
    {
        var changes = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (var (field, value) in record.Fields)
        {
            // A new record has no value to clear, and an explicit null would override the field's default
            if (existing is null && value is null) continue;

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

    private sealed record PreparedUnit(string Key, List<PreparedRecord> Records, EventType? EventType);

    private sealed record PlannedUnit(string Key, List<Op> Ops);

    /// <summary>The collection a write goes into.</summary>
    /// <param name="Depth">0 for a record that looks up nothing in its unit, 1 for its children, ...</param>
    /// <param name="ByReference">It points at its parent's create by reference.</param>
    private sealed record Group(int Depth, string SObject, bool Create, bool ByReference);

    /// <summary>A lookup to set to a record that is created in the same request.</summary>
    /// <param name="ExternalIdField">The parent's external id field when the link is made through it; null = by reference.</param>
    private sealed record ParentLink(SalesforceField Field, Op Parent, string ExternalId, string? ExternalIdField);

    /// <summary>One record to create (<paramref name="id"/> null) or update, and what came of it.</summary>
    private sealed class Op(PreparedRecord record, string? id, Dictionary<string, object?> changes, List<ParentLink> links,
        HashSet<string> lookupFields, List<WriteEcho> echoes, int depth)
    {
        public bool Create => id is null;
        public string ReferenceId => record.Record.ReferenceId;
        public List<ParentLink> Links => links;
        public List<WriteEcho> Echoes => echoes;

        /// <summary>The lookups this write sets, by field and relationship name.</summary>
        public HashSet<string> LookupFields => lookupFields;

        public Group Group { get; } = new(depth, record.Schema.Name, id is null, links.Any(l => l.ExternalIdField is null));

        /// <summary>Where the record is in the request: its collection's reference id and its position in it.</summary>
        public string? Collection { get; set; }
        public int Index { get; set; }

        public OpKind Kind { get; set; } = OpKind.NotAttempted;
        public List<SalesforceError> Errors { get; set; } = [];

        public IReadOnlyDictionary<string, object?> Body()
        {
            var body = new Dictionary<string, object?> { ["attributes"] = new Dictionary<string, object?> { ["type"] = record.Schema.Name } };
            if (id is not null) body["id"] = id;
            foreach (var (field, value) in changes) body[field] = value;
            foreach (var link in links)
            {
                if (link.ExternalIdField is not null)
                    body[link.Field.RelationshipName!] = new Dictionary<string, object?> { [link.ExternalIdField] = link.ExternalId };
                else
                    // The parent's row in its collection's response. Composite documents @{referenceId.field} with []
                    // indexing; indexing into an sObject Collections response is not confirmed: verify in a sandbox.
                    body[link.Field.Name] = $"@{{{link.Parent.Collection}[{link.Parent.Index}].id}}";
            }
            return body;
        }
    }
}
