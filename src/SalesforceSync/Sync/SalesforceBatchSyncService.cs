using System.Text.Json;
using Microsoft.Extensions.Options;
using SalesforceSync.Data;
using SalesforceSync.Data.Inbox;
using SalesforceSync.Infrastructure.RateLimiting;
using SalesforceSync.Infrastructure.Salesforce;

namespace SalesforceSync.Sync;

/// <summary>
/// The one sync engine for every Salesforce object. <see cref="SyncBatchAsync{TEvent, TRow}"/>
/// runs one batch end to end; the inbox repository (its row type is the type's inbox table) holds each entity's newest
/// event, and the strategy says how to read one, how it maps to Salesforce records and what to read from Salesforce.
/// <list type="number">
/// <item>Takes a global concurrency slot and respects a shared pause (rate limits, all pods).</item>
/// <item>Claims up to <see cref="SyncOptions.BatchSize"/> due entities: each one's inbox row is locked with FOR UPDATE
/// SKIP LOCKED, leased and read, so no two workers in any pod ever process the same entity at once.</item>
/// <item>Per entity, reads the claimed event: the newest one stored (events carry the full record, and a newer event
/// overwrites the entity's row; one not newer than a synced event is never stored). Arrival order plays no
/// part, so events may come out of order, over any partitions. An event that can't be read or mapped is
/// dead-lettered on its own.</item>
/// <item>Writes the units to Salesforce (<see cref="WriteToSalesforceAsync"/>), giving up before the lease runs out:
/// reads the current records (one query per object), then sends what differs, one upsert per object with parents first,
/// and last the records to delete that Salesforce has, in one delete.</item>
/// <item>Records each entity's outcome in one transaction: done, released for the
/// next claim, retried with backoff, or dead-lettered. A Salesforce call that fails as a whole fails the whole batch:
/// every claimed entity is held (the write's outcome is unknown), deferred (rate limited) or retried. An entity whose
/// row a newer event overwrote meanwhile gets no outcome: its row stays pending and due.</item>
/// </list>
/// <para>
/// From an event to Salesforce, step by step (event objects and SObjects are two different classes):
/// </para>
/// <list type="number">
/// <item><b>Event object</b> (<c>AccountEvent</c>, <c>ContactEvent</c>, ...): the inbox payload as the source sent it
/// (camelCase names, its own shape). Only the strategy's <c>Map</c> reads it.</item>
/// <item><b>SObject</b> (<c>AccountSObject</c>, <c>ContactSObject</c>, ...): the same data as Salesforce names it (API
/// field names, <c>[SObject]</c> attributes). <c>Map</c> builds one per Salesforce record and
/// <see cref="SObjectRecord.From{TSObject}"/> turns it into a plain <see cref="SObjectRecord"/>: that is where the event stops
/// and Salesforce's side begins, and the engine never sees an event object again.</item>
/// <item>The records of one entity are its <see cref="SyncUnit"/>.</item>
/// <item><b>Diff</b> (<see cref="Plan"/>): each record is compared with what the read found in Salesforce, and
/// becomes a create (all its fields), an update (only the fields that differ), a delete or nothing.</item>
/// <item><b>Write</b> (<see cref="WriteAsync"/>): one upsert per object, parents first, then the deletes.</item>
/// </list>
/// This class is the flow (<see cref="SyncBatchAsync{TEvent, TRow}"/>) and, below it, the steps of
/// <see cref="WriteToSalesforceAsync"/> in the order they run. The pure ones (normalize, build the read, diff) are static
/// and do no I/O; Salesforce is reached only through <see cref="ISalesforceRestClient"/> and the inbox only through
/// <see cref="IRepository{TEntity}"/>, whose every unit of work runs through <see cref="PostgresRetryStrategy"/>.
/// </summary>
public sealed class SalesforceBatchSyncService(
    ISalesforceBatchGate gate,
    ISalesforceRestClient salesforce,
    SalesforceSchemaCache schemaCache,
    SalesforceErrors errors,
    IOptions<SalesforceOptions> salesforceOptions,
    IOptions<SyncOptions> options,
    ILogger<SalesforceBatchSyncService> log)
{
    /// <summary>
    /// How inbox payloads (whole event envelopes) are deserialized into a strategy's event type: camelCase JSON,
    /// matched to the PascalCase CLR properties without regard to case.
    /// </summary>
    public static readonly JsonSerializerOptions EventJson = new(JsonSerializerDefaults.Web);

    /// <summary>
    /// Share of the lease a batch may spend before its Salesforce call is cancelled; the rest is margin for recording
    /// the outcome. A write cancelled this way has an unknown outcome and puts its entities on hold.
    /// </summary>
    private const double LeaseUsableFraction = 0.8;

    /// <summary>Recording a hold must not be cut short by the cancellation that caused it (shutdown), but may not hang it either.</summary>
    private static readonly TimeSpan HoldRecordingTimeout = TimeSpan.FromSeconds(10);

    private readonly SyncOptions _opt = options.Value;
    private readonly SalesforceOptions _salesforceOpt = salesforceOptions.Value;

    /// <summary>Claims, writes (upserts and deletes) and records one batch of the strategy's entity type.</summary>
    /// <returns>Inbox rows processed (0 = nothing to do, or no capacity right now).</returns>
    public async Task<int> SyncBatchAsync<TEvent, TRow>(SyncStrategy<TEvent> strategy, IRepository<TRow> inbox, CancellationToken ct)
        where TRow : InboxRow, IInboxRow, new()
    {
        await using var admission = await gate.TryEnterAsync(ct);
        if (admission is null)
        {
            return 0;
        }

        // Give up on Salesforce before the lease runs out, so another worker never claims an entity while this one
        // can still write to it. Started before the claim, so the margin is measured from when the lease began.
        using var leaseCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        leaseCts.CancelAfter(_opt.Lease * LeaseUsableFraction);

        // 1. Claim entities and their rows from the inbox table (short transaction; the lease, not a transaction,
        // protects the entities until step 4)
        var claim = await inbox.ClaimAsync(ct);
        if (claim.Events.Count == 0)
        {
            return 0;
        }

        // 2. Per entity: map its event to records
        var batch = PreparedBatch.Prepare(strategy, claim.Events);

        // 3. Salesforce (outside any transaction): read, decide, send, interpret. Not inside one on purpose: Salesforce
        // can't take part in a Postgres transaction (a rollback can't undo a saved write), the execution strategy
        // re-runs its whole delegate on a transient Postgres error (the HTTP calls would be sent again), and a
        // transaction held for up to the request timeout keeps locks, a pooled connection per worker and autovacuum's
        // horizon on a high-churn table. A crash between here and step 4 leaves the rows pending under an expiring
        // lease; the retry reads Salesforce and writes only what still differs.
        Dictionary<string, SyncResult> results;
        try
        {
            results = (await WriteToSalesforceAsync(batch.Units, [.. strategy.Queries([.. batch.Work.Select(w => w.Unit)])],
                TRow.MaxValuesPerQuery, TRow.MaxRecordsPerWrite, leaseCts.Token)).ToDictionary(r => r.Key);
        }
        catch (UnknownOutcomeException ex)
        {
            // Salesforce may still apply the write. Caught on shutdown too: an entity released now could be written
            // by another worker before the lost write lands. If the hold can't be recorded (or the process is killed
            // first), the lease is what still protects the entities.
            log.LogWarning(ex, "{Type} batch: holding {Count} events for {Hold}", strategy.EntityType, claim.Events.Count, _opt.UnknownOutcomeHold);
            using var recording = new CancellationTokenSource(HoldRecordingTimeout);
            await inbox.HoldAsync(claim, ex.Message, recording.Token);
            ct.ThrowIfCancellationRequested();
            return claim.Events.Count;
        }
        catch (MappingErrorException ex)
        {
            // Nothing was written. The inbox stops (every due row waits) with the error on the rows to fix, so nothing is
            // dead-lettered and the rows go on after the deploy with no replay.
            log.LogError(ex, "{Type} inbox stopped for {Hold} by {Count} mapping error(s), fix the mapping or add the field: {Errors}",
                strategy.EntityType, _opt.MappingErrorHold, ex.Errors.Count,
                string.Join("; ", ex.Errors.Select(e => $"{e.Key}: {e.Error.Code} {e.Error.Message}")));
            await inbox.HaltAsync(claim, [.. ex.Errors.GroupBy(e => e.Key).Select(g => new FailedEvent(g.Key,
                string.Join("; ", g.Select(e => $"{e.Error.Code}: {e.Error.Message}")), InboxRow.MappingErrorCode))],
                $"Held: the {strategy.EntityType} inbox is stopped by a mapping error ({ex.Message})", ct);
            return claim.Events.Count;
        }
        catch (RateLimitedException ex)
        {
            log.LogInformation("Rate limited; deferring {Count} {Type} events for {Delay}", claim.Events.Count, strategy.EntityType, ex.RetryAfter);
            await inbox.DeferAsync(claim, ex.RetryAfter, ct);
            return 0;
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            // A request failed that Salesforce provably did not apply (describe, the read or a write): every entity is
            // retried. Records that earlier write requests of the batch saved are safe: the retry reads them and writes
            // only what still differs. Shutdown (ct) is not caught: the failed request wrote nothing, and the lease
            // simply expires.
            log.LogError(ex, "{Type} batch failed", strategy.EntityType);
            await inbox.FailAllAsync(claim, ex.Message, (ex as SalesforceResponseException)?.ErrorCode, ct);
            return claim.Events.Count;
        }

        // 4. Record every entity's outcome in the inbox
        LogFailures(strategy.EntityType, batch, results.Values);
        await inbox.RecordAsync(claim, batch.Outcomes(results), ct);
        return claim.Events.Count;
    }

    /// <summary>
    /// The Salesforce half of a batch, for any sObjects, in six steps: describe, normalize, read (one query per object),
    /// diff, send (one upsert per object, parents first, then one delete) and interpret the answers. Returns one outcome
    /// per unit. Nothing here touches the inbox.
    /// </summary>
    /// <remarks>
    /// A call that fails as a whole (the call, not records in it) throws, and the caller fails, holds or defers the whole
    /// claim, even when an earlier call of the batch saved its records: the failed call's outcome decides, so an unknown
    /// one holds every claimed entity. Nothing earlier calls learned is recorded, and nothing needs to be: every write is
    /// matched by external id, so the retry reads Salesforce, finds what they saved and writes only what still differs.
    /// </remarks>
    /// <param name="units">At most <see cref="SyncOptions.MaxEntitiesPerBatch"/>, keyed by entity key.</param>
    /// <param name="queries">The strategy's reads: the units' records and the records their foreign keys point at.</param>
    /// <param name="maxValuesPerQuery">The row type's <see cref="IInboxRow.MaxValuesPerQuery"/>.</param>
    /// <param name="maxRecordsPerWrite">The row type's <see cref="IInboxRow.MaxRecordsPerWrite"/>.</param>
    /// <param name="ct">Cancels the calls; shutdown, not a failure.</param>
    /// <exception cref="UnknownOutcomeException">The write call failed without proving that nothing was applied.</exception>
    internal async Task<List<SyncResult>> WriteToSalesforceAsync(IReadOnlyList<(string Key, SyncUnit Unit)> units,
        IReadOnlyList<SoqlQuery> queries, int maxValuesPerQuery, int maxRecordsPerWrite, CancellationToken ct)
    {
        if (units.Count > _opt.MaxEntitiesPerBatch)
        {
            throw new ArgumentOutOfRangeException(nameof(units), $"At most {_opt.MaxEntitiesPerBatch} units per call");
        }

        var results = new List<SyncResult>(units.Count);
        if (units.Count == 0)
        {
            return results;
        }

        var schemas = await DescribeAsync(units, queries, ct);

        // A unit Salesforce would reject fails on its own, before any call
        var (normalized, rejected) = Normalize(units, schemas);
        if (MappingErrors(rejected).Count > 0)
        {
            // The cached describe may just be out of date (a field was added to the org): describe again before stopping
            await InvalidateAsync(schemas.Keys, ct);
            schemas = await DescribeAsync(units, queries, ct);
            (normalized, rejected) = Normalize(units, schemas);
            await ThrowIfMappingErrorsAsync(rejected, schemas.Keys, ct);
        }

        results.AddRange(rejected);

        if (normalized.Count > 0)
        {
            var current = await ReadCurrentAsync(BuildReadQueries(normalized, queries, schemas, maxValuesPerQuery), schemas.Keys, ct);
            var (planned, decided) = Plan(normalized, current);
            await ThrowIfMappingErrorsAsync(decided, schemas.Keys, ct);
            results.AddRange(decided);

            try
            {
                results.AddRange(await WriteAsync(planned, maxRecordsPerWrite, ct));
            }
            catch (Exception ex) when (WriteOutcome.IsUnknown(ex))
            {
                // Everything before the first write only read from Salesforce, so its failures are safe to retry. From
                // here on Salesforce may have the write, and may still apply it after we stop waiting.
                throw new UnknownOutcomeException(ex);
            }
        }

        await InvalidateRecordTypesAsync(units, results, ct);
        return results;
    }

    // ---------- Mapping errors ----------

    /// <summary>
    /// A field the code sends that Salesforce doesn't have, or won't let us set: the mapping is wrong, for every event of
    /// that kind, so only a code change or a field in the org fixes it. Retrying or dead-lettering one entity at a time
    /// would only spend attempts, so the caller stops the inbox instead.
    /// </summary>
    private static bool IsMappingError(SalesforceError e)
    {
        return e.Code is "INVALID_FIELD" or "INVALID_FIELD_FOR_INSERT_UPDATE";
    }

    private static List<(string Key, SalesforceError Error)> MappingErrors(IEnumerable<SyncResult> results)
    {
        return [.. results.SelectMany(r => r.Errors.Where(IsMappingError).Select(e => (r.Key, e)))];
    }

    /// <summary>Stops the batch, before any write, if a unit has a mapping error; the described objects are described again next time.</summary>
    /// <exception cref="MappingErrorException">At least one unit has a mapping error.</exception>
    private async Task ThrowIfMappingErrorsAsync(IEnumerable<SyncResult> results, IEnumerable<string> sobjects, CancellationToken ct)
    {
        if (MappingErrors(results) is { Count: > 0 } found)
        {
            await InvalidateAsync(sobjects, ct);
            throw new MappingErrorException(found);
        }
    }

    private async Task InvalidateAsync(IEnumerable<string> sobjects, CancellationToken ct)
    {
        foreach (var sobject in sobjects)
        {
            await schemaCache.InvalidateAsync(sobject, ct);
        }
    }

    /// <summary>Units whose mapping Salesforce rejects, with what to fix. Thrown before any write.</summary>
    internal sealed class MappingErrorException(IReadOnlyList<(string Key, SalesforceError Error)> errors)
        : Exception($"{errors.Count} mapping error(s), e.g. {errors[0].Error.Code}: {errors[0].Error.Message}")
    {
        public IReadOnlyList<(string Key, SalesforceError Error)> Errors => errors;
    }

    // ---------- 1. Describe ----------

    /// <summary>Field metadata of every object involved (cached per process, revalidated with If-Modified-Since).</summary>
    private async Task<Dictionary<string, SObjectSchema>> DescribeAsync(IReadOnlyList<(string Key, SyncUnit Unit)> units,
        IReadOnlyList<SoqlQuery> queries, CancellationToken ct)
    {
        var sobjects = units.SelectMany(u => u.Unit.Records)
            .SelectMany(r => r.Parents.Select(p => p.SObject).Append(r.SObject))
            .Concat(queries.Select(q => q.SObject))
            .Distinct(StringComparer.OrdinalIgnoreCase);

        var schemas = new Dictionary<string, SObjectSchema>(StringComparer.OrdinalIgnoreCase);
        foreach (var sobject in sobjects)
        {
            schemas[sobject] = await schemaCache.GetAsync(sobject, _salesforceOpt.SchemaRefreshInterval,
                (since, token) => salesforce.DescribeAsync(sobject, since, token), ct);
        }

        return schemas;
    }

    /// <summary>
    /// A record type the cached describe doesn't have (or not as active and available), or whose Id Salesforce rejected,
    /// may just have been created, activated or assigned: its object is described again next time.
    /// </summary>
    private async Task InvalidateRecordTypesAsync(IReadOnlyList<(string Key, SyncUnit Unit)> units, IEnumerable<SyncResult> results,
        CancellationToken ct)
    {
        var failed = results
            .SelectMany(r => r.Errors.Where(e => e.Code == SalesforceErrors.InvalidRecordType).Select(e => (r.Key, e.ReferenceId)))
            .ToHashSet();
        var sobjects = units
            .SelectMany(u => u.Unit.Records.Where(r => failed.Contains((u.Key, r.ReferenceId))))
            .Select(r => r.SObject)
            .Distinct(StringComparer.OrdinalIgnoreCase);
        foreach (var sobject in sobjects)
        {
            await schemaCache.InvalidateAsync(sobject, ct);
        }
    }

    // ---------- 2. Normalize ----------

    /// <summary>A unit whose values are in the form Salesforce stores them, ready to be compared and sent.</summary>
    internal sealed record NormalizedUnit(string Key, IReadOnlyList<NormalizedRecord> Records);

    /// <summary>A record with its object's Describe metadata and its values normalized per field.</summary>
    internal sealed record NormalizedRecord(SObjectRecord Record, SObjectSchema Schema, IReadOnlyDictionary<SalesforceField, object?> Fields)
    {
        public string ReferenceId => Record.ReferenceId;
    }

    /// <summary>
    /// Checks each unit against the Describe metadata and normalizes its values (<see cref="FieldValues"/>), before any
    /// call. An unknown field, a value Salesforce would reject, a foreign key that isn't a text field, or a record type
    /// the org doesn't offer is a problem of that unit alone: the unit is rejected, permanent if any of its problems is
    /// (<see cref="SalesforceErrors.IsPermanent"/>), else transient (a record type the org doesn't offer yet), and the
    /// rest of the batch carries on. A record's record type becomes its RecordTypeId here, a field like any other.
    /// </summary>
    internal (List<NormalizedUnit> Normalized, List<SyncResult> Rejected) Normalize(
        IReadOnlyList<(string Key, SyncUnit Unit)> units, IReadOnlyDictionary<string, SObjectSchema> schemas)
    {
        var normalized = new List<NormalizedUnit>(units.Count);
        var rejected = new List<SyncResult>();
        foreach (var (key, unit) in units)
        {
            var candidate = Normalize(key, unit, schemas, out var problems);
            if (problems.Count == 0)
            {
                normalized.Add(candidate);
                continue;
            }

            var permanent = problems.Any(p => errors.IsPermanent(p.Code));
            rejected.Add(new(key, permanent ? SyncOutcome.PermanentFailure : SyncOutcome.TransientFailure,
                [.. problems.OrderByDescending(p => errors.IsPermanent(p.Code))]));
        }
        return (normalized, rejected);
    }

    /// <param name="key">The entity's key.</param>
    /// <param name="unit">The records the strategy mapped.</param>
    /// <param name="schemas">Describe of every object in the unit, by API name.</param>
    /// <param name="problems">Why Salesforce would reject the unit; empty when it can be written.</param>
    internal static NormalizedUnit Normalize(string key, SyncUnit unit, IReadOnlyDictionary<string, SObjectSchema> schemas,
        out List<SalesforceError> problems)
    {
        var found = problems = [];
        return new NormalizedUnit(key, [.. unit.Records.Select(r => Normalize(r, schemas, found))]);
    }

    private static NormalizedRecord Normalize(SObjectRecord record, IReadOnlyDictionary<string, SObjectSchema> schemas,
        List<SalesforceError> problems)
    {
        var schema = schemas[record.SObject];
        SalesforceError NoSuchColumn(string column)
        {
            return new(record.ReferenceId, "INVALID_FIELD", $"No such column '{column}' on {schema.Name}");
        }

        var values = RecordTypeId(record, schema, problems) is { } recordTypeId
            ? record.Fields.Append(new(SObjectSchema.RecordTypeIdField, recordTypeId))
            : record.Fields;

        var fields = new Dictionary<SalesforceField, object?>();
        foreach (var (name, value) in values)
        {
            if (schema.Find(name) is not { } field)
            {
                problems.Add(NoSuchColumn(name));
            }
            else if (FieldValues.TryNormalize(field, value, out var normalized, out var error))
            {
                fields[field] = normalized;
            }
            else
            {
                problems.Add(new(record.ReferenceId, error.Code, $"{field.Name}: {error.Message}"));
            }
        }

        // A foreign key needs the external id field on its parent's object, and a field on the record that holds text
        // (not a reference, which holds an Id; the field itself is checked with the values)
        foreach (var key in record.Parents)
        {
            if (schema.Find(key.Field) is { Type: "reference" } || schemas[key.SObject].Find(key.ExternalIdField) is null)
            {
                problems.Add(new(record.ReferenceId, "INVALID_FIELD",
                    $"{schema.Name}.{key.Field} is a lookup, not a foreign key, or {key.SObject}.{key.ExternalIdField} doesn't exist"));
            }
        }

        if (schema.Find(record.ExternalIdField) is null)
        {
            problems.Add(NoSuchColumn(record.ExternalIdField));
        }

        return new NormalizedRecord(record, schema, fields);
    }

    /// <summary>
    /// The Id of the record's record type, from Describe's record types: null if the record has none, or if the org has
    /// no such record type for the running user (a problem; the object is described again next time). Describe rather
    /// than a query on RecordType: no extra call, cached and refreshed with the fields, and it already says whether the
    /// record type is active and available to the running user.
    /// </summary>
    private static string? RecordTypeId(SObjectRecord record, SObjectSchema schema, List<SalesforceError> problems)
    {
        if (record.RecordType is not { } developerName)
        {
            return null;
        }

        var recordType = schema.RecordTypes.GetValueOrDefault(developerName);
        var why = recordType switch
        {
            null => "doesn't exist",
            { Active: false } => "is not active",
            { Available: false } => "is not available to the running user",
            _ => null
        };
        if (why is null)
        {
            return recordType!.RecordTypeId;
        }

        problems.Add(new(record.ReferenceId, SalesforceErrors.InvalidRecordType,
            $"Record type '{developerName}' of {schema.Name} {why}", [SObjectSchema.RecordTypeIdField]));
        return null;
    }

    // ---------- 3. Read ----------

    /// <summary>A query of the batch's read and the object its rows belong to.</summary>
    internal sealed record ReadQuery(string Soql, string SObject);

    /// <summary>
    /// The strategy's queries as SOQL, each selecting every field the diff compares, the Id (a delete needs it) and the
    /// external ids that show a parent exists, split into chunks of at most <paramref name="maxValuesPerQuery"/> values
    /// (the row type's) whose URL fits a call (<see cref="SalesforceRestClient.MaxUrlBytes"/>).
    /// </summary>
    /// <exception cref="InvalidOperationException">A query's URL is too long even with a single value.</exception>
    internal List<ReadQuery> BuildReadQueries(IReadOnlyList<NormalizedUnit> units, IReadOnlyList<SoqlQuery> queries,
        IReadOnlyDictionary<string, SObjectSchema> schemas, int maxValuesPerQuery)
    {
        var records = units.SelectMany(u => u.Records).ToList();
        var read = new List<ReadQuery>();

        foreach (var query in queries.Where(q => q.Values.Count > 0))
        {
            var target = schemas[query.SObject];

            // What the diff compares, the external ids its records and the foreign keys to it are matched by, the field the
            // query filters on, and the Id
            var fields = records.Where(r => r.Schema == target)
                .SelectMany(r => r.Fields.Keys.Select(f => f.Name).Append(r.Record.ExternalIdField))
                .Concat(records.SelectMany(r => r.Record.Parents)
                    .Where(p => target.Name.Equals(p.SObject, StringComparison.OrdinalIgnoreCase)).Select(p => p.ExternalIdField))
                .Append(query.WhereField)
                .Append("Id")
                .Select(f => target.Find(f)?.Name ?? f)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();

            read.AddRange(Chunked(target.Name, fields, query.WhereField, query.Values.Distinct(StringComparer.Ordinal), maxValuesPerQuery)
                .Select(soql => new ReadQuery(soql, target.Name)));
        }

        return read;
    }

    /// <summary>
    /// The values in order as queries of at most <paramref name="maxValues"/> values: a query takes the next value while
    /// its URL, as the client sends it, still fits. Percent-encoding works character by character, so a URL's length is the
    /// empty query's plus each value's encoded literal and separator: counted per value, not rebuilt.
    /// </summary>
    private IEnumerable<string> Chunked(string sobject, List<string> fields, string whereField, IEnumerable<string> values, int maxValues)
    {
        var empty = SalesforceRestClient.QueryUrl(salesforce.DataPath, SalesforceRestClient.Soql(sobject, fields, whereField, [])).Length;
        var separator = Uri.EscapeDataString(",").Length;
        var chunk = new List<string>();
        var length = empty;
        foreach (var value in values)
        {
            var literal = Uri.EscapeDataString(SalesforceRestClient.SoqlLiteral(value)).Length;
            if (chunk.Count > 0 && chunk.Count < maxValues && length + separator + literal <= SalesforceRestClient.MaxUrlBytes)
            {
                chunk.Add(value);
                length += separator + literal;
                continue;
            }

            if (chunk.Count > 0)
            {
                yield return SalesforceRestClient.Soql(sobject, fields, whereField, chunk);
            }

            chunk = [value];
            length = empty + literal;
            if (length > SalesforceRestClient.MaxUrlBytes)
            {
                throw new InvalidOperationException(
                    $"The read of {sobject} by {whereField} does not fit a URL ({SalesforceRestClient.MaxUrlBytes:N0} bytes) " +
                    $"even for one value ({value.Length:N0} characters) and its {fields.Count} fields");
            }
        }

        if (chunk.Count > 0)
        {
            yield return SalesforceRestClient.Soql(sobject, fields, whereField, chunk);
        }
    }

    private async Task<CurrentRecords> ReadCurrentAsync(IReadOnlyList<ReadQuery> queries, IEnumerable<string> describedSObjects,
        CancellationToken ct)
    {
        var rows = new List<(string SObject, JsonElement Row)>();
        try
        {
            foreach (var query in queries)
            {
                rows.AddRange((await salesforce.SelectAsync(query.Soql, ct)).Select(row => (query.SObject, row)));
            }
        }
        catch (SalesforceResponseException ex) when (ex.ErrorCode == "INVALID_FIELD")
        {
            // The cached describe is out of date (a field was removed or renamed): describe again next time
            await InvalidateAsync(describedSObjects, ct);
            throw;
        }

        return new CurrentRecords(rows);
    }

    /// <summary>A record as Salesforce has it now: the fields the read selected, by API name (case-insensitive).</summary>
    internal sealed class CurrentRecord(Dictionary<string, JsonElement> fields)
    {
        /// <summary>The Id as Salesforce returned it; a delete addresses the record by it.</summary>
        public string Id => fields["Id"].GetString()!;

        public IEnumerable<KeyValuePair<string, JsonElement>> Fields => fields;

        public bool TryGetValue(string field, out JsonElement value)
        {
            return fields.TryGetValue(field, out value);
        }
    }

    /// <summary>Rows read from Salesforce, indexed by every text field that was selected (external ids, Id).</summary>
    internal sealed class CurrentRecords
    {
        public static readonly CurrentRecords None = new([]);

        private readonly Dictionary<(string SObject, string Field, string Value), CurrentRecord> _index = [];

        /// <summary>The rows a read found, each with the object its query was for.</summary>
        public CurrentRecords(IEnumerable<(string SObject, JsonElement Row)> rows)
        {
            foreach (var (sobject, row) in rows)
            {
                var record = new CurrentRecord(row.EnumerateObject().Where(p => p.Name != "attributes")
                    .ToDictionary(p => p.Name, p => p.Value, StringComparer.OrdinalIgnoreCase));
                foreach (var (field, value) in record.Fields.Where(f => f.Value.ValueKind == JsonValueKind.String))
                {
                    _index.TryAdd((sobject.ToLowerInvariant(), field.ToLowerInvariant(), value.GetString()!), record);
                }
            }
        }

        public CurrentRecord? Find(string sobject, string field, string value)
        {
            return _index.GetValueOrDefault((sobject.ToLowerInvariant(), field.ToLowerInvariant(), value));
        }
    }

    // ---------- 4. Diff ----------

    /// <summary>One record to send, as decided by the diff: a create, an update (only the fields that differ) or a delete.</summary>
    /// <param name="UnitKey">The entity the record belongs to.</param>
    /// <param name="Record">The record, normalized against Describe.</param>
    /// <param name="Id">Its Salesforce Id when it exists there (a delete needs it), null when it is new.</param>
    /// <param name="Fields">What to send, by API name: all of a new record's non-empty fields, only the changed ones of an existing one.</param>
    internal sealed record Change(string UnitKey, NormalizedRecord Record, string? Id, IReadOnlyDictionary<string, object?> Fields)
    {
        public bool IsDelete => Record.Record.Delete;
        public bool IsCreate => !IsDelete && Id is null;
        public string SObject => Record.Schema.Name;
        public string ExternalIdField => Record.Schema.Find(Record.Record.ExternalIdField)!.Name;
        public string ReferenceId => Record.ReferenceId;

        /// <summary>The record as an upsert takes it: its type, its external id (which decides create or update) and the fields to send.</summary>
        public SObjectBody Body()
        {
            var body = new SObjectBody(SObject) { [ExternalIdField] = Record.Record.ExternalId };
            foreach (var (field, value) in Fields)
            {
                body[field] = value;
            }
            return body;
        }
    }

    /// <summary>An entity that has something to send.</summary>
    internal sealed record PlannedUnit(string Key, IReadOnlyList<Change> Changes);

    /// <summary>
    /// What each record needs, by comparing it with what Salesforce has (what the read found):
    /// <list type="bullet">
    /// <item>not in Salesforce: a create, with all its non-empty fields;</item>
    /// <item>in Salesforce, some field differs: an update, with only the fields that differ (<see cref="Diff"/>);</item>
    /// <item>in Salesforce, nothing differs: nothing to send;</item>
    /// <item>to delete and in Salesforce: a delete by the Id the read found; to delete and not there: nothing to send.</item>
    /// </list>
    /// Then each entity (unit) is one of: rejected (a field can't be written: permanent), waiting (a record it points at
    /// is neither in Salesforce nor in the unit: transient), unchanged (nothing to send) or planned. No I/O.
    /// </summary>
    /// <returns>The entities with something to send, and the results of those that need no call.</returns>
    internal static (List<PlannedUnit> Planned, List<SyncResult> Decided) Plan(IReadOnlyList<NormalizedUnit> units, CurrentRecords current)
    {
        var planned = new List<PlannedUnit>();
        var decided = new List<SyncResult>();

        foreach (var unit in units)
        {
            var problems = new List<SalesforceError>(); // what Salesforce would reject: permanent
            var missing = new List<SalesforceError>();  // a parent that isn't there yet: transient
            var changes = ChangesOf(unit, current, problems, missing);

            if (problems.Count == 0 && missing.Count == 0 && changes.Count > 0)
            {
                planned.Add(new PlannedUnit(unit.Key, changes));
            }
            else
            {
                decided.Add(problems.Count > 0 ? new(unit.Key, SyncOutcome.PermanentFailure, problems)
                    : missing.Count > 0 ? new(unit.Key, SyncOutcome.TransientFailure, missing)
                    : new(unit.Key, SyncOutcome.Unchanged, []));
            }
        }

        return (planned, decided);
    }

    private static List<Change> ChangesOf(NormalizedUnit unit, CurrentRecords current, List<SalesforceError> problems,
        List<SalesforceError> missing)
    {
        var changes = new List<Change>();
        var inUnit = unit.Records.Where(r => !r.Record.Delete).Select(r => (r.Record.SObject.ToLowerInvariant(), r.Record.ExternalId)).ToHashSet();

        foreach (var record in unit.Records)
        {
            var existing = current.Find(record.Record.SObject, record.Record.ExternalIdField, record.Record.ExternalId);

            if (record.Record.Delete)
            {
                // By the Id the read found. A record the read didn't find is deleted already (or never was): nothing to send.
                if (existing is not null)
                {
                    changes.Add(new Change(unit.Key, record, existing.Id, new Dictionary<string, object?>()));
                }
                continue;
            }

            // A record that points at a parent needs it to exist: in Salesforce already, or created by this same unit
            foreach (var parent in record.Record.Parents)
            {
                if (!inUnit.Contains((parent.SObject.ToLowerInvariant(), parent.ExternalId)) && current.Find(parent.SObject, parent.ExternalIdField, parent.ExternalId) is null)
                {
                    missing.Add(new(record.ReferenceId, SalesforceErrors.DependencyNotSynced,
                        $"{record.Schema.Name}.{parent.Field}: {parent.SObject} {parent.ExternalIdField}={parent.ExternalId} is not in Salesforce yet"));
                }
            }

            var fields = Diff(record, existing, problems);

            // A new record is always written; an existing one only when a field differs
            if (existing is null || fields.Count > 0)
            {
                changes.Add(new Change(unit.Key, record, existing?.Id, fields));
            }
        }
        return changes;
    }

    /// <summary>
    /// The fields to send: every non-empty one for a new record, otherwise only those whose normalized value differs from
    /// Salesforce's. A field Salesforce won't let us set (not createable / updateable) is a problem only if it must
    /// actually be written.
    /// </summary>
    /// <param name="record">The desired record, normalized.</param>
    /// <param name="existing">The record in Salesforce, or null if it is new.</param>
    /// <param name="problems">Receives the fields that must be written but can't be.</param>
    internal static Dictionary<string, object?> Diff(NormalizedRecord record, CurrentRecord? existing, List<SalesforceError> problems)
    {
        var fields = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (var (field, value) in record.Fields)
        {
            // A new record has no value to clear, and an explicit null would override the field's default
            if (existing is null && value is null)
            {
                continue;
            }

            if (existing is not null && existing.TryGetValue(field.Name, out var stored) && FieldValues.AreEqual(field, value, stored))
            {
                continue;
            }

            var creating = existing is null;
            if (!(creating ? field.Createable : field.Updateable))
            {
                problems.Add(new SalesforceError(record.ReferenceId, "INVALID_FIELD_FOR_INSERT_UPDATE",
                    $"{record.Schema.Name}.{field.Name} is not {(creating ? "createable" : "updateable")}"));
                continue;
            }

            fields[field.Name] = FieldValues.ToWire(value);
        }
        return fields;
    }

    // ---------- 5. Send ----------

    /// <summary>
    /// Sends the planned changes, one call per object, and reads the answers into one outcome per entity:
    /// <list type="number">
    /// <item>Upserts, object by object, parents first: an object comes after every object its records point at, because
    /// the org rejects a child whose parent doesn't exist yet. One call per object
    /// (<see cref="ISalesforceRestClient.UpsertAsync"/>), or one per 200 records of it.</item>
    /// <item>Then the deletes, in one call (<see cref="ISalesforceRestClient.DeleteAsync"/>), after every upsert: a child
    /// an upsert moved to another parent must not be taken down by the delete of its old parent.</item>
    /// </list>
    /// An entity that failed in one call is not sent in the next ones: its outcome is already decided, and what it still has
    /// to send would only fail for the same reason. Records it saved stay saved, and the retry reads and diffs them.
    /// </summary>
    internal async Task<List<SyncResult>> WriteAsync(IReadOnlyList<PlannedUnit> units, int maxRecordsPerWrite, CancellationToken ct)
    {
        var all = units.SelectMany(u => u.Changes).ToList();
        var worst = units.ToDictionary(u => u.Key, _ => SyncOutcome.Saved);       // an entity's outcome: its worst record's
        var failures = units.ToDictionary(u => u.Key, _ => new List<(SyncOutcome Outcome, SalesforceError Error)>());

        async Task SendAsync(IEnumerable<Change> changes, Func<Change[], Task<IReadOnlyList<RecordResult>>> call)
        {
            foreach (var chunk in changes.Where(c => worst[c.UnitKey] == SyncOutcome.Saved).ToList().Chunk(maxRecordsPerWrite))
            {
                var answers = await call(chunk);
                for (var i = 0; i < chunk.Length; i++)
                {
                    // An answer is expected for every record, in order; a missing one is a failure that says nothing about the data
                    var (outcome, recordErrors) = Interpret(chunk[i], answers.Count == chunk.Length ? answers[i] : null);
                    failures[chunk[i].UnitKey].AddRange(recordErrors.Select(e => (outcome, e)));
                    worst[chunk[i].UnitKey] = (SyncOutcome)Math.Max((int)worst[chunk[i].UnitKey], (int)outcome);
                }
            }
        }

        foreach (var sobject in ParentsFirst(all.Where(c => !c.IsDelete)))
        {
            await SendAsync(all.Where(c => !c.IsDelete && c.SObject == sobject),
                chunk => salesforce.UpsertAsync(sobject, chunk[0].ExternalIdField, [.. chunk.Select(c => c.Body())], ct));
        }

        await SendAsync(all.Where(c => c.IsDelete), chunk => salesforce.DeleteAsync([.. chunk.Select(c => c.Id!)], ct));

        return [.. units.Select(u => new SyncResult(u.Key, worst[u.Key], worst[u.Key] == SyncOutcome.Saved ? []
            : [.. failures[u.Key].OrderByDescending(f => f.Outcome).Select(f => f.Error)]))];
    }

    /// <summary>
    /// The objects with changes, each after the objects its records point at (a foreign key). An object whose parent has
    /// no changes of its own goes first: that parent is in Salesforce already.
    /// </summary>
    private static List<string> ParentsFirst(IEnumerable<Change> upserts)
    {
        var parentsOf = upserts.GroupBy(c => c.SObject, StringComparer.OrdinalIgnoreCase).ToDictionary(g => g.Key,
            g => g.SelectMany(c => c.Record.Record.Parents).Select(k => k.SObject)
                .Where(p => !p.Equals(g.Key, StringComparison.OrdinalIgnoreCase)).ToHashSet(StringComparer.OrdinalIgnoreCase),
            StringComparer.OrdinalIgnoreCase);

        var ordered = new List<string>();
        while (ordered.Count < parentsOf.Count)
        {
            ordered.Add(parentsOf.Keys.FirstOrDefault(o => !ordered.Contains(o)
                                                           && parentsOf[o].All(p => ordered.Contains(p) || !parentsOf.ContainsKey(p)))
                        ?? throw new InvalidOperationException(
                            $"Objects point at each other: {string.Join(", ", parentsOf.Keys.Except(ordered))}"));
        }
        return ordered;
    }

    // ---------- 6. Interpret ----------

    /// <summary>
    /// Reads Salesforce's answer for one record: saved, or failed and, if failed, whether retrying can help. Upserts and
    /// deletes are read the same way: a refused delete is permanent or transient by its code like a rejected upsert.
    /// </summary>
    /// <param name="change">The record that was sent.</param>
    /// <param name="result">Salesforce's answer for it; null when it gave none.</param>
    /// <returns>Saved, transient or permanent failure, and the errors (permanent first).</returns>
    internal (SyncOutcome Outcome, List<SalesforceError> Errors) Interpret(Change change, RecordResult? result)
    {
        if (result is null)
        {
            return (SyncOutcome.TransientFailure, [new SalesforceError(change.ReferenceId, "UNKNOWN_ERROR", "Salesforce gave no result for the record")]);
        }

        if (result is { Success: true, Created: true } && !change.IsCreate && !change.IsDelete)
        {
            // An update sent as an upsert found no record by its external id (deleted after the read) and created one with
            // only the changed fields: the retry reads it and writes the rest
            return (SyncOutcome.TransientFailure, [new SalesforceError(change.ReferenceId, "ENTITY_IS_DELETED",
                "The record was gone when the update was upserted; it was created with the changed fields only")]);
        }

        // A record to delete that is gone already (deleted after the read, by someone else or by an earlier attempt whose
        // answer was lost) is what the delete wanted
        var gone = change.IsDelete && result.Errors.Count > 0 && result.Errors.All(SalesforceErrors.IsAlreadyDeleted);
        if (result.Success || gone)
        {
            return (SyncOutcome.Saved, []);
        }

        var failures = result.Errors.Select(e => Translate(change, e)).ToList();
        if (failures.Count == 0)
        {
            failures.Add(new SalesforceError(change.ReferenceId, "UNKNOWN_ERROR", "The record failed without error details"));
        }

        // An update of a record deleted between the read and the write is retried, not dead-lettered: the next read finds
        // it gone and creates it. The update, an upsert, then tries to create the record from the changed fields alone,
        // which fails on a required field the update didn't need to send.
        var permanent = failures.Any(e => errors.IsPermanent(e.Code)
            && !(!change.IsCreate && !change.IsDelete && e.Code is "INVALID_CROSS_REFERENCE_KEY" or "ENTITY_IS_DELETED" or "REQUIRED_FIELD_MISSING"));
        return (permanent ? SyncOutcome.PermanentFailure : SyncOutcome.TransientFailure,
            [.. failures.OrderByDescending(e => errors.IsPermanent(e.Code))]);
    }

    /// <summary>
    /// The org rejecting a record because its foreign key's parent isn't there (a message listed in
    /// <see cref="SalesforceOptions.ParentMissingMessages"/>, whatever the code) is the same situation as the read not
    /// finding it: the parent is expected to arrive through its own sync, so the record waits for it rather than being
    /// dead-lettered. The same in reverse: a delete refused because records still point at it
    /// (<see cref="SalesforceErrors.HasDependents"/>, an Account whose contacts are not deleted yet) waits for their deletes,
    /// which their own inboxes sync. Salesforce rejecting the RecordTypeId (the record type was deactivated or unassigned
    /// after the describe) is an <see cref="SalesforceErrors.InvalidRecordType"/>, like the describe not offering it:
    /// retried, not dead-lettered. Salesforce often leaves <c>fields</c> empty on that error ("Record Type ID: this ID
    /// value isn't valid for the user"), so the message identifies it too.
    /// </summary>
    private SalesforceError Translate(Change change, SalesforceError e)
    {
        if (e.Code is "INVALID_CROSS_REFERENCE_KEY"
            && (e.Fields?.Contains(SObjectSchema.RecordTypeIdField, StringComparer.OrdinalIgnoreCase) == true
                || e.Message?.Contains("Record Type", StringComparison.OrdinalIgnoreCase) == true))
        {
            return new SalesforceError(change.ReferenceId, SalesforceErrors.InvalidRecordType, $"{e.Code}: {e.Message}", e.Fields);
        }

        return errors.IsParentMissing(e) || (change.IsDelete && SalesforceErrors.HasDependents(e))
            ? new SalesforceError(change.ReferenceId, SalesforceErrors.DependencyNotSynced, $"{e.Code}: {e.Message}", e.Fields)
            : e with { ReferenceId = change.ReferenceId };
    }

    private void LogFailures(string entityType, PreparedBatch batch, IEnumerable<SyncResult> results)
    {
        foreach (var invalid in batch.Invalid)
        {
            log.LogWarning("Dead-lettered event {Id} ({Code}): {Error}", invalid.EntityId, invalid.Code, invalid.Error);
        }

        foreach (var r in results.Where(r => r.Outcome is SyncOutcome.PermanentFailure or SyncOutcome.TransientFailure))
        {
            log.LogWarning("{Type} {Key}: {Outcome} ({Code}) {Errors}", entityType, r.Key, r.Outcome,
                r.PrimaryError?.Code, string.Join("; ", r.Errors));
        }
    }
}
