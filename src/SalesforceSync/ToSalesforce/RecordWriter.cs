using System.Text.Json;
using SalesforceSync.Data.Inbox;
using SalesforceSync.Infrastructure.Salesforce;
using SalesforceSync.Sync;

namespace SalesforceSync.ToSalesforce;

/// <summary>
/// The Salesforce half of a batch, the same for every entity type: given the records each entity wants, it reads what
/// Salesforce has, sends what differs and says, per entity, what went wrong. Nothing here touches the inbox, and nothing
/// names an object: the types only say which <see cref="Rec"/>s their events ask for.
/// <list type="number">
/// <item><b>Record types</b>: Salesforce takes a record type as an Id, which differs between orgs: look it up (cached).</item>
/// <item><b>Read</b>: one query per object for every record we'll write, delete or point at (its Id, current values).</item>
/// <item><b>Decide</b>, per entity: create if Salesforce lacks the record, update the fields that differ, nothing if none
/// does, delete by Id if it is there. A record whose parent isn't in Salesforce waits.</item>
/// <item><b>Write</b>: one upsert call per object (parents' objects first), then one delete call.</item>
/// <item><b>Interpret</b> each record's answer: saved, or failed, and whether retrying can help.</item>
/// </list>
/// </summary>
public sealed class RecordWriter(ISalesforceRestClient salesforce, RecordTypeCache recordTypes, SalesforceErrors errors)
{
    /// <summary>The field every object is matched by.</summary>
    public const string ExternalIdField = "ExternalId__c";

    /// <summary>A record to send or delete: the decision for one <see cref="Rec"/>.</summary>
    private sealed record Change(string Key, Rec Rec, string? Id, Dictionary<string, object?> Fields)
    {
        public bool IsCreate => !Rec.Delete && Id is null;
    }

    /// <summary>One entity's records with their values in canonical form and the record type as an Id.</summary>
    private sealed record Item(Rec Rec, Dictionary<string, object?> Fields);

    /// <summary>Maps each event to its records, then sends them.</summary>
    /// <param name="events">The batch's valid events.</param>
    /// <param name="map">An event (and its entity's id) -> the records it asks for, in write order; throws if it can't be mapped.</param>
    /// <param name="maxValuesPerQuery">The row type's <see cref="IInboxRow.MaxValuesPerQuery"/>.</param>
    /// <param name="maxRecordsPerWrite">The row type's <see cref="IInboxRow.MaxRecordsPerWrite"/>.</param>
    /// <param name="ct">Cancels the calls; shutdown or the lease deadline, not a failure.</param>
    /// <returns>The entities that failed, with why. An entity not in it is done: written, or already as wanted.</returns>
    /// <exception cref="UnknownOutcomeException">A write call failed without proving that Salesforce didn't apply it.</exception>
    /// <exception cref="MappingErrorException">Salesforce lacks a field we send, or won't let us set it.</exception>
    public async Task<Dictionary<string, List<Failure>>> WriteAsync<TEvent>(IReadOnlyList<ReadEvent<TEvent>> events,
        Func<string, TEvent, IReadOnlyList<Rec>> map, int maxValuesPerQuery, int maxRecordsPerWrite, CancellationToken ct)
    {
        var failures = new Dictionary<string, List<Failure>>();

        // An event that can't be mapped is invalid on its own: retrying can't fix it
        var entities = new List<(string Key, IReadOnlyList<Rec> Records)>();
        foreach (var e in events)
        {
            try
            {
                entities.Add((e.EntityId, map(e.EntityId, e.Event)));
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                Fail(failures, e.EntityId, new SalesforceError(null, SalesforceErrors.InvalidEvent, ex.Message), permanent: true);
            }
        }

        // 1. RECORD TYPES. An entity whose record type the org lacks fails alone
        var items = new List<(string Key, List<Item> Items)>();
        foreach (var (key, records) in entities)
        {
            if (await ItemsOfAsync(key, records, failures, ct) is { } entityItems)
            {
                items.Add((key, entityItems));
            }
        }

        // 2. READ
        var current = await ReadAsync(items, maxValuesPerQuery, ct);

        // 3. DECIDE
        var changes = new List<Change>();
        foreach (var (key, entityItems) in items)
        {
            changes.AddRange(Decide(key, entityItems, current, failures));
        }

        // 4. WRITE and 5. INTERPRET. Upserts, one call per object in the order objects first appear (parents first),
        // then the deletes in one call. An entity's record is not sent once an earlier record of the entity failed: it
        // depends on it (a Profile on its Account; an Account's delete on its Profile's). From the first write on, a
        // lost answer means Salesforce may have the write
        var byObject = changes.Where(c => !c.Rec.Delete).GroupBy(c => c.Rec.SObject).ToList();
        foreach (var group in byObject)
        {
            foreach (var chunk in group.Where(c => !failures.ContainsKey(c.Key)).Chunk(maxRecordsPerWrite))
            {
                var bodies = chunk.Select(Body).ToList();
                var results = await Guard(() => salesforce.UpsertAsync(group.Key, ExternalIdField, bodies, ct));
                Interpret(chunk, results, failures);
            }
        }

        foreach (var chunk in changes.Where(c => c.Rec.Delete && !failures.ContainsKey(c.Key)).Chunk(maxRecordsPerWrite))
        {
            var results = await Guard(() => salesforce.DeleteAsync([.. chunk.Select(c => c.Id!)], ct));
            Interpret(chunk, results, failures);
        }

        ThrowIfMappingErrors(failures);
        await ForgetRejectedRecordTypesAsync(items, failures, ct);
        return failures;
    }

    // ---------- 1. Record types ----------

    private async Task<List<Item>?> ItemsOfAsync(string key, IReadOnlyList<Rec> records, Dictionary<string, List<Failure>> failures, CancellationToken ct)
    {
        var items = new List<Item>(records.Count);
        foreach (var rec in records)
        {
            var fields = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            foreach (var (name, value) in rec.Fields)
            {
                fields[name] = FieldValues.Normalize(value);
            }

            if (rec.RecordType is { } developerName)
            {
                if (await recordTypes.IdOfAsync(rec.SObject, developerName, ct) is not { } id)
                {
                    Fail(failures, key, new SalesforceError(null, SalesforceErrors.InvalidRecordType,
                        $"Record type '{developerName}' of {rec.SObject} doesn't exist or is not active", [RecordTypeCache.RecordTypeIdField]));
                    return null;
                }

                fields[RecordTypeCache.RecordTypeIdField] = id;
            }

            items.Add(new Item(rec, fields));
        }

        return items;
    }

    // ---------- 2. Read ----------

    /// <summary>What Salesforce has for every record we'll touch or point at, by object and external id.</summary>
    private async Task<Dictionary<(string SObject, string ExternalId), JsonElement>> ReadAsync(
        List<(string Key, List<Item> Items)> items, int maxValuesPerQuery, CancellationToken ct)
    {
        var all = items.SelectMany(e => e.Items).ToList();
        var current = new Dictionary<(string, string), JsonElement>();
        foreach (var sobject in all.Select(i => i.Rec.SObject).Concat(all.SelectMany(i => i.Rec.Parents ?? []).Select(p => p.SObject))
                     .Distinct(StringComparer.OrdinalIgnoreCase))
        {
            // Every field we compare, the external id the rows are matched by, and the Id (a delete needs it)
            var fields = all.Where(i => i.Rec.SObject == sobject).SelectMany(i => i.Fields.Keys)
                .Append(ExternalIdField).Append("Id").Distinct(StringComparer.OrdinalIgnoreCase).ToList();
            var ids = all.Where(i => i.Rec.SObject == sobject).Select(i => i.Rec.ExternalId)
                .Concat(all.SelectMany(i => i.Rec.Parents ?? []).Where(p => p.SObject == sobject).Select(p => p.ExternalId))
                .Distinct(StringComparer.Ordinal).ToList();

            try
            {
                foreach (var chunk in ids.Chunk(maxValuesPerQuery))
                {
                    foreach (var row in await SelectAsync(sobject, fields, chunk, ct))
                    {
                        current[(sobject, row.GetProperty(ExternalIdField).GetString()!)] = row;
                    }
                }
            }
            catch (SalesforceResponseException ex) when (ex.ErrorCode == "INVALID_FIELD")
            {
                // The org has no such field: the mapping is wrong for every entity that has this object
                throw new MappingErrorException([.. items.Where(e => e.Items.Any(i => i.Rec.SObject == sobject))
                    .Select(e => (e.Key, new SalesforceError(null, "INVALID_FIELD", ex.Message)))]);
            }
        }

        return current;
    }

    /// <summary>One query for these external ids; two when the URL would be too long for one.</summary>
    private async Task<List<JsonElement>> SelectAsync(string sobject, List<string> fields, string[] ids, CancellationToken ct)
    {
        var soql = SalesforceRestClient.Soql(sobject, fields, ExternalIdField, ids);
        if (ids.Length > 1 && !SalesforceRestClient.FitsUrlLimit(SalesforceRestClient.QueryUrl(salesforce.DataPath, soql)))
        {
            var half = ids.Length / 2;
            return [.. await SelectAsync(sobject, fields, ids[..half], ct), .. await SelectAsync(sobject, fields, ids[half..], ct)];
        }

        return await salesforce.SelectAsync(soql, ct);
    }

    // ---------- 3. Decide ----------

    /// <summary>
    /// What one entity needs sent. An entity with a parent missing sends nothing and waits (transient): the parent is
    /// expected to arrive through its own sync.
    /// </summary>
    private static List<Change> Decide(string key, List<Item> items, Dictionary<(string SObject, string ExternalId), JsonElement> current,
        Dictionary<string, List<Failure>> failures)
    {
        var changes = new List<Change>();
        var written = items.Where(i => !i.Rec.Delete).Select(i => (i.Rec.SObject, i.Rec.ExternalId)).ToHashSet();
        var waiting = false;
        foreach (var (rec, fields) in items)
        {
            var existing = current.TryGetValue((rec.SObject, rec.ExternalId), out var row) ? row : default;
            var exists = existing.ValueKind == JsonValueKind.Object;

            if (rec.Delete)
            {
                // A record Salesforce doesn't have is already deleted
                if (exists)
                {
                    changes.Add(new Change(key, rec, existing.GetProperty("Id").GetString(), []));
                }

                continue;
            }

            foreach (var parent in (rec.Parents ?? []).Where(p => !written.Contains((p.SObject, p.ExternalId)) && !current.ContainsKey((p.SObject, p.ExternalId))))
            {
                waiting = true;
                Fail(failures, key, new SalesforceError(null, SalesforceErrors.DependencyNotSynced,
                    $"{rec.SObject}.{parent.Field}: {parent.SObject} {ExternalIdField}={parent.ExternalId} is not in Salesforce yet"));
            }

            // A new record gets its non-empty fields (a null would override a default); an existing one only what differs
            var send = fields.Where(f => exists ? !(existing.TryGetProperty(f.Key, out var now) && FieldValues.AreEqual(f.Value, now)) : f.Value is not null)
                .ToDictionary(f => f.Key, f => FieldValues.ToWire(f.Value));
            if (!exists || send.Count > 0)
            {
                changes.Add(new Change(key, rec, exists ? existing.GetProperty("Id").GetString() : null, send));
            }
        }

        return waiting ? [] : changes;
    }

    // ---------- 4. Write ----------

    /// <summary>The record as an upsert takes it: its object, its external id (which decides create or update) and the fields to send.</summary>
    private static SObjectBody Body(Change change)
    {
        var body = new SObjectBody(change.Rec.SObject) { [ExternalIdField] = change.Rec.ExternalId };
        foreach (var (field, value) in change.Fields)
        {
            body[field] = value;
        }

        return body;
    }

    /// <summary>Runs a write. If it fails without proving Salesforce didn't apply it, the outcome is unknown.</summary>
    private static async Task<IReadOnlyList<RecordResult>> Guard(Func<Task<IReadOnlyList<RecordResult>>> write)
    {
        try
        {
            return await write();
        }
        catch (Exception ex) when (WriteOutcome.IsUnknown(ex))
        {
            throw new UnknownOutcomeException(ex);
        }
    }

    // ---------- 5. Interpret ----------

    /// <summary>One result per change, in order: saved, or a failure for the change's entity.</summary>
    private void Interpret(Change[] chunk, IReadOnlyList<RecordResult> results, Dictionary<string, List<Failure>> failures)
    {
        for (var i = 0; i < chunk.Length; i++)
        {
            var change = chunk[i];
            var result = i < results.Count ? results[i] : null;

            if (result is null)
            {
                Fail(failures, change.Key, new SalesforceError(null, "UNKNOWN_ERROR", "Salesforce gave no result for the record"));
            }
            else if (result is { Success: true, Created: true } && !change.IsCreate && !change.Rec.Delete)
            {
                // An update found no record by its external id (deleted after the read) and created one with only the
                // changed fields. The retry reads it and writes the rest
                Fail(failures, change.Key, new SalesforceError(null, "ENTITY_IS_DELETED",
                    "The record was gone when the update was upserted; it was created with the changed fields only"));
            }
            else if (!result.Success && !(change.Rec.Delete && result.Errors.Count > 0 && result.Errors.All(SalesforceErrors.IsAlreadyDeleted)))
            {
                // (a delete of a record that is gone already is what the delete wanted)
                foreach (var error in result.Errors.DefaultIfEmpty(new SalesforceError(null, "UNKNOWN_ERROR", "The record failed without error details")))
                {
                    var translated = Translate(change, error);
                    Fail(failures, change.Key, translated, IsPermanent(change, translated));
                }
            }
        }
    }

    /// <summary>
    /// Our own codes for what retrying can fix by waiting: a parent not there (an org rule's message, whatever the code),
    /// records still pointing at a record to delete (their own deletes come through their inboxes), and a record type
    /// that was deactivated since its Id was read.
    /// </summary>
    private SalesforceError Translate(Change change, SalesforceError e)
    {
        var recordType = e.Code is "INVALID_CROSS_REFERENCE_KEY"
                         && (e.Fields?.Contains(RecordTypeCache.RecordTypeIdField, StringComparer.OrdinalIgnoreCase) == true
                             || e.Message?.Contains("Record Type", StringComparison.OrdinalIgnoreCase) == true);

        return recordType ? new SalesforceError(null, SalesforceErrors.InvalidRecordType, $"{e.Code}: {e.Message}", e.Fields)
            : errors.IsParentMissing(e) || (change.Rec.Delete && SalesforceErrors.HasDependents(e))
                ? new SalesforceError(null, SalesforceErrors.DependencyNotSynced, $"{e.Code}: {e.Message}", e.Fields)
                : e;
    }

    /// <summary>
    /// Whether retrying the same data fails the same way. Except for an update: its record may have been deleted since the
    /// read, and the next read finds it gone and creates it whole, so these codes are retried.
    /// </summary>
    private bool IsPermanent(Change change, SalesforceError error)
    {
        return errors.IsPermanent(error.Code)
               && !(!change.IsCreate && !change.Rec.Delete && error.Code is "INVALID_CROSS_REFERENCE_KEY" or "ENTITY_IS_DELETED" or "REQUIRED_FIELD_MISSING");
    }

    /// <summary>
    /// A field the code sends that Salesforce lacks or won't let us set: the mapping is wrong for every event of that
    /// kind, so only a code change or a field in the org fixes it. The caller stops the inbox rather than spend each
    /// entity's attempts.
    /// </summary>
    private static void ThrowIfMappingErrors(Dictionary<string, List<Failure>> failures)
    {
        var found = failures.SelectMany(f => f.Value.Where(v => v.Error.Code is "INVALID_FIELD" or "INVALID_FIELD_FOR_INSERT_UPDATE").Select(v => (f.Key, v.Error))).ToList();
        if (found.Count > 0)
        {
            throw new MappingErrorException(found);
        }
    }

    /// <summary>A record type the org rejected may since have been created, activated or assigned: read it again next time.</summary>
    private async Task ForgetRejectedRecordTypesAsync(List<(string Key, List<Item> Items)> items, Dictionary<string, List<Failure>> failures, CancellationToken ct)
    {
        foreach (var sobject in items.Where(e => failures.TryGetValue(e.Key, out var f) && f.Any(x => x.Error.Code == SalesforceErrors.InvalidRecordType))
                     .SelectMany(e => e.Items.Select(i => i.Rec.SObject)).Distinct(StringComparer.OrdinalIgnoreCase))
        {
            await recordTypes.InvalidateAsync(sobject, ct);
        }
    }

    private static void Fail(Dictionary<string, List<Failure>> failures, string key, SalesforceError error, bool permanent = false)
    {
        if (!failures.TryGetValue(key, out var list))
        {
            failures[key] = list = [];
        }

        list.Add(new Failure(error, permanent));
    }
}

/// <summary>Entities whose mapping Salesforce rejects, with what to fix.</summary>
public sealed class MappingErrorException(IReadOnlyList<(string Key, SalesforceError Error)> errors)
    : Exception($"{errors.Count} mapping error(s), e.g. {errors[0].Error.Code}: {errors[0].Error.Message}")
{
    public IReadOnlyList<(string Key, SalesforceError Error)> Errors => errors;
}
