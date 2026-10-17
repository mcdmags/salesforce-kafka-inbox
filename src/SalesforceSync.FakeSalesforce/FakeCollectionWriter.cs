using System.Text.Json.Nodes;
using static SalesforceSync.FakeSalesforce.FakeJson;

namespace SalesforceSync.FakeSalesforce;

/// <summary>
/// sObject Collections upsert by external id (PATCH .../composite/sobjects/{SObject}/{field}): each record is checked
/// the way Salesforce checks it (type, fields, lookups, foreign keys, uniqueness, injected faults) and gets a result of
/// its own (allOrNone false, the only mode the sync uses); and delete by Id (<see cref="Delete"/>). Not thread-safe;
/// <see cref="FakeSalesforceStore"/> holds its lock.
/// </summary>
internal sealed class FakeCollectionWriter(FakeRecords records, FakeFaults faults, FakeCounters counters)
{
    public const int MaxRecords = 200;

    private const string ExternalIdField = FakeSalesforceStore.ExternalIdField;

    /// <summary>
    /// Upsert: each record is matched by <paramref name="externalIdField"/>, updated if it exists and created if not.
    /// One result per record, in request order, with <c>created</c>.
    /// </summary>
    public JsonArray Upsert(string sobject, string externalIdField, JsonObject body)
    {
        var sent = body["records"]?.AsArray() ?? throw new FakeSalesforceException(400, "JSON_PARSER_ERROR", "Missing 'records'");
        if (sent.Count > MaxRecords)
        {
            throw new FakeSalesforceException(400, "LIMIT_EXCEEDED", $"A maximum of {MaxRecords} records is allowed per request");
        }

        var results = new JsonArray();
        foreach (var record in sent)
        {
            results.Add(UpsertOne(sobject, externalIdField, record!.DeepClone().AsObject()));
        }

        var failed = results.Count(r => !r!["success"]!.GetValue<bool>());
        counters.RecordsSucceeded += results.Count - failed;
        counters.RecordsFailed += failed;
        return results;
    }

    private JsonObject UpsertOne(string sobject, string externalIdField, JsonObject record)
    {
        var type = record["attributes"]?["type"]?.GetValue<string>();
        record.Remove("attributes");
        var extId = Str(record[externalIdField]);
        var existing = extId is null ? null : records.Find(sobject, externalIdField, extId);
        var id = Str(existing?["Id"]);
        if (existing is not null)
        {
            record.Remove(externalIdField); // the match, not a change
        }

        var outcome = type is not null && !type.Equals(sobject, StringComparison.OrdinalIgnoreCase)
            ? (400, Errors("INVALID_TYPE", $"Record type '{type}' does not match the upsert's sObject {sobject}"))
            : record.ContainsKey("Id") || record.ContainsKey("id")
            ? (400, Errors("INVALID_FIELD", "Id can't be set in an upsert by external id", "Id"))
            : extId is null
            ? (400, Errors("MISSING_ARGUMENT", $"{externalIdField} not specified", externalIdField))
            : existing is null ? Create(sobject, record) : Update(sobject, id!, record);

        var result = Result(outcome, id);
        result["created"] = existing is null && result["success"]!.GetValue<bool>();
        return result;
    }

    /// <summary>
    /// Delete by Id (DELETE .../composite/sobjects?ids=...): the records may be of any objects, and each gets a result of
    /// its own, in request order. A deleted record goes to the Recycle Bin; deleting it again is
    /// <c>ENTITY_IS_DELETED</c>, an Id no record ever had <c>INVALID_CROSS_REFERENCE_KEY</c>. Injected faults apply
    /// (e.g. <c>DELETE_FAILED</c>, a Contact with cases), and so do lost responses.
    /// </summary>
    public JsonArray Delete(IReadOnlyList<string> ids)
    {
        if (ids.Count is 0 or > MaxRecords)
        {
            throw new FakeSalesforceException(400, "LIMIT_EXCEEDED", $"Between 1 and {MaxRecords} ids are allowed per request");
        }

        // A malformed id fails the whole request, not just its record
        if (ids.FirstOrDefault(id => id.Length is not (15 or 18) || !id.All(char.IsAsciiLetterOrDigit)) is { } malformed)
        {
            throw new FakeSalesforceException(400, "MALFORMED_ID", $"malformed id {malformed}");
        }

        var results = new JsonArray();
        foreach (var id in ids)
        {
            results.Add(Result(DeleteOne(id), id));
        }

        var failed = results.Count(r => !r!["success"]!.GetValue<bool>());
        counters.RecordsSucceeded += results.Count - failed;
        counters.RecordsFailed += failed;
        return results;
    }

    private (int Status, JsonNode? Body) DeleteOne(string id)
    {
        if (records.FindById(id) is not var (sobject, record))
        {
            return records.IsDeleted(id)
                ? (400, Errors("ENTITY_IS_DELETED", "entity is deleted"))
                : (400, Errors("INVALID_CROSS_REFERENCE_KEY", $"invalid cross reference id: {id}"));
        }

        var extId = Str(record[ExternalIdField]);
        if (faults.Take(sobject, extId) is { } fault)
        {
            return (400, Errors(fault.Code, fault.Message));
        }

        if (Dependents(sobject, extId) is { Count: > 0 } dependents)
        {
            return (400, Errors("DELETE_FAILED", $"Your attempt to delete {extId} could not be completed because it is associated " +
                $"with the following records: {string.Join(", ", dependents)}"));
        }

        faults.NoteWrite(sobject, extId);
        records.Delete(sobject, id);
        counters.Deletes++;
        return (204, null);
    }

    /// <summary>
    /// The records whose foreign keys (Text fields holding a parent's external id) name this one: the org's automation,
    /// simulated, refuses to delete a parent while they exist, as it refuses a child whose parent is missing
    /// (<see cref="MissingParent"/>). A Text field is no relationship to Salesforce, so nothing cascades.
    /// </summary>
    private List<string> Dependents(string sobject, string? externalId)
    {
        return externalId is null
            ? []
            : [.. FakeSchema.Objects
                .SelectMany(o => o.Value.Where(f => sobject.Equals(f.KeyOf, StringComparison.OrdinalIgnoreCase)).Select(f => (Child: o.Key, Key: f.Name)))
                .SelectMany(k => records.Table(k.Child).Values.Where(r => Str(r[k.Key]) == externalId)
                    .Select(r => $"{k.Child} {Str(r[ExternalIdField])}"))
                .Distinct()];
    }

    /// <summary>A record's result: <c>{ "id", "success", "errors" }</c>, errors as Collections lists them.</summary>
    private static JsonObject Result((int Status, JsonNode? Body) outcome, string? id)
    {
        var ok = outcome.Status < 400;
        return new JsonObject
        {
            ["id"] = ok ? (outcome.Body?["id"]?.GetValue<string>() ?? id) : null,
            ["success"] = ok,
            ["errors"] = ok
                ? []
                : new JsonArray([.. outcome.Body!.AsArray().Select(e => (JsonNode)new JsonObject
                {
                    ["statusCode"] = e!["errorCode"]!.GetValue<string>(),
                    ["message"] = e["message"]?.GetValue<string>(),
                    ["fields"] = e["fields"]?.DeepClone() ?? new JsonArray()
                })])
        };
    }

    private (int Status, JsonNode? Body) Create(string sobject, JsonObject body)
    {
        var extId = Str(body[ExternalIdField]);
        if (faults.Take(sobject, extId) is { } fault)
        {
            return (400, Errors(fault.Code, fault.Message));
        }

        var (stored, error) = StoredValues(sobject, body, creating: true);
        if (stored is null)
        {
            return (400, error);
        }

        if (MissingParent(sobject, stored) is { } missingTarget)
        {
            return (400, missingTarget);
        }

        if (extId is not null && records.Find(sobject, ExternalIdField, extId) is not null)
        {
            return (400, Errors("DUPLICATE_VALUE", $"duplicate value found: {ExternalIdField} duplicates value {extId}"));
        }

        faults.NoteWrite(sobject, extId);
        var id = records.NewId(sobject);
        var record = stored.DeepClone().AsObject();
        record["Id"] = id;
        var now = FakeSchema.FormatDateTime(DateTimeOffset.UtcNow);
        record["CreatedDate"] = now;
        record["LastModifiedDate"] = now;
        records.Insert(sobject, id, record, stored);
        counters.Creates++;

        return (201, new JsonObject { ["id"] = id, ["success"] = true, ["errors"] = new JsonArray() });
    }

    private (int Status, JsonNode? Body) Update(string sobject, string id, JsonObject body)
    {
        if (!records.Table(sobject).TryGetValue(id, out var existing))
        {
            return (400, Errors("INVALID_CROSS_REFERENCE_KEY", $"invalid cross reference id: {id}", "Id"));
        }

        var extId = Str(existing[ExternalIdField]);
        if (faults.Take(sobject, extId) is { } fault)
        {
            return (400, Errors(fault.Code, fault.Message));
        }

        var (stored, error) = StoredValues(sobject, body, creating: false);
        if (stored is null)
        {
            return (400, error);
        }

        if (MissingParent(sobject, stored) is { } missingTarget)
        {
            return (400, missingTarget);
        }

        faults.NoteWrite(sobject, extId);
        records.Update(sobject, id, stored);
        counters.Updates++;

        return (204, null);
    }

    /// <summary>
    /// A lookup must point at a record that exists (and is of the right object); a record type must be one of the
    /// object's that is active and available to the user; a foreign key must hold the external id of a record that
    /// exists (the org's automation, simulated).
    /// </summary>
    private JsonNode? MissingParent(string sobject, JsonObject stored)
    {
        foreach (var (name, value) in stored)
        {
            if (Str(value) is not { } id || FakeSchema.Find(sobject, name) is not { } field)
            {
                continue;
            }

            if (field.KeyOf is { } parent)
            {
                if (records.Find(parent, ExternalIdField, id) is null)
                {
                    return Errors(FakeSchema.ParentMissingCode, $"{FakeSchema.ParentMissingMessage}: {parent} {ExternalIdField}={id}", name);
                }
            }
            else if (field is not { Type: "reference", ReferenceTo: { } target })
            {
                continue;
            }
            else if (name == FakeSchema.RecordTypeIdField)
            {
                if (!FakeSchema.IsAssignableRecordType(sobject, id))
                {
                    return Errors("INVALID_CROSS_REFERENCE_KEY", $"Record Type ID: this ID value isn't valid for the user: {id}", name);
                }
            }
            else if (!records.Table(target).ContainsKey(id))
            {
                return Errors("INVALID_CROSS_REFERENCE_KEY", $"invalid cross reference id: {id}", name);
            }
        }
        return null;
    }

    /// <summary>The values Salesforce would store for the body, or the error it would return.</summary>
    private static (JsonObject? Stored, JsonNode? Error) StoredValues(string sobject, JsonObject body, bool creating)
    {
        var stored = new JsonObject();
        foreach (var (name, value) in body)
        {
            if (FakeSchema.Find(sobject, name) is not { } field)
            {
                return (null, Errors("INVALID_FIELD", $"No such column '{name}' on sobject of type {sobject}"));
            }

            if (creating ? !field.Createable : !field.Updateable)
            {
                return (null, Errors("INVALID_FIELD_FOR_INSERT_UPDATE",
                    $"Unable to create/update fields: {field.Name}. Please check the security settings of this field."));
            }

            var (v, code, message) = FakeSchema.Store(field, value);
            if (code is not null)
            {
                return (null, Errors(code, message!));
            }

            stored[field.Name] = v;
        }
        return (stored, null);
    }
}