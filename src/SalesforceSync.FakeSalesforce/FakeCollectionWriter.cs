using System.Text.Json.Nodes;
using static SalesforceSync.FakeSalesforce.FakeJson;

namespace SalesforceSync.FakeSalesforce;

/// <summary>
/// sObject Collections create (POST), update (PATCH) and upsert by external id (PATCH .../{SObject}/{field}): each
/// record is checked the way Salesforce checks it (type, id, fields, lookups, uniqueness, injected faults) and gets a
/// result of its own (allOrNone false, the only mode the sync uses). A lookup can also be given by the parent's
/// external id through its relationship (<c>"Account": { "External_Id__c": "A1" }</c>). Not thread-safe;
/// <see cref="FakeSalesforceStore"/> holds its lock.
/// </summary>
internal sealed class FakeCollectionWriter(FakeRecords records, FakeFaults faults, FakeCounters counters)
{
    public const int MaxRecords = 200;

    private const string ExternalIdField = FakeSalesforceStore.ExternalIdField;

    /// <summary>One result per record, in request order.</summary>
    public JsonArray Execute(bool create, JsonObject body)
    {
        return Each(body, record => Write(create, record));
    }

    /// <summary>
    /// Upsert: each record is matched by <paramref name="externalIdField"/>, updated if it exists and created if not.
    /// One result per record, in request order, with <c>created</c>.
    /// </summary>
    public JsonArray Upsert(string sobject, string externalIdField, JsonObject body)
    {
        return Each(body, record => UpsertOne(sobject, externalIdField, record));
    }

    private JsonArray Each(JsonObject body, Func<JsonObject, JsonObject> write)
    {
        var sent = body["records"]?.AsArray() ?? throw new FakeSalesforceException(400, "JSON_PARSER_ERROR", "Missing 'records'");
        if (sent.Count > MaxRecords)
        {
            throw new FakeSalesforceException(400, "LIMIT_EXCEEDED", $"A maximum of {MaxRecords} records is allowed per request");
        }

        var results = new JsonArray();
        foreach (var record in sent)
        {
            results.Add(write(record!.DeepClone().AsObject()));
        }

        var failed = results.Count(r => !r!["success"]!.GetValue<bool>());
        counters.RecordsSucceeded += results.Count - failed;
        counters.RecordsFailed += failed;
        return results;
    }

    /// <summary>Creates or updates one record and returns its result: <c>{ "id", "success", "errors" }</c>.</summary>
    private JsonObject Write(bool create, JsonObject record)
    {
        var sobject = record["attributes"]?["type"]?.GetValue<string>();
        var id = Str(record["id"]) ?? Str(record["Id"]);
        record.Remove("attributes");
        record.Remove("id");
        record.Remove("Id");

        var outcome = sobject is null || !FakeSchema.Objects.ContainsKey(sobject)
            ? (400, Errors("INVALID_TYPE", $"sObject type '{sobject}' is not supported."))
            : !create && id is null
            ? (400, Errors("MISSING_ARGUMENT", "Id not specified in an update call"))
            : create && id is not null
            ? (400, Errors("INVALID_FIELD_FOR_INSERT_UPDATE", "cannot specify Id in an insert call", "Id"))
            : ResolveRelationships(sobject, record) is { } unresolved
            ? (400, unresolved)
            : create ? Create(sobject, record) : Update(sobject, id!, record);

        return Result(outcome, id);
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
            : ResolveRelationships(sobject, record) is { } unresolved
            ? (400, unresolved)
            : existing is null ? Create(sobject, record) : Update(sobject, id!, record);

        var result = Result(outcome, id);
        result["created"] = existing is null && result["success"]!.GetValue<bool>();
        return result;
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

    /// <summary>
    /// Replaces each <c>"Relationship": { "External_Id__c": "A1" }</c> by the lookup field set to that parent's Id.
    /// A parent that doesn't exist is Salesforce's INVALID_FIELD "Foreign key external ID: A1 not found ...".
    /// </summary>
    private JsonNode? ResolveRelationships(string sobject, JsonObject record)
    {
        foreach (var name in record.Where(kv => kv.Value is JsonObject).Select(kv => kv.Key).ToList())
        {
            var parent = record[name]!.AsObject();
            var field = FakeSchema.Objects[sobject].FirstOrDefault(f => string.Equals(f.RelationshipName, name, StringComparison.OrdinalIgnoreCase));
            var keys = parent.Where(kv => kv.Key != "attributes").ToList();
            if (field?.ReferenceTo is not { } target || keys.Count != 1 || FakeSchema.Find(target, keys[0].Key) is not { } key)
            {
                return Errors("INVALID_FIELD", $"No such relationship '{name}' by an external id field on entity '{sobject}'");
            }

            var value = Str(keys[0].Value);
            if (records.Find(target, key.Name, value) is not { } found)
            {
                return Errors("INVALID_FIELD", $"Foreign key external ID: {value} not found for field {key.Name} in entity {target}");
            }

            record.Remove(name);
            record[field.Name] = found["Id"]!.DeepClone();
        }
        return null;
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

        if (MissingLookupTarget(sobject, stored) is { } missingTarget)
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

        if (MissingLookupTarget(sobject, stored) is { } missingTarget)
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
    /// object's that is active and available to the user.
    /// </summary>
    private JsonNode? MissingLookupTarget(string sobject, JsonObject stored)
    {
        foreach (var (name, value) in stored)
        {
            if (Str(value) is not { } id || FakeSchema.Find(sobject, name) is not { Type: "reference", ReferenceTo: { } target })
            {
                continue;
            }

            if (name == FakeSchema.RecordTypeIdField)
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