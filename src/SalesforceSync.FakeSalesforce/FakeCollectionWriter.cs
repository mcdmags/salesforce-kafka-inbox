using System.Text.Json.Nodes;
using static SalesforceSync.FakeSalesforce.FakeJson;

namespace SalesforceSync.FakeSalesforce;

/// <summary>
/// sObject Collections create (POST) and update (PATCH): each record is checked the way Salesforce checks it (type,
/// id, fields, lookups, uniqueness, injected faults) and gets a result of its own; allOrNone rolls the collection
/// back if any record fails. Not thread-safe; <see cref="FakeSalesforceStore"/> holds its lock.
/// </summary>
internal sealed class FakeCollectionWriter(FakeRecords records, FakeFaults faults, FakeCounters counters)
{
    public const int MaxRecords = 200;

    private const string ExternalIdField = FakeSalesforceStore.ExternalIdField;
    private const string RolledBack = "ALL_OR_NONE_OPERATION_ROLLED_BACK";
    private const string RolledBackMessage = "Record rolled back because not all records were valid and the request was using AllOrNone header";

    /// <summary>One result per record, in request order.</summary>
    /// <param name="outerUndo">Receives how to undo what was committed, for a rollback of the whole Composite request.</param>
    public JsonArray Execute(bool create, JsonObject body, List<Action> outerUndo)
    {
        var sent = body["records"]?.AsArray() ?? throw new FakeSalesforceException(400, "JSON_PARSER_ERROR", "Missing 'records'");
        if (sent.Count > MaxRecords)
        {
            throw new FakeSalesforceException(400, "LIMIT_EXCEEDED", $"A maximum of {MaxRecords} records is allowed per request");
        }

        var allOrNone = body["allOrNone"]?.GetValue<bool>() ?? false;

        var results = new JsonArray();
        var undo = new List<Action>();
        foreach (var record in sent)
        {
            results.Add(Write(create, record!.DeepClone().AsObject(), undo));
        }

        var failed = results.Count(r => !r!["success"]!.GetValue<bool>());
        if (allOrNone && failed > 0)
        {
            for (var i = undo.Count - 1; i >= 0; i--)
            {
                undo[i]();
            }

            foreach (var result in results.Where(r => r!["success"]!.GetValue<bool>()))
            {
                MarkRolledBack(result!.AsObject());
            }
        }
        else
        {
            outerUndo.AddRange(undo);
            counters.RecordsSucceeded += results.Count - failed;
            outerUndo.Add(() => counters.RecordsSucceeded -= results.Count - failed);
        }
        counters.RecordsFailed += failed;
        return results;
    }

    /// <summary>A record that had succeeded is reported as rolled back because of another one.</summary>
    public void MarkRolledBack(JsonObject result)
    {
        result["success"] = false;
        result["id"] = null;
        result["errors"] = new JsonArray(new JsonObject { ["statusCode"] = RolledBack, ["message"] = RolledBackMessage, ["fields"] = new JsonArray() });
        counters.RecordsRolledBack++;
    }

    /// <summary>Creates or updates one record and returns its result: <c>{ "id", "success", "errors" }</c>.</summary>
    private JsonObject Write(bool create, JsonObject record, List<Action> undo)
    {
        var sobject = record["attributes"]?["type"]?.GetValue<string>();
        var id = Str(record["id"]) ?? Str(record["Id"]);
        record.Remove("attributes");
        record.Remove("id");
        record.Remove("Id");

        (int Status, JsonNode? Body) outcome;
        if (sobject is null || !FakeSchema.Objects.ContainsKey(sobject))
        {
            outcome = (400, Errors("INVALID_TYPE", $"sObject type '{sobject}' is not supported."));
        }
        else if (!create && id is null)
        {
            outcome = (400, Errors("MISSING_ARGUMENT", "Id not specified in an update call"));
        }
        else if (create && id is not null)
        {
            outcome = (400, Errors("INVALID_FIELD_FOR_INSERT_UPDATE", "cannot specify Id in an insert call", "Id"));
        }
        else
        {
            outcome = ResolveRelationships(sobject, record) is { } missing
                ? ((int Status, JsonNode? Body))(400, missing)
                : ((int Status, JsonNode? Body))(create ? Create(sobject, record, undo) : Update(sobject, id!, record, undo));
        }

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

    private (int, JsonNode?) Create(string sobject, JsonObject body, List<Action> undo)
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
        var remove = records.Insert(sobject, id, record, stored);
        counters.Creates++;
        undo.Add(() =>
        {
            remove();
            counters.Creates--;
        });

        return (201, new JsonObject { ["id"] = id, ["success"] = true, ["errors"] = new JsonArray() });
    }

    private (int, JsonNode?) Update(string sobject, string id, JsonObject body, List<Action> undo)
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
        var restore = records.Update(sobject, id, stored);
        counters.Updates++;
        undo.Add(() =>
        {
            restore();
            counters.Updates--;
        });

        return (204, null);
    }

    /// <summary>
    /// A lookup given as a relationship reference, <c>"Account__r": { "External_Id__c": "ACC-1" }</c>, becomes the
    /// target's Id in the lookup field. Returns the error Salesforce gives when no record has that external id.
    /// </summary>
    private JsonNode? ResolveRelationships(string sobject, JsonObject record)
    {
        foreach (var (name, value) in record.ToList())
        {
            if (value is not JsonObject reference)
            {
                continue;
            }

            var field = FakeSchema.Objects[sobject].FirstOrDefault(f => name.Equals(f.RelationshipName, StringComparison.OrdinalIgnoreCase));
            if (field?.ReferenceTo is null)
            {
                return Errors("INVALID_FIELD", $"No such relationship '{name}' on {sobject}", name);
            }

            var (externalField, externalValue) = reference.Where(kv => kv.Key != "attributes").Select(kv => (kv.Key, Str(kv.Value))).FirstOrDefault();
            var target = externalField is null ? null : records.Find(field.ReferenceTo, externalField, externalValue);
            if (target is null)
            {
                return Errors("INVALID_FIELD",
                    $"Foreign key external ID: {externalValue} not found for field {externalField} in entity {field.ReferenceTo}");
            }

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
            if (Str(value) is not { } id || FakeSchema.Find(sobject, name) is not { Type: "reference", ReferenceTo: { } target })
            {
                continue;
            }

            if (!records.Table(target).ContainsKey(id))
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