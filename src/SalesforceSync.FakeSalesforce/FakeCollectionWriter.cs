using System.Text.Json.Nodes;
using static SalesforceSync.FakeSalesforce.FakeJson;

namespace SalesforceSync.FakeSalesforce;

/// <summary>
/// sObject Collections create (POST) and update (PATCH): each record is checked the way Salesforce checks it (type,
/// id, fields, lookups, uniqueness, injected faults) and gets a result of its own (allOrNone false, the only mode the
/// sync uses). Not thread-safe; <see cref="FakeSalesforceStore"/> holds its lock.
/// </summary>
internal sealed class FakeCollectionWriter(FakeRecords records, FakeFaults faults, FakeCounters counters)
{
    public const int MaxRecords = 200;

    private const string ExternalIdField = FakeSalesforceStore.ExternalIdField;

    /// <summary>One result per record, in request order.</summary>
    public JsonArray Execute(bool create, JsonObject body)
    {
        var sent = body["records"]?.AsArray() ?? throw new FakeSalesforceException(400, "JSON_PARSER_ERROR", "Missing 'records'");
        if (sent.Count > MaxRecords)
        {
            throw new FakeSalesforceException(400, "LIMIT_EXCEEDED", $"A maximum of {MaxRecords} records is allowed per request");
        }

        var results = new JsonArray();
        foreach (var record in sent)
        {
            results.Add(Write(create, record!.DeepClone().AsObject()));
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
            : create ? Create(sobject, record) : Update(sobject, id!, record);

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