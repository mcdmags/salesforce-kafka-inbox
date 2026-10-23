using System.Text.Json.Nodes;
using static SalesforceSync.FakeSalesforce.FakeJson;

namespace SalesforceSync.FakeSalesforce;

/// <summary>
/// sObject Collections upsert by external id (PATCH .../composite/sobjects/{SObject}/{field}): each record is checked
/// as Salesforce checks it (type, fields, lookups, foreign keys, uniqueness, injected faults) and gets its own result;
/// and delete by Id (<see cref="Delete"/>). With allOrNone true, one failed record rolls back the others.
/// Not thread-safe; <see cref="FakeSalesforceStore"/> holds its lock.
/// </summary>
internal sealed class FakeCollectionWriter(FakeRecords records, FakeFaults faults, FakeCounters counters, FakeSalesforceOptions options)
{
    public const int MaxRecords = 200;

    private const string ExternalIdField = FakeSalesforceStore.ExternalIdField;

    /// <summary>
    /// Upsert: each record is matched by <paramref name="externalIdField"/>, updated if it exists, else created.
    /// One result per record, in request order, with <c>created</c>. Records of one request that share an external id
    /// all fail and none is written (SOAP API Developer Guide, upsert(): "those records are marked as errors").
    /// </summary>
    public JsonArray Upsert(string sobject, string externalIdField, JsonObject body)
    {
        var sent = body["records"]?.AsArray() ?? throw new FakeSalesforceException(400, "JSON_PARSER_ERROR", "Missing 'records'");
        if (sent.Count > MaxRecords)
        {
            throw TooMany();
        }

        var shared = sent.Select(r => Str(r?[externalIdField])).OfType<string>()
            .GroupBy(id => id, StringComparer.Ordinal).Where(g => g.Count() > 1).Select(g => g.Key).ToHashSet(StringComparer.Ordinal);
        return Write(IsTrue(body["allOrNone"]), () => new JsonArray([.. sent.Select(record =>
            Str(record?[externalIdField]) is { } id && shared.Contains(id)
                ? Upserted(Result((400, Errors("DUPLICATE_VALUE", $"Duplicate external id specified: {id}", externalIdField)), null), false)
                : UpsertOne(sobject, externalIdField, record!.DeepClone().AsObject()))]));
    }

    /// <summary>
    /// Delete by Id (DELETE .../composite/sobjects?ids=...): records may be of any objects, each gets its own result in
    /// request order; a malformed Id is one of them, without an <c>id</c> (REST API Developer Guide,
    /// "Delete Records Using sObject Collections"). A deleted record goes to the Recycle Bin; deleting it again is
    /// <c>ENTITY_IS_DELETED</c>, an Id no record ever had <c>INVALID_CROSS_REFERENCE_KEY</c>. Injected faults apply
    /// (e.g. <c>DELETE_FAILED</c>, a Contact with cases), and so do lost responses.
    /// </summary>
    public JsonArray Delete(IReadOnlyList<string> ids, bool allOrNone)
    {
        return ids.Count == 0
            ? throw new FakeSalesforceException(400, "MISSING_ARGUMENT", "ids not specified")
            : ids.Count > MaxRecords
            ? throw TooMany()
            : Write(allOrNone, () => new JsonArray([.. ids.Select(id => FakeRecords.WellFormedId(id) is { } valid
                ? Result(DeleteOne(valid), valid)
                : Result((400, Errors("MALFORMED_ID", $"malformed id {id}")), null))]));
    }

    /// <summary>A Collections allOrNone: a JSON boolean in a body, "true" or "false" in a URL; absent is false.</summary>
    public static bool IsTrue(JsonNode? allOrNone)
    {
        return allOrNone is JsonValue v && (v.TryGetValue<bool>(out var b) ? b : v.TryGetValue<string>(out var s) && bool.TryParse(s, out b) && b);
    }

    /// <summary>
    /// Runs a collection's writes. With <paramref name="allOrNone"/>, a failed record undoes the others, which then fail
    /// with <c>ALL_OR_NONE_OPERATION_ROLLED_BACK</c>; the answer is still 200 with one result per record (REST API
    /// Developer Guide, "Some Items Failed and allOrNone is true").
    /// </summary>
    private JsonArray Write(bool allOrNone, Func<JsonArray> write)
    {
        var saved = allOrNone ? records.Save() : null;
        var (creates, updates, deletes) = (counters.Creates, counters.Updates, counters.Deletes);
        var results = write();
        if (saved is not null && results.Any(Failed))
        {
            records.Restore(saved);
            (counters.Creates, counters.Updates, counters.Deletes) = (creates, updates, deletes);
            foreach (var result in results.Where(r => !Failed(r)))
            {
                result!["success"] = false;
                result["errors"] = new JsonArray(ToRecordError(Errors("ALL_OR_NONE_OPERATION_ROLLED_BACK",
                    "Record rolled back because not all records were valid and the request was using AllOrNone header")[0]));
                if (result.AsObject().ContainsKey("created"))
                {
                    result["created"] = false;
                }
            }
        }

        var failed = results.Count(Failed);
        counters.RecordsSucceeded += results.Count - failed;
        counters.RecordsFailed += failed;
        return results;
    }

    private static bool Failed(JsonNode? result)
    {
        return !result!["success"]!.GetValue<bool>();
    }

    /// <summary>
    /// More than 200 records or Ids fail the whole request. The code is the SOAP API's for too many Ids in a call
    /// (Core Data Types, StatusCode), which client libraries report for sObject Collections too.
    /// </summary>
    private static FakeSalesforceException TooMany()
    {
        return new FakeSalesforceException(400, "EXCEEDED_ID_LIMIT",
            $"record limit reached. cannot submit more than {MaxRecords} records into this call");
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

        // A failed upsert has no id, even for a record that exists (SOAP API, UpsertResult: "If there was an error, the field is null")
        var result = Result(outcome, outcome.Item1 < 400 ? id : null);
        return Upserted(result, existing is null && !Failed(result));
    }

    private static JsonObject Upserted(JsonObject result, bool created)
    {
        result["created"] = created;
        return result;
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

        if (options.RefuseDeletingReferencedRecords && Dependents(sobject, extId) is { Count: > 0 } dependents)
        {
            return (400, Errors("DELETE_FAILED", $"Your attempt to delete {extId} could not be completed because it is associated " +
                $"with the following records: {string.Join(", ", dependents)}"));
        }

        faults.NoteWrite(sobject, extId);
        records.Delete(sobject, id);
        counters.Deletes++;
        // A lookup that cascades takes its records along (a Text foreign key never does)
        foreach (var (child, lookup) in FakeSchema.Objects.SelectMany(o => o.Value
                     .Where(f => f.CascadeDelete && sobject.Equals(f.ReferenceTo, StringComparison.OrdinalIgnoreCase)).Select(f => (o.Key, f.Name))))
        {
            foreach (var childId in records.Table(child).Where(r => Str(r.Value[lookup]) == id).Select(r => r.Key).ToList())
            {
                records.Delete(child, childId);
                counters.Deletes++;
            }
        }

        return (204, null);
    }

    /// <summary>
    /// Records whose foreign keys (Text fields holding a parent's external id) name this one. With
    /// <see cref="FakeSalesforceOptions.RefuseDeletingReferencedRecords"/> the simulated org automation refuses to
    /// delete a parent while they exist, as it refuses a child whose parent is missing (<see cref="MissingParent"/>).
    /// A Text field is no relationship to Salesforce, so nothing cascades either way.
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

    /// <summary>
    /// A record's result: <c>{ "id", "success", "errors" }</c>, errors as Collections lists them. Without an Id (failed
    /// upsert, malformed Id) it has no <c>id</c>, as in the guide's examples; a failed delete keeps its Id.
    /// </summary>
    private static JsonObject Result((int Status, JsonNode? Body) outcome, string? id)
    {
        var ok = outcome.Status < 400;
        var result = new JsonObject();
        var recordId = ok ? outcome.Body?["id"]?.GetValue<string>() ?? id : id;
        if (recordId is not null)
        {
            result["id"] = recordId;
        }

        result["success"] = ok;
        result["errors"] = ok ? [] : new JsonArray([.. outcome.Body!.AsArray().Select(ToRecordError)]);
        return result;
    }

    /// <summary>An error of an error response (<c>errorCode</c>) as a record's error in a collection (<c>statusCode</c>).</summary>
    private static JsonNode ToRecordError(JsonNode? e)
    {
        return new JsonObject
        {
            ["statusCode"] = e!["errorCode"]!.GetValue<string>(),
            ["message"] = e["message"]?.GetValue<string>(),
            ["fields"] = e["fields"]?.DeepClone() ?? new JsonArray()
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

        if (PersonOrBusiness(sobject, stored, FakeSchema.IsPersonRecordType(sobject, Str(stored[FakeSchema.RecordTypeIdField])), creating: true) is { } kindError)
        {
            return (400, kindError);
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
        SetPersonName(record);
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

        if (PersonOrBusiness(sobject, stored, existing[PersonAccountField]?.GetValue<bool>() == true, creating: false) is { } kindError)
        {
            return (400, kindError);
        }

        if (MissingParent(sobject, stored) is { } missingTarget)
        {
            return (400, missingTarget);
        }

        faults.NoteWrite(sobject, extId);
        records.Update(sobject, id, stored);
        SetPersonName(existing);
        counters.Updates++;

        return (204, null);
    }

    /// <summary>
    /// A lookup must point at an existing record of the right object; a record type must be one of the object's that
    /// is active and available to the user; a foreign key must hold the external id of an existing record (the org's
    /// automation, simulated).
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

    private const string PersonAccountField = "IsPersonAccount";
    private static readonly string[] PersonOnlyFields = ["FirstName", "LastName"];

    /// <summary>
    /// An Account is a person account (its record type is a person one) or a business account, and each takes its own
    /// name fields: a business one Name, a person one FirstName and LastName (LastName required), from which Salesforce
    /// sets Name. The other kind's fields are refused as Salesforce refuses them. A create's IsPersonAccount is set on
    /// <paramref name="stored"/>.
    /// </summary>
    private static JsonNode? PersonOrBusiness(string sobject, JsonObject stored, bool person, bool creating)
    {
        if (!sobject.Equals("Account", StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }

        var wrong = (person ? ["Name"] : PersonOnlyFields).Where(stored.ContainsKey).ToArray();
        if (wrong.Length > 0)
        {
            return Errors("INVALID_FIELD_FOR_INSERT_UPDATE", $"Account: bad field names on insert/update call: {string.Join(", ", wrong)}", wrong);
        }

        if (person && (creating || stored.ContainsKey("LastName")) && string.IsNullOrEmpty(Str(stored["LastName"])))
        {
            return Errors("REQUIRED_FIELD_MISSING", "Required fields are missing: [LastName]", "LastName");
        }

        if (creating)
        {
            stored[PersonAccountField] = person;
        }
        return null;
    }

    /// <summary>A person account's Name, which Salesforce keeps as its first and last name.</summary>
    private static void SetPersonName(JsonObject record)
    {
        if (record[PersonAccountField]?.GetValue<bool>() == true)
        {
            record["Name"] = $"{Str(record["FirstName"])} {Str(record["LastName"])}".Trim();
        }
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

        // A create has to give every required field a value, as a real org insists (a checkbox is never missing: it is
        // false). A person account has no Name of its own: Salesforce sets it from FirstName and LastName
        var person = sobject.Equals("Account", StringComparison.OrdinalIgnoreCase) && FakeSchema.IsPersonRecordType(sobject, Str(stored[FakeSchema.RecordTypeIdField]));
        if (creating && FakeSchema.Objects[sobject].Where(f => f is { Nillable: false, Createable: true, Type: not "boolean" } && !(person && f.Name == "Name"))
                .Where(f => stored[f.Name] is null).Select(f => f.Name).ToList() is { Count: > 0 } missing)
        {
            return (null, Errors("REQUIRED_FIELD_MISSING", $"Required fields are missing: [{string.Join(", ", missing)}]"));
        }
        return (stored, null);
    }
}