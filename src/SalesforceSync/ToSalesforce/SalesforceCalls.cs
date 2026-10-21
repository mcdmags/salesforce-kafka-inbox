using System.Reflection;
using System.Text.Json;
using SalesforceSync.Infrastructure.Salesforce;

namespace SalesforceSync.ToSalesforce;

/// <summary>
/// A Salesforce object and the external-id field its records are matched by. Each object has its own: Salesforce sets
/// no common name, so an org may use e.g. <c>Legacy_Id__c</c> on Account and <c>Source_Id__c</c> on Contact. Declared
/// on each record class (<see cref="SObjectAttribute"/>), read with <see cref="Of{T}"/>.
/// </summary>
/// <param name="Name">Object API name, e.g. <c>Account</c>.</param>
/// <param name="ExternalIdField">API name of its external-id field, e.g. <c>ExternalId__c</c>.</param>
public sealed record SalesforceObject(string Name, string ExternalIdField)
{
    /// <summary>The object the record class <typeparamref name="T"/> declares.</summary>
    public static SalesforceObject Of<T>()
    {
        var sobject = typeof(T).GetCustomAttribute<SObjectAttribute>()
                      ?? throw new InvalidOperationException($"{typeof(T).Name} has no [SObject]");
        return new SalesforceObject(sobject.Name, sobject.ExternalIdField);
    }
}

/// <summary>
/// Salesforce calls the types' send steps are made of: each one API call (a few for big batches), no decisions about
/// what to write (type's job). Record failures go in its entity's <c>failures</c> as <see cref="Failure"/>s.
/// </summary>
public sealed class SalesforceCalls(ISalesforceRestClient salesforce, RecordTypeCache recordTypes, SalesforceErrors errors)
{
    /// <summary>Id of an object's active record type by developer name (Salesforce takes an Id). Null if org has none.</summary>
    public Task<string?> RecordTypeIdAsync(SalesforceObject sobject, string developerName, CancellationToken ct)
    {
        return recordTypes.IdOfAsync(sobject.Name, developerName, ct);
    }

    /// <summary>
    /// Current Salesforce records for these external ids: fields, Id (deletes need it), external id. Missing record =
    /// absent from result.
    /// </summary>
    /// <exception cref="MappingErrorException">Org lacks such a field on the object.</exception>
    public async Task<Dictionary<string, JsonElement>> ReadAsync(SalesforceObject sobject, IEnumerable<string> fields, IEnumerable<string> externalIds,
        CancellationToken ct)
    {
        var columns = fields.Append(sobject.ExternalIdField).Append("Id").Distinct(StringComparer.OrdinalIgnoreCase).ToList();
        var found = new Dictionary<string, JsonElement>(StringComparer.Ordinal);
        foreach (var (soql, ids) in Queries(sobject, columns, externalIds))
        {
            try
            {
                foreach (var row in await salesforce.SelectAsync(soql, ct))
                {
                    found[row.GetProperty(sobject.ExternalIdField).GetString()!] = row;
                }
            }
            catch (SalesforceResponseException ex) when (ex.ErrorCode == "INVALID_FIELD")
            {
                // Org lacks field: mapping wrong for every entity of the type
                throw new MappingErrorException([.. ids.Select(id => (id, new SalesforceError(null, "INVALID_FIELD", ex.Message)))]);
            }
        }

        return found;
    }

    /// <summary>
    /// 200 ids per query, halved while the query's URL is over Salesforce's limit: the query is a GET, so long external
    /// ids can overflow its URL long before 200 of them.
    /// </summary>
    private IEnumerable<(string Soql, string[] Ids)> Queries(SalesforceObject sobject, List<string> columns, IEnumerable<string> externalIds)
    {
        var pending = new Stack<string[]>(externalIds.Distinct(StringComparer.Ordinal).Chunk(SalesforceRestClient.MaxCollectionRecords).Reverse());
        while (pending.TryPop(out var ids))
        {
            var soql = SalesforceRestClient.Soql(sobject.Name, columns, sobject.ExternalIdField, ids);
            if (ids.Length > 1 && !SalesforceRestClient.FitsUrlLimit(SalesforceRestClient.QueryUrl(salesforce.DataPath, soql)))
            {
                pending.Push(ids[(ids.Length / 2)..]);
                pending.Push(ids[..(ids.Length / 2)]);
                continue;
            }

            yield return (soql, ids);
        }
    }

    /// <summary>
    /// What to send: new record → non-empty fields; existing → only differing fields. Empty = nothing to send. Values
    /// normalized to Salesforce's form.
    /// </summary>
    /// <param name="wanted">Fields by API name; null clears the field.</param>
    /// <param name="current">Record in Salesforce, null if absent.</param>
    public static Dictionary<string, object?> Changes(Dictionary<string, object?> wanted, JsonElement? current)
    {
        var changes = new Dictionary<string, object?>();
        foreach (var (field, value) in wanted)
        {
            var normalized = FieldValues.Normalize(value);
            var same = current is { } now ? now.TryGetProperty(field, out var stored) && FieldValues.AreEqual(normalized, stored) : normalized is null;
            if (!same)
            {
                changes[field] = FieldValues.ToWire(normalized);
            }
        }

        return changes;
    }

    /// <summary>
    /// One call creates/updates all records (≤ 200 per call), matched by external id. Each record saves or fails on its
    /// own; failure → its entity's failures.
    /// </summary>
    /// <param name="sobject">Object, and the external-id field the records are matched by.</param>
    /// <param name="items">Owning entity, record's external id, fields to send (<see cref="Changes"/>).</param>
    /// <param name="failures">Where record failures go, by entity.</param>
    /// <param name="ct">Cancels calls; shutdown or lease deadline, not a failure.</param>
    /// <exception cref="UnknownOutcomeException">Call failed without proof Salesforce didn't apply it.</exception>
    /// <exception cref="MappingErrorException">Salesforce refused a field: not in org, or not settable.</exception>
    public async Task UpsertAsync(SalesforceObject sobject, IEnumerable<(string Key, string ExternalId, Dictionary<string, object?> Fields)> items,
        Dictionary<string, List<Failure>> failures, CancellationToken ct)
    {
        foreach (var chunk in items.Chunk(SalesforceRestClient.MaxCollectionRecords))
        {
            var bodies = chunk.Select(i =>
            {
                var body = new SObjectBody(sobject.Name) { [sobject.ExternalIdField] = i.ExternalId };
                foreach (var (field, value) in i.Fields)
                {
                    body[field] = value;
                }

                return body;
            }).ToList();

            var results = await GuardedAsync(() => salesforce.UpsertAsync(sobject.Name, sobject.ExternalIdField, bodies, ct));
            for (var i = 0; i < chunk.Length; i++)
            {
                AddFailures(failures, chunk[i].Key, results[i], isDelete: false);
            }
        }

        // Rejected record type may since be created/activated/assigned: re-read next time
        if (failures.Values.Any(list => list.Any(f => f.Error.StartsWith(SalesforceErrors.InvalidRecordType, StringComparison.Ordinal))))
        {
            await recordTypes.InvalidateAsync(sobject.Name, ct);
        }

        ThrowIfMappingErrors(failures);
    }

    /// <summary>One call deletes records by Salesforce Id (≤ 200 per call). Already gone = success.</summary>
    /// <exception cref="UnknownOutcomeException">Call failed without proof Salesforce didn't apply it.</exception>
    public async Task DeleteAsync(IEnumerable<(string Key, string Id)> targets, Dictionary<string, List<Failure>> failures, CancellationToken ct)
    {
        foreach (var chunk in targets.Chunk(SalesforceRestClient.MaxCollectionRecords))
        {
            var results = await GuardedAsync(() => salesforce.DeleteAsync([.. chunk.Select(t => t.Id)], ct));
            for (var i = 0; i < chunk.Length; i++)
            {
                AddFailures(failures, chunk[i].Key, results[i], isDelete: true);
            }
        }
    }

    public static void Fail(Dictionary<string, List<Failure>> failures, string key, string code, string message, bool permanent = false)
    {
        if (!failures.TryGetValue(key, out var list))
        {
            failures[key] = list = [];
        }

        list.Add(new Failure($"{code}: {message}", permanent));
    }

    /// <summary>Runs a write. Fails without proof Salesforce didn't apply it → outcome unknown.</summary>
    private static async Task<IReadOnlyList<RecordResult>> GuardedAsync(Func<Task<IReadOnlyList<RecordResult>>> write)
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

    /// <summary>
    /// Classifies a record's errors. Waited for, not abandoned: missing parent (org rule's message), records still
    /// pointing at one we delete (their deletes come via their inboxes), deactivated record type. Data's own errors
    /// (<see cref="SalesforceErrors.IsPermanent"/>) abandoned; rest retried.
    /// </summary>
    private void AddFailures(Dictionary<string, List<Failure>> failures, string key, RecordResult result, bool isDelete)
    {
        if (result.Success || (isDelete && result.Errors.Count > 0 && result.Errors.All(SalesforceErrors.IsAlreadyDeleted)))
        {
            return;
        }

        foreach (var error in result.Errors.DefaultIfEmpty(new SalesforceError(null, "UNKNOWN_ERROR", "The record failed without details")))
        {
            if (error.Code == "INVALID_CROSS_REFERENCE_KEY" && error.Message?.Contains("Record Type", StringComparison.OrdinalIgnoreCase) == true)
            {
                Fail(failures, key, SalesforceErrors.InvalidRecordType, error.Message);
            }
            else if (errors.IsParentMissing(error) || (isDelete && SalesforceErrors.HasDependents(error)))
            {
                Fail(failures, key, SalesforceErrors.DependencyNotSynced, $"{error.Code}: {error.Message}");
            }
            else
            {
                Fail(failures, key, error.Code, error.Message ?? "", errors.IsPermanent(error.Code));
            }
        }
    }

    /// <summary>
    /// Field we send that Salesforce lacks or won't let us set: mapping wrong for every event of that kind; only code
    /// change or org field fixes it. Caller stops inbox rather than burn each entity's attempts.
    /// </summary>
    private static void ThrowIfMappingErrors(Dictionary<string, List<Failure>> failures)
    {
        var found = failures.SelectMany(f => f.Value.Where(v => v.Error.StartsWith("INVALID_FIELD", StringComparison.Ordinal))
            .Select(v => (f.Key, new SalesforceError(null, "INVALID_FIELD", v.Error)))).ToList();
        if (found.Count > 0)
        {
            throw new MappingErrorException(found);
        }
    }
}

/// <summary>Entities whose fields Salesforce rejects. Stops the type's inbox until the mapping or org is fixed.</summary>
public sealed class MappingErrorException(IReadOnlyList<(string Key, SalesforceError Error)> errors)
    : Exception($"{errors.Count} mapping error(s), e.g. {errors[0].Error.Code}: {errors[0].Error.Message}")
{
    public IReadOnlyList<(string Key, SalesforceError Error)> Errors => errors;
}
