using System.Text.Json;
using SalesforceSync.Infrastructure.Salesforce;

namespace SalesforceSync.ToSalesforce;

/// <summary>
/// The Salesforce calls the types' send steps are made of, each one API call (or a few, for a big batch) and nothing
/// else: no decisions about what to write, that is each type's. Answers a record fails with are put in the
/// <c>failures</c> of its entity, as <see cref="Failure"/>s.
/// </summary>
public sealed class SalesforceCalls(ISalesforceRestClient salesforce, RecordTypeCache recordTypes, SalesforceErrors errors)
{
    /// <summary>The field every object is matched by.</summary>
    public const string ExternalIdField = "ExternalId__c";

    /// <summary>The Id of an active record type of an object, by developer name; Salesforce takes a record type as an Id. Null if the org has none.</summary>
    public Task<string?> RecordTypeIdAsync(string sobject, string developerName, CancellationToken ct)
    {
        return recordTypes.IdOfAsync(sobject, developerName, ct);
    }

    /// <summary>
    /// What Salesforce has now for the records with these external ids (queries of up to 200 ids): the fields, the Id (a
    /// delete needs it) and the external id they are found by. A record Salesforce doesn't have isn't in the result.
    /// </summary>
    /// <exception cref="MappingErrorException">The org has no such field on the object.</exception>
    public async Task<Dictionary<string, JsonElement>> ReadAsync(string sobject, IEnumerable<string> fields, IEnumerable<string> externalIds,
        CancellationToken ct)
    {
        var columns = fields.Append(ExternalIdField).Append("Id").Distinct(StringComparer.OrdinalIgnoreCase).ToList();
        var found = new Dictionary<string, JsonElement>(StringComparer.Ordinal);
        foreach (var ids in externalIds.Distinct(StringComparer.Ordinal).Chunk(SalesforceRestClient.MaxCollectionRecords))
        {
            try
            {
                foreach (var row in await salesforce.SelectAsync(SalesforceRestClient.Soql(sobject, columns, ExternalIdField, ids), ct))
                {
                    found[row.GetProperty(ExternalIdField).GetString()!] = row;
                }
            }
            catch (SalesforceResponseException ex) when (ex.ErrorCode == "INVALID_FIELD")
            {
                // The org has no such field: the mapping is wrong for every entity of the type
                throw new MappingErrorException([.. ids.Select(id => (id, new SalesforceError(null, "INVALID_FIELD", ex.Message)))]);
            }
        }

        return found;
    }

    /// <summary>
    /// What to send for a record: for a new one, its non-empty fields; for one Salesforce has, only the fields that differ.
    /// Empty = nothing to send (it is as wanted). Values are normalized and put in the form Salesforce takes.
    /// </summary>
    /// <param name="wanted">The record's fields by API name; a null clears the field.</param>
    /// <param name="current">The record as Salesforce has it, or null if it doesn't.</param>
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
    /// One call creates or updates all the records (up to 200 per call): Salesforce finds each by its external id. Each
    /// record is saved or fails on its own; a failure is added to its entity's failures.
    /// </summary>
    /// <param name="sobject">The object's API name.</param>
    /// <param name="items">The entity each record belongs to, the record's external id and the fields to send (<see cref="Changes"/>).</param>
    /// <param name="failures">Where a record's failure is added, by entity.</param>
    /// <param name="ct">Cancels the calls; shutdown or the lease deadline, not a failure.</param>
    /// <exception cref="UnknownOutcomeException">The call failed without proving that Salesforce didn't apply it.</exception>
    /// <exception cref="MappingErrorException">Salesforce refused a field: it is not in the org, or can't be set.</exception>
    public async Task UpsertAsync(string sobject, IEnumerable<(string Key, string ExternalId, Dictionary<string, object?> Fields)> items,
        Dictionary<string, List<Failure>> failures, CancellationToken ct)
    {
        foreach (var chunk in items.Chunk(SalesforceRestClient.MaxCollectionRecords))
        {
            var bodies = chunk.Select(i =>
            {
                var body = new SObjectBody(sobject) { [ExternalIdField] = i.ExternalId };
                foreach (var (field, value) in i.Fields)
                {
                    body[field] = value;
                }

                return body;
            }).ToList();

            var results = await GuardedAsync(() => salesforce.UpsertAsync(sobject, ExternalIdField, bodies, ct));
            for (var i = 0; i < chunk.Length; i++)
            {
                AddFailures(failures, chunk[i].Key, results[i], isDelete: false);
            }
        }

        // A record type the org rejected may since have been created, activated or assigned: read it again next time
        if (failures.Values.Any(list => list.Any(f => f.Error.StartsWith(SalesforceErrors.InvalidRecordType, StringComparison.Ordinal))))
        {
            await recordTypes.InvalidateAsync(sobject, ct);
        }

        ThrowIfMappingErrors(failures);
    }

    /// <summary>One call deletes records by Salesforce Id (up to 200 per call). A record that is gone already is what the delete wanted.</summary>
    /// <exception cref="UnknownOutcomeException">The call failed without proving that Salesforce didn't apply it.</exception>
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

    /// <summary>Adds a failure of the entity.</summary>
    public static void Fail(Dictionary<string, List<Failure>> failures, string key, string code, string message, bool permanent = false)
    {
        if (!failures.TryGetValue(key, out var list))
        {
            failures[key] = list = [];
        }

        list.Add(new Failure($"{code}: {message}", permanent));
    }

    /// <summary>Runs a write. If it fails without proving Salesforce didn't apply it, the outcome is unknown.</summary>
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
    /// What a record's errors mean. A parent that isn't there (an org rule's message) or records still pointing at one we
    /// delete (their own deletes come through their inboxes) is waited for, not given up on; so is a record type that
    /// was deactivated. The data's own errors (<see cref="SalesforceErrors.IsPermanent"/>) are given up on; the rest is retried.
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
    /// A field the code sends that Salesforce lacks or won't let us set: the mapping is wrong for every event of that kind,
    /// so only a code change or a field in the org fixes it. The caller stops the inbox rather than spend each entity's attempts.
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

/// <summary>Entities whose mapping Salesforce rejects, with what to fix.</summary>
public sealed class MappingErrorException(IReadOnlyList<(string Key, SalesforceError Error)> errors)
    : Exception($"{errors.Count} mapping error(s), e.g. {errors[0].Error.Code}: {errors[0].Error.Message}")
{
    public IReadOnlyList<(string Key, SalesforceError Error)> Errors => errors;
}
