using System.Text.Json;
using SalesforceSync.Infrastructure.RateLimiting;
using SalesforceSync.Infrastructure.Salesforce;
using SalesforceSync.Sync;

namespace SalesforceSync.ToSalesforce;

/// <summary>
/// Reads, diffs and writes records in Salesforce for the syncs: read what Salesforce has, keep only the fields that
/// differ (<see cref="ChangedFields"/>), upsert or delete those.
/// </summary>
/// <remarks>
/// Each method is one API call (a few for big batches) and decides nothing about what to write: that is each sync's
/// job. A record's failure is recorded on its entity in <see cref="Failures"/>.
/// </remarks>
public sealed class SalesforceCalls(ISalesforceRestClient salesforce, RecordTypeCache recordTypes, SalesforceErrors errors)
{
    /// <summary>
    /// Errors that are wrong for every entity of the type, not one: only a code change or an org change fixes them, so
    /// the inbox pauses rather than spend each entity's attempts. First match wins.
    /// </summary>
    private static readonly (string CodePrefix, PauseReason Reason)[] InboxPausers =
    [
        ("INVALID_FIELD", PauseReason.MappingError), // also INVALID_FIELD_FOR_INSERT_UPDATE: a field the org lacks or won't let us set
        (SalesforceErrors.StorageLimitExceeded, PauseReason.StorageFull)
    ];

    /// <summary>The Id of an object's active record type by developer name (Salesforce takes an Id), or null if the org has none.</summary>
    public Task<string?> RecordTypeIdAsync(SalesforceObject sobject, string developerName, CancellationToken ct)
    {
        return recordTypes.IdOfAsync(sobject.Name, developerName, ct);
    }

    /// <summary>
    /// The records Salesforce has for these external ids, by external id, with the given fields, the Id (deletes need it)
    /// and the external id. A record Salesforce doesn't have is not in the result.
    /// </summary>
    /// <exception cref="PauseInboxException">The org lacks one of the fields on the object (a mapping error).</exception>
    public async Task<Dictionary<string, JsonElement>> ReadAsync(SalesforceObject sobject, IEnumerable<string> fields, IEnumerable<string> externalIds,
        CancellationToken ct)
    {
        var columns = fields.Append(sobject.ExternalIdField).Append("Id").Distinct(StringComparer.OrdinalIgnoreCase).ToList();
        var found = new Dictionary<string, JsonElement>(StringComparer.Ordinal);
        foreach (var (soql, ids) in QueriesWithinUrlLimit(sobject, columns, externalIds))
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
                throw new PauseInboxException(PauseReason.MappingError, [.. ids.Select(id => (id, new SalesforceError(null, "INVALID_FIELD", ex.Message)))]);
            }
        }

        return found;
    }

    /// <summary>
    /// The fields to send for one record. A new record gets every field with a value; an existing one only the fields
    /// that differ from what Salesforce has. Empty means nothing to send.
    /// </summary>
    /// <param name="desired">Fields by API name; null clears the field.</param>
    /// <param name="current">The record in Salesforce, or null if it has none.</param>
    public static Dictionary<string, object?> ChangedFields(Dictionary<string, object?> desired, JsonElement? current)
    {
        var changes = new Dictionary<string, object?>();
        foreach (var (field, value) in desired)
        {
            var normalized = FieldValues.Normalize(value);
            var same = current is { } record
                ? record.TryGetProperty(field, out var stored) && FieldValues.AreEqual(normalized, stored)
                : normalized is null;
            if (!same)
            {
                changes[field] = FieldValues.ToSalesforceValue(normalized);
            }
        }

        return changes;
    }

    /// <summary>
    /// Upserts the records that differ from what Salesforce has (<paramref name="current"/>, from <see cref="ReadAsync"/>),
    /// each with only its changed fields. A record that is already as desired is not sent.
    /// </summary>
    /// <inheritdoc cref="UpsertAsync" path="/exception"/>
    public Task UpsertChangedAsync(SalesforceObject sobject, IEnumerable<DesiredRecord> desired, Dictionary<string, JsonElement> current,
        Failures failures, CancellationToken ct)
    {
        var changed = desired
            .Select(d => d with { Fields = ChangedFields(d.Fields, current.TryGetValue(d.ExternalId, out var record) ? record : null) })
            .Where(d => d.Fields.Count > 0);
        return UpsertAsync(sobject, changed, failures, ct);
    }

    /// <summary>
    /// Creates or updates the records as they are, matched by external id, 200 per call. Each record saves or fails on
    /// its own; a failure is recorded on its entity.
    /// </summary>
    /// <exception cref="UnknownOutcomeException">A call failed without proof that Salesforce didn't apply it.</exception>
    /// <exception cref="PauseInboxException">An error that is wrong for every entity of the type: a field the org lacks or
    /// won't let us set, or full data storage.</exception>
    public async Task UpsertAsync(SalesforceObject sobject, IEnumerable<DesiredRecord> records, Failures failures, CancellationToken ct)
    {
        foreach (var chunk in records.Chunk(SalesforceRestClient.MaxCollectionRecords))
        {
            var bodies = chunk.Select(r => Body(sobject, r)).ToList();
            var results = await SendWriteAsync(() => salesforce.UpsertAsync(sobject.Name, sobject.ExternalIdField, bodies, ct));
            for (var i = 0; i < chunk.Length; i++)
            {
                RecordFailures(failures, chunk[i].EntityId, results[i], isDelete: false);
            }
        }

        // A rejected record type may since have been created, activated or assigned: read them again next time
        if (failures.WithCodePrefix(SalesforceErrors.InvalidRecordType).Any())
        {
            await recordTypes.InvalidateAsync(sobject.Name, ct);
        }

        ThrowIfInboxPauses(failures);
    }

    /// <summary>
    /// The deletes whose record Salesforce has, with its Id. A record found by <paramref name="externalId"/> (by default
    /// the entity id); one Salesforce doesn't have is already deleted.
    /// </summary>
    public static IEnumerable<RecordToDelete> RecordsToDelete<TEvent>(IEnumerable<ReadEvent<TEvent>> deletes, Dictionary<string, JsonElement> current,
        Func<ReadEvent<TEvent>, string>? externalId = null)
    {
        externalId ??= e => e.EntityId;
        foreach (var e in deletes)
        {
            if (current.TryGetValue(externalId(e), out var record))
            {
                yield return new RecordToDelete(e.EntityId, record.GetProperty("Id").GetString()!);
            }
        }
    }

    /// <summary>Deletes records by Salesforce Id, 200 per call. A record that is already gone counts as deleted.</summary>
    /// <exception cref="UnknownOutcomeException">A call failed without proof that Salesforce didn't apply it.</exception>
    public async Task DeleteAsync(IEnumerable<RecordToDelete> records, Failures failures, CancellationToken ct)
    {
        foreach (var chunk in records.Chunk(SalesforceRestClient.MaxCollectionRecords))
        {
            var results = await SendWriteAsync(() => salesforce.DeleteAsync([.. chunk.Select(r => r.Id)], ct));
            for (var i = 0; i < chunk.Length; i++)
            {
                RecordFailures(failures, chunk[i].EntityId, results[i], isDelete: true);
            }
        }
    }

    /// <summary>The events <paramref name="isValid"/> accepts. Any other is a bad event, dead-lettered at once with <paramref name="problem"/>.</summary>
    public static List<ReadEvent<TEvent>> KeepValid<TEvent>(IEnumerable<ReadEvent<TEvent>> events, Func<ReadEvent<TEvent>, bool> isValid, string problem,
        Failures failures)
    {
        var valid = new List<ReadEvent<TEvent>>();
        foreach (var e in events)
        {
            if (isValid(e))
            {
                valid.Add(e);
            }
            else
            {
                failures.Add(e.EntityId, SalesforceErrors.InvalidEvent, problem, permanent: true);
            }
        }

        return valid;
    }

    /// <summary>
    /// The events whose parent Accounts (<paramref name="parents"/>, by external id) are all in <paramref name="accounts"/>.
    /// Any other waits, retried until the account sync has created its parents.
    /// </summary>
    public static List<ReadEvent<TEvent>> WithParentsInSalesforce<TEvent>(IEnumerable<ReadEvent<TEvent>> events, Func<ReadEvent<TEvent>, string[]> parents,
        Dictionary<string, JsonElement> accounts, Failures failures)
    {
        var ready = new List<ReadEvent<TEvent>>();
        foreach (var e in events)
        {
            var missing = parents(e).Where(p => !accounts.ContainsKey(p)).ToList();
            if (missing.Count > 0)
            {
                failures.Add(e.EntityId, SalesforceErrors.DependencyNotSynced, $"Account {string.Join(", ", missing)} is not in Salesforce yet");
            }
            else
            {
                ready.Add(e);
            }
        }

        return ready;
    }

    /// <summary>
    /// Whether a failed write may still have been applied. False only when the failure proves it wasn't; anything else
    /// is unknown, the safe default.
    /// <list type="bullet">
    /// <item>Not applied: a failure before the request left (<see cref="SalesforceRestClient.MarkNotSent"/>), Salesforce's
    /// own rejection (any 4xx, or a 5xx carrying a Salesforce error body), a rate limit (no token before sending, 429,
    /// REQUEST_LIMIT_EXCEEDED, a 503 from Salesforce), or a connection that was never established (refused, DNS, TLS
    /// handshake).</item>
    /// <item>Unknown: a cancellation or timeout (the lease cutoff, the HTTP timeout, shutdown), a connection lost once
    /// the request may have left, 502/503/504 from something in front of Salesforce, or a response that can't be read.</item>
    /// </list>
    /// </summary>
    internal static bool IsUnknownOutcome(Exception ex)
    {
        return !SalesforceRestClient.WasNotSent(ex) && ex switch
        {
            RateLimitedException limited => limited.MayHaveBeenProcessed,
            SalesforceResponseException r => (int)r.StatusCode! >= 500 && !r.IsSalesforceError,
            HttpRequestException { StatusCode: { } status } => (int)status >= 500,
            HttpRequestException
            {
                HttpRequestError: HttpRequestError.ConnectionError or HttpRequestError.NameResolutionError
                    or HttpRequestError.SecureConnectionError or HttpRequestError.ProxyTunnelError
            } => false,
            ArgumentException => false, // refused by the client before anything was sent (too many records)
            _ => true
        };
    }

    private static SObjectBody Body(SalesforceObject sobject, DesiredRecord record)
    {
        var body = new SObjectBody(sobject.Name) { [sobject.ExternalIdField] = record.ExternalId };
        foreach (var (field, value) in record.Fields)
        {
            body[field] = value;
        }

        return body;
    }

    /// <summary>
    /// Sends a write. If it fails without proof that Salesforce didn't apply it, throws <see cref="UnknownOutcomeException"/>.
    /// </summary>
    private static async Task<IReadOnlyList<RecordResult>> SendWriteAsync(Func<Task<IReadOnlyList<RecordResult>>> write)
    {
        try
        {
            return await write();
        }
        catch (Exception ex) when (IsUnknownOutcome(ex))
        {
            throw new UnknownOutcomeException(ex);
        }
    }

    /// <summary>
    /// Records a record's errors on its entity. These wait instead of failing: a missing parent (the org's message), a
    /// delete blocked by records that still point at it (they are deleted through their own inboxes), and a record
    /// type the org lacks. Errors in the data itself (<see cref="SalesforceErrors.IsPermanent"/>) are permanent; the
    /// rest are retried.
    /// </summary>
    private void RecordFailures(Failures failures, string entityId, RecordResult result, bool isDelete)
    {
        if (result.Success || (isDelete && result.Errors.Count > 0 && result.Errors.All(SalesforceErrors.IsAlreadyDeleted)))
        {
            return;
        }

        foreach (var error in result.Errors.DefaultIfEmpty(new SalesforceError(null, "UNKNOWN_ERROR", "The record failed without details")))
        {
            if (error.Code == "INVALID_CROSS_REFERENCE_KEY" && error.Message?.Contains("Record Type", StringComparison.OrdinalIgnoreCase) == true)
            {
                failures.Add(entityId, SalesforceErrors.InvalidRecordType, error.Message);
            }
            else if (errors.IsParentMissing(error) || (isDelete && SalesforceErrors.HasDependents(error)))
            {
                failures.Add(entityId, SalesforceErrors.DependencyNotSynced, $"{error.Code}: {error.Message}");
            }
            else
            {
                failures.Add(entityId, error.Code, error.Message ?? "", errors.IsPermanent(error.Code));
            }
        }
    }

    private static void ThrowIfInboxPauses(Failures failures)
    {
        foreach (var (codePrefix, reason) in InboxPausers)
        {
            var found = failures.WithCodePrefix(codePrefix).Select(f => (f.EntityId, new SalesforceError(null, codePrefix, f.Failure.Error))).ToList();
            if (found.Count > 0)
            {
                throw new PauseInboxException(reason, found);
            }
        }
    }

    /// <summary>
    /// 200 ids per query, halved while the query's URL is over Salesforce's limit: a query is a GET, so long external
    /// ids can overflow its URL well before 200 of them.
    /// </summary>
    private IEnumerable<(string Soql, string[] Ids)> QueriesWithinUrlLimit(SalesforceObject sobject, List<string> columns, IEnumerable<string> externalIds)
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
}
