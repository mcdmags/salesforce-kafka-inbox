using System.Reflection;
using System.Text.Json;
using SalesforceSync.Infrastructure.Salesforce;
using SalesforceSync.Sync;

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

/// <summary>A record an event wants in Salesforce: its entity, the external id it is matched by, every field it sends.</summary>
public sealed record DesiredRecord(string EntityId, string ExternalId, Dictionary<string, object?> Fields);

/// <summary>
/// Reads, diffs and writes records in Salesforce for the syncs: read what Salesforce has, keep only the fields that
/// differ (<see cref="ChangedFields"/>), upsert or delete those.
/// </summary>
/// <remarks>
/// Each method is one API call (a few for big batches) and decides nothing about what to write: that is each sync's
/// job. A record's failure goes in its entity's <c>failures</c> as a <see cref="Failure"/>.
/// </remarks>
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
    /// <exception cref="StopInboxException">Org lacks such a field on the object (a mapping error).</exception>
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
                // Org lacks field: mapping wrong for every entity of the type
                throw new StopInboxException(StopReason.MappingError, [.. ids.Select(id => (id, new SalesforceError(null, "INVALID_FIELD", ex.Message)))]);
            }
        }

        return found;
    }

    /// <summary>
    /// 200 ids per query, halved while the query's URL is over Salesforce's limit: the query is a GET, so long external
    /// ids can overflow its URL long before 200 of them.
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

    /// <summary>
    /// The diff: the fields to send. For a new record, every field with a value; for an existing one, only the fields
    /// that differ. Empty means nothing to send.
    /// </summary>
    /// <param name="desired">Fields by API name; null clears the field.</param>
    /// <param name="current">Record in Salesforce, null if absent.</param>
    public static Dictionary<string, object?> ChangedFields(Dictionary<string, object?> desired, JsonElement? current)
    {
        var changes = new Dictionary<string, object?>();
        foreach (var (field, value) in desired)
        {
            var normalized = FieldValues.Normalize(value);
            var same = current is { } now ? now.TryGetProperty(field, out var stored) && FieldValues.AreEqual(normalized, stored) : normalized is null;
            if (!same)
            {
                changes[field] = FieldValues.ToSalesforceValue(normalized);
            }
        }

        return changes;
    }

    /// <summary>The events <paramref name="isValid"/> accepts; any other is a bad event, dead-lettered at once with <paramref name="problem"/>.</summary>
    public static List<ReadEvent<TEvent>> KeepValid<TEvent>(IEnumerable<ReadEvent<TEvent>> events, Func<ReadEvent<TEvent>, bool> isValid, string problem,
        Dictionary<string, List<Failure>> failures)
    {
        var all = events.ToList();
        foreach (var e in all.Where(e => !isValid(e)))
        {
            Fail(failures, e.EntityId, SalesforceErrors.InvalidEvent, problem, permanent: true);
        }

        return [.. all.Where(isValid)];
    }

    public static void FailAll<TEvent>(IEnumerable<ReadEvent<TEvent>> events, Dictionary<string, List<Failure>> failures, string code, string message)
    {
        foreach (var e in events)
        {
            Fail(failures, e.EntityId, code, message);
        }
    }

    /// <summary>
    /// The events whose parent Accounts (<paramref name="parents"/>: external ids) are all in <paramref name="accounts"/>.
    /// Any other waits: retried until the account sync has created them.
    /// </summary>
    public static List<ReadEvent<TEvent>> WithParentsInSalesforce<TEvent>(IEnumerable<ReadEvent<TEvent>> events, Func<ReadEvent<TEvent>, string[]> parents,
        Dictionary<string, JsonElement> accounts, Dictionary<string, List<Failure>> failures)
    {
        var ready = new List<ReadEvent<TEvent>>();
        foreach (var e in events)
        {
            if (parents(e).Where(p => !accounts.ContainsKey(p)).ToList() is { Count: > 0 } missing)
            {
                Fail(failures, e.EntityId, SalesforceErrors.DependencyNotSynced, $"Account {string.Join(", ", missing)} is not in Salesforce yet");
            }
            else
            {
                ready.Add(e);
            }
        }

        return ready;
    }

    /// <summary>
    /// What Salesforce has for these external ids: the values to compare, and the Ids deletes need. Reads every field
    /// the desired records send, so a field the org lacks shows here, before anything is written.
    /// </summary>
    public Task<Dictionary<string, JsonElement>> ReadCurrentAsync(SalesforceObject sobject, IEnumerable<DesiredRecord> desired, IEnumerable<string> externalIds,
        CancellationToken ct)
    {
        return ReadAsync(sobject, desired.SelectMany(w => w.Fields.Keys), externalIds, ct);
    }

    /// <summary>What to write (<see cref="ChangedFields"/>): records missing or differing; one already as desired is left out.</summary>
    public static List<(string Key, string ExternalId, Dictionary<string, object?> Fields)> ChangedRecords(IEnumerable<DesiredRecord> desired,
        Dictionary<string, JsonElement> current)
    {
        return [.. desired
            .Select(w => (w.EntityId, w.ExternalId, Fields: ChangedFields(w.Fields, current.TryGetValue(w.ExternalId, out var row) ? row : null)))
            .Where(w => w.Fields.Count > 0)];
    }

    /// <summary>
    /// The deletes whose record Salesforce has, by its Id (found by <paramref name="externalId"/>, default the entity
    /// id). One Salesforce lacks is already deleted.
    /// </summary>
    public static IEnumerable<(string Key, string Id)> RecordsToDelete<TEvent>(IEnumerable<ReadEvent<TEvent>> deletes, Dictionary<string, JsonElement> current,
        Func<ReadEvent<TEvent>, string>? externalId = null)
    {
        externalId ??= e => e.EntityId;
        return deletes.Where(e => current.ContainsKey(externalId(e))).Select(e => (e.EntityId, current[externalId(e)].GetProperty("Id").GetString()!));
    }

    /// <summary>
    /// One call creates/updates all records (≤ 200 per call), matched by external id. Each record saves or fails on its
    /// own; failure → its entity's failures.
    /// </summary>
    /// <param name="sobject">Object, and the external-id field the records are matched by.</param>
    /// <param name="items">Owning entity, record's external id, fields to send (<see cref="ChangedFields"/>).</param>
    /// <param name="failures">Where record failures go, by entity.</param>
    /// <param name="ct">Cancels calls; shutdown or lease deadline, not a failure.</param>
    /// <exception cref="UnknownOutcomeException">Call failed without proof Salesforce didn't apply it.</exception>
    /// <exception cref="StopInboxException">An error wrong for every entity of the type: a field the org lacks or won't let
    /// us set, or full data storage.</exception>
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

            var results = await SendWriteAsync(() => salesforce.UpsertAsync(sobject.Name, sobject.ExternalIdField, bodies, ct));
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

        ThrowIfInboxStops(failures);
    }

    /// <summary>One call deletes records by Salesforce Id (≤ 200 per call). Already gone = success.</summary>
    /// <exception cref="UnknownOutcomeException">Call failed without proof Salesforce didn't apply it.</exception>
    public async Task DeleteAsync(IEnumerable<(string Key, string Id)> targets, Dictionary<string, List<Failure>> failures, CancellationToken ct)
    {
        foreach (var chunk in targets.Chunk(SalesforceRestClient.MaxCollectionRecords))
        {
            var results = await SendWriteAsync(() => salesforce.DeleteAsync([.. chunk.Select(t => t.Id)], ct));
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
    /// Whether a failed write may still have been applied. False only when the failure proves it wasn't; anything
    /// else is unknown, the safe default.
    /// <list type="bullet">
    /// <item>Not applied: no access token (<see cref="SalesforceAuthException"/>: not sent), Salesforce's own rejection
    /// (any 4xx, including 429 and REQUEST_LIMIT_EXCEEDED, or a 5xx carrying a Salesforce error body), or a connection
    /// that was never established (refused, DNS, TLS handshake).</item>
    /// <item>Unknown: a cancellation or timeout (the lease cutoff, the HTTP timeout, shutdown), a connection lost once
    /// the request may have left, 502/503/504 from something in front of Salesforce, or a response that can't be read.</item>
    /// </list>
    /// </summary>
    internal static bool IsUnknownOutcome(Exception ex)
    {
        return ex switch
        {
            SalesforceAuthException => false,
            SalesforceResponseException r => (int)r.StatusCode! >= 500 && !r.IsSalesforceError,
            HttpRequestException { StatusCode: { } status } => (int)status >= 500,
            HttpRequestException
            {
                HttpRequestError: HttpRequestError.ConnectionError or HttpRequestError.NameResolutionError
                    or HttpRequestError.SecureConnectionError or HttpRequestError.ProxyTunnelError
            } => false,
            ArgumentException => false, // refused by the client before anything was sent (too many collections or records)
            _ => true
        };
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
    /// Errors wrong for every entity of the type, not one: only a code change or an org change fixes them. The caller
    /// stops the inbox rather than burn each entity's attempts. First match wins.
    /// </summary>
    private static readonly (string Code, StopReason Reason)[] InboxStoppers =
    [
        ("INVALID_FIELD", StopReason.MappingError), // and INVALID_FIELD_FOR_INSERT_UPDATE: a field the org lacks or won't let us set
        (SalesforceErrors.StorageLimitExceeded, StopReason.StorageFull)
    ];

    private static void ThrowIfInboxStops(Dictionary<string, List<Failure>> failures)
    {
        foreach (var (code, reason) in InboxStoppers)
        {
            var found = failures.SelectMany(f => f.Value.Where(v => v.Error.StartsWith(code, StringComparison.Ordinal))
                .Select(v => (f.Key, new SalesforceError(null, code, v.Error)))).ToList();
            if (found.Count > 0)
            {
                throw new StopInboxException(reason, found);
            }
        }
    }
}

/// <summary>Why an inbox stops (<see cref="StopInboxException"/>).</summary>
public enum StopReason
{
    /// <summary>A field the code sends that the org lacks or won't let us set: fixed by a deploy or an org field.</summary>
    MappingError,

    /// <summary>The org's data storage is full (STORAGE_LIMIT_EXCEEDED): fixed by freeing space.</summary>
    StorageFull,

    /// <summary>Salesforce asks us to back off (<see cref="SalesforceErrors.IsRequestLimit"/>): clears with time.</summary>
    RequestLimit
}

/// <summary>
/// An error wrong for every entity of the type, not one (<see cref="StopReason"/>): stops the type's inbox for a hold,
/// no attempt spent, nothing dead-lettered, until the cause is fixed.
/// </summary>
public sealed class StopInboxException(StopReason reason, IReadOnlyList<(string Key, SalesforceError Error)> errors)
    : Exception($"{errors.Count} error(s), e.g. {errors[0].Error.Code}: {errors[0].Error.Message}")
{
    public StopReason Reason => reason;

    public IReadOnlyList<(string Key, SalesforceError Error)> Errors => errors;
}
