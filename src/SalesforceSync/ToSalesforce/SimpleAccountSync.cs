using System.Globalization;
using System.Text.Json;
using SalesforceSync.Data.Inbox;
using SalesforceSync.Infrastructure.RateLimiting;
using SalesforceSync.Infrastructure.Salesforce;
using SalesforceSync.Sync;

namespace SalesforceSync.ToSalesforce;

/// <summary>
/// A version of the account sync you can read top to bottom. It does what <see cref="AccountSync"/> plus the engine
/// (<see cref="SalesforceBatchSyncService"/>) do for Accounts and their Profiles, with no shared machinery: it calls
/// <see cref="ISalesforceRestClient"/> itself. It is NOT registered in DI (the real sync is), so nothing runs it; it is
/// here to read. Left out on purpose, each one a layer of the real engine: the rate limiter pause, the lease deadline,
/// holding a write whose outcome is unknown, the mapping-error stop, retries of a lost response, splitting a batch
/// that is over a URL or record limit (a claim is at most 180 entities, under every limit), and the record types'
/// cache.
///
/// One pass of <see cref="SyncBatchAsync"/>:
///   1. CLAIM   take up to 180 due rows from the inbox table (each row = one entity's newest event).
///   2. PARSE   read each row's JSON into an AccountEvent. A row that isn't one is dead (retrying can't fix it).
///   3. READ    ask Salesforce what it has now for those Accounts and Profiles (2 queries).
///   4. DECIDE  per entity: a delete event deletes what exists; any other event upserts, but only if a field differs.
///   5. WRITE   upsert Accounts, upsert Profiles, delete Profiles, delete Accounts (at most 4 calls).
///   6. RECORD  tell the inbox, per entity, whether it is done, failed (retry later) or dead.
/// </summary>
public sealed class SimpleAccountSync(ISalesforceRestClient salesforce, IRepository<AccountInboxRow> inbox) : IEntitySync
{
    // How the org knows these objects (what the sObject classes say with attributes)
    private const string ExternalIdField = "ExternalId__c";
    private const string AccountType = "Account";
    private const string ProfileType = "Profile__c";

    // The fields of each object we write, in API names. Read back in step 3 to compare with what we want to send.
    private static readonly string[] AccountFields =
    [
        ExternalIdField, "Name", "Rating", "Industry", "Description", "AnnualRevenue", "NumberOfEmployees", "Phone", "Website",
        "Active__c", "Tags__c", "Customer_Since__c", "Last_Contacted__c", "RecordTypeId"
    ];

    private static readonly string[] ProfileFields =
    [
        ExternalIdField, "Name", "AccountId__c", "Tier__c", "Score__c", "Discount__c", "Notes__c", "Opt_In__c", "Start_Date__c", "RecordTypeId"
    ];

    // Salesforce error codes that mean "the data is wrong, retrying the same data fails the same way". Any other
    // error (a locked row, a timeout, a server error) is worth trying again later.
    private static readonly HashSet<string> NeverWorks =
    [
        "FIELD_CUSTOM_VALIDATION_EXCEPTION", "FIELD_INTEGRITY_EXCEPTION", "REQUIRED_FIELD_MISSING", "INVALID_FIELD",
        "INVALID_FIELD_FOR_INSERT_UPDATE", "INVALID_TYPE_ON_FIELD_IN_RECORD", "INVALID_OR_NULL_FOR_RESTRICTED_PICKLIST",
        "INVALID_EMAIL_ADDRESS", "MALFORMED_ID", "NUMBER_OUTSIDE_VALID_RANGE", "STRING_TOO_LONG", "DELETE_FAILED"
    ];

    public string EntityType => AccountEvent.Type;

    /// <summary>What step 4 decided for one entity, and what step 6 will say about it.</summary>
    private sealed class Entity(string key, AccountEvent @event)
    {
        public string Key { get; } = key;
        public AccountEvent Event { get; } = @event;

        /// <summary>The profile's own key, or the account's when the event doesn't give one.</summary>
        public string ProfileKey => Event.Account?.Profile?.ExternalId ?? Key;

        public bool IsDelete => Event.Meta?.IsDelete() == true;

        /// <summary>Everything Salesforce said was wrong for this entity's records; empty = all went well.</summary>
        public List<SalesforceError> Errors { get; } = [];
    }

    public async Task<int> SyncBatchAsync(CancellationToken ct)
    {
        // 1. CLAIM. The rows are leased to us: no other worker gets them until the lease runs out
        var claim = await inbox.ClaimAsync(ct);
        if (claim.Events.Count == 0)
        {
            return 0;
        }

        try
        {
            // 2. PARSE
            var dead = new List<FailedEvent>();
            var entities = new List<Entity>();
            foreach (var row in claim.Events)
            {
                if (TryParse(row, out var entity, out var problem))
                {
                    entities.Add(entity);
                }
                else
                {
                    dead.Add(new FailedEvent(row.EntityId, $"INVALID_EVENT: {problem}"));
                }
            }

            if (entities.Count > 0)
            {
                // 3. READ. Salesforce Ids by external id; a delete needs the Id, an upsert needs the current values to diff
                var accountRecordType = await RecordTypeIdAsync(AccountType, "Business", ct);
                var profileRecordType = await RecordTypeIdAsync(ProfileType, "Standard", ct);
                var accountsNow = await ReadAsync(AccountType, AccountFields, entities.Select(e => e.Key), ct);
                var profilesNow = await ReadAsync(ProfileType, ProfileFields, entities.Select(e => e.ProfileKey), ct);

                // 4. DECIDE
                var upserts = entities.Where(e => !e.IsDelete).ToList();
                var accountBodies = upserts
                    .Select(e => (Entity: e, Body: AccountBody(e, accountRecordType)))
                    .Where(x => Differs(x.Body, accountsNow.GetValueOrDefault(x.Entity.Key))).ToList();
                var profileBodies = upserts
                    .Where(e => e.Event.Account?.Profile is not null) // no profile in the event: leave Salesforce's profile alone
                    .Select(e => (Entity: e, Body: ProfileBody(e, profileRecordType)))
                    .Where(x => Differs(x.Body, profilesNow.GetValueOrDefault(x.Entity.ProfileKey))).ToList();
                var deletes = entities.Where(e => e.IsDelete).ToList();

                // 5. WRITE. Accounts before their profiles; on a delete, profiles before their account
                await UpsertAsync(AccountType, accountBodies, ct);
                await UpsertAsync(ProfileType, profileBodies, ct);
                await DeleteAsync(deletes, e => profilesNow.GetValueOrDefault(e.ProfileKey), ct);
                await DeleteAsync(deletes, e => accountsNow.GetValueOrDefault(e.Key), ct);

                // 6. RECORD
                var failed = new List<FailedEvent>();
                var done = new List<string>();
                foreach (var entity in entities)
                {
                    if (entity.Errors.Count == 0)
                    {
                        done.Add(entity.Key);
                    }
                    else
                    {
                        var error = new FailedEvent(entity.Key, string.Join("; ", entity.Errors));
                        (entity.Errors.Any(e => NeverWorks.Contains(e.Code) && !IsWaitingForDependents(e)) ? dead : failed).Add(error);
                    }
                }

                await inbox.RecordAsync(claim, new BatchOutcomes(done, dead, failed), ct);
            }
            else
            {
                await inbox.RecordAsync(claim, new BatchOutcomes([], dead, []), ct);
            }
        }
        catch (RateLimitedException ex)
        {
            // Salesforce told us to slow down: give the rows back, to be claimed again after the wait
            await inbox.ReleaseAsync(claim, new Release(ex.RetryAfter), ct);
            return 0;
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            // A whole call failed (network, 5xx, ...): every row is retried later with a backoff. Some records may have
            // been saved already; that is fine because the retry reads Salesforce again and writes only what differs
            await inbox.FailAllAsync(claim, ex.Message, ct);
        }

        return claim.Events.Count;
    }

    // ---- 2. PARSE ----

    private static bool TryParse(InboxEvent row, out Entity entity, out string problem)
    {
        // The same check ingest did when it stored the row: an account with an id, a meta with a time and a type
        var ok = AccountEvent.Reader.TryRead(row.Payload, out var read, out var why);
        entity = ok ? new Entity(row.EntityId, read!.Event) : null!;
        problem = why ?? "";
        return ok;
    }

    // ---- 3. READ ----

    /// <summary>The Id of an active record type of an object, by developer name. Salesforce takes a record type as an Id.</summary>
    private async Task<string> RecordTypeIdAsync(string sobject, string developerName, CancellationToken ct)
    {
        var rows = await salesforce.SelectAsync(SalesforceRestClient.Soql("RecordType", ["Id", "DeveloperName", "IsActive"], "SobjectType", [sobject]), ct);
        var match = rows.FirstOrDefault(r => r.GetProperty("DeveloperName").GetString() == developerName && r.GetProperty("IsActive").GetBoolean());
        return match.ValueKind == JsonValueKind.Undefined
            ? throw new InvalidOperationException($"{sobject} has no active record type '{developerName}'")
            : match.GetProperty("Id").GetString()!;
    }

    /// <summary>One query: the records of an object with these external ids, by external id. A missing key = not in Salesforce.</summary>
    private async Task<Dictionary<string, JsonElement>> ReadAsync(string sobject, string[] fields, IEnumerable<string> externalIds, CancellationToken ct)
    {
        var rows = await salesforce.SelectAsync(SalesforceRestClient.Soql(sobject, [.. fields, "Id"], ExternalIdField, externalIds.Distinct()), ct);
        return rows.ToDictionary(r => r.GetProperty(ExternalIdField).GetString()!, r => r, StringComparer.Ordinal);
    }

    // ---- 4. DECIDE: what to send ----

    /// <summary>The Account's fields as Salesforce names them. A null clears the field in Salesforce.</summary>
    private static Dictionary<string, object?> AccountBody(Entity entity, string recordTypeId)
    {
        var a = entity.Event.Account ?? throw new InvalidOperationException("account is required");
        return new Dictionary<string, object?>
        {
            [ExternalIdField] = entity.Key,
            ["Name"] = a.Name,
            ["Rating"] = a.Rating,
            ["Industry"] = a.Industry,
            ["Description"] = a.Description,
            ["AnnualRevenue"] = a.AnnualRevenue,
            ["NumberOfEmployees"] = a.NumberOfEmployees,
            ["Phone"] = a.Phone,
            ["Website"] = a.Website,
            ["Active__c"] = a.IsActive,
            ["Tags__c"] = a.Tags is null ? null : string.Join(';', a.Tags), // a list here, a ';'-separated picklist there
            ["Customer_Since__c"] = a.CustomerSince,
            ["Last_Contacted__c"] = a.LastContactedAt,
            ["RecordTypeId"] = recordTypeId
        };
    }

    /// <summary>The Profile's fields. It belongs to the event's Account: AccountId__c is the account's external id.</summary>
    private static Dictionary<string, object?> ProfileBody(Entity entity, string recordTypeId)
    {
        var p = entity.Event.Account!.Profile!;
        return new Dictionary<string, object?>
        {
            [ExternalIdField] = entity.ProfileKey,
            ["Name"] = p.Name,
            ["AccountId__c"] = entity.Key,
            ["Tier__c"] = p.Tier,
            ["Score__c"] = p.Score,
            ["Discount__c"] = p.DiscountPercent,
            ["Notes__c"] = p.Notes,
            ["Opt_In__c"] = p.OptIn,
            ["Start_Date__c"] = p.StartDate,
            ["RecordTypeId"] = recordTypeId
        };
    }

    /// <summary>True if Salesforce doesn't have the record, or has it with at least one different value.</summary>
    private static bool Differs(Dictionary<string, object?> wanted, JsonElement current)
    {
        return current.ValueKind == JsonValueKind.Undefined
               || wanted.Any(f => !SameValue(f.Value, current.TryGetProperty(f.Key, out var c) ? c : default));
    }

    /// <summary>Whether what we want to send equals what Salesforce returned for the field.</summary>
    private static bool SameValue(object? wanted, JsonElement current)
    {
        var isEmpty = current.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined;
        return wanted switch
        {
            null or "" => isEmpty || current.ValueKind == JsonValueKind.False, // Salesforce stores "" as null, an unset checkbox as false
            _ when isEmpty => false,
            bool b => current.ValueKind is JsonValueKind.True or JsonValueKind.False && current.GetBoolean() == b,
            string s => current.ValueKind == JsonValueKind.String && current.GetString() == s,
            int or long or double or decimal => current.ValueKind == JsonValueKind.Number
                                                && current.GetDecimal() == Convert.ToDecimal(wanted, CultureInfo.InvariantCulture),
            DateOnly d => current.GetString() == d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
            // Salesforce answers "2026-10-14T01:02:03.000+0000" and keeps whole seconds
            DateTimeOffset t => DateTimeOffset.TryParse(current.GetString(), CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var c)
                                && c.ToUniversalTime() == t.ToUniversalTime().AddTicks(-(t.UtcTicks % TimeSpan.TicksPerSecond)),
            _ => false
        };
    }

    // ---- 5. WRITE ----

    /// <summary>
    /// One call creates or updates all the records of an object: Salesforce finds each by its external id (PATCH
    /// composite/sobjects/{object}/ExternalId__c). It answers one result per record, in the order sent; each succeeds
    /// or fails on its own.
    /// </summary>
    private async Task UpsertAsync(string sobject, List<(Entity Entity, Dictionary<string, object?> Body)> items, CancellationToken ct)
    {
        if (items.Count == 0)
        {
            return;
        }

        var records = items.Select(x =>
        {
            var body = new SObjectBody(sobject);
            foreach (var (field, value) in x.Body)
            {
                body[field] = Wire(value);
            }

            return body;
        }).ToList();

        var results = await salesforce.UpsertAsync(sobject, ExternalIdField, records, ct);
        for (var i = 0; i < items.Count; i++)
        {
            items[i].Entity.Errors.AddRange(results[i].Errors);
        }
    }

    /// <summary>
    /// One call deletes records by Salesforce Id (DELETE composite/sobjects?ids=...). Ids came from the read in step 3; an
    /// entity whose record isn't there is already deleted, which is what the event asked for.
    /// </summary>
    private async Task DeleteAsync(List<Entity> deletes, Func<Entity, JsonElement> recordOf, CancellationToken ct)
    {
        var targets = deletes.Select(e => (Entity: e, Record: recordOf(e))).Where(x => x.Record.ValueKind != JsonValueKind.Undefined).ToList();
        if (targets.Count == 0)
        {
            return;
        }

        var results = await salesforce.DeleteAsync([.. targets.Select(x => x.Record.GetProperty("Id").GetString()!)], ct);
        for (var i = 0; i < targets.Count; i++)
        {
            // Someone deleted it between our read and now: the record is gone, so this delete succeeded
            targets[i].Entity.Errors.AddRange(results[i].Errors.Where(e => e.Code is not ("ENTITY_IS_DELETED" or "NOT_FOUND" or "INVALID_CROSS_REFERENCE_KEY")));
        }
    }

    /// <summary>The JSON value Salesforce takes: dates as yyyy-MM-dd, date-times in UTC to the second, empty text as null.</summary>
    private static object? Wire(object? value)
    {
        return value switch
        {
            string { Length: 0 } => null,
            DateOnly d => d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
            DateTimeOffset t => t.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture),
            _ => value
        };
    }

    // ---- 6. RECORD ----

    /// <summary>
    /// An Account can't be deleted while its contacts or relationships still exist: they are deleted by their own events,
    /// so this delete is tried again later rather than given up on.
    /// </summary>
    private static bool IsWaitingForDependents(SalesforceError error)
    {
        return error.Code == "DELETE_FAILED" && error.Message?.Contains("associated with", StringComparison.OrdinalIgnoreCase) == true;
    }
}
