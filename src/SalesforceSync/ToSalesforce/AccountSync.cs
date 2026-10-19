using System.Text.Json;
using SalesforceSync.Data.Inbox;
using SalesforceSync.Infrastructure.Salesforce;
using SalesforceSync.Sync;

namespace SalesforceSync.ToSalesforce;

/// <summary>
/// Account + its Profile__c, written in the same batch (Accounts first). The Profile names its Account by the Account's
/// external id (a Text foreign key). An event without a <c>profile</c> writes only the Account.
/// The flow (claim, parse, record, ...) is <see cref="SalesforceBatchSyncService"/>'s; what happens with the events is
/// <see cref="Send"/>, top to bottom.
/// </summary>
public sealed class AccountSync(SalesforceBatchSyncService batch, SalesforceCalls salesforce, IRepository<AccountInboxRow> inbox) : IEntitySync
{
    public const string Type = AccountEvent.Type;

    private const string AccountObject = "Account";
    private const string ProfileObject = "Profile__c";

    public string EntityType => Type;

    public Task<int> SyncBatchAsync(CancellationToken ct)
    {
        return batch.SyncBatchAsync(AccountEvent.Reader, inbox, Send, ct);
    }

    private async Task<Dictionary<string, List<Failure>>> Send(IReadOnlyList<ReadEvent<AccountEvent>> events, CancellationToken ct)
    {
        var failures = new Dictionary<string, List<Failure>>();
        var deletes = events.Where(e => e.Event.Meta?.IsDelete() == true).ToList();
        var upserts = events.Except(deletes).ToList();

        // An upsert needs the account in the event, and both record types
        foreach (var e in upserts.Where(e => e.Event.Account is null))
        {
            SalesforceCalls.Fail(failures, e.EntityId, SalesforceErrors.InvalidEvent, "account is required", permanent: true);
        }

        upserts = [.. upserts.Where(e => e.Event.Account is not null)];
        var accountType = await salesforce.RecordTypeIdAsync(AccountObject, "Business", ct);
        var profileType = await salesforce.RecordTypeIdAsync(ProfileObject, "Standard", ct);
        if (accountType is null || profileType is null)
        {
            foreach (var e in upserts)
            {
                SalesforceCalls.Fail(failures, e.EntityId, SalesforceErrors.InvalidRecordType, "the Account or Profile record type is not in the org");
            }

            upserts = [];
        }

        // The record each upsert wants: the fields as Salesforce names them
        var wantedAccounts = upserts.Select(e => (e.EntityId, Fields: AccountFields(e.Event.Account!, accountType!))).ToList();
        var wantedProfiles = upserts.Where(e => e.Event.Account!.Profile is not null) // no profile in the event: leave Salesforce's alone
            .Select(e => (e.EntityId, ExternalId: ProfileKey(e), Fields: ProfileFields(e, profileType!))).ToList();

        // READ what Salesforce has: the Ids a delete needs, the values an upsert is compared with
        var accountsNow = await salesforce.ReadAsync(AccountObject, wantedAccounts.SelectMany(w => w.Fields.Keys), events.Select(e => e.EntityId), ct);
        var profilesNow = await salesforce.ReadAsync(ProfileObject, wantedProfiles.SelectMany(w => w.Fields.Keys), events.Select(ProfileKey), ct);

        // DECIDE what to send: a record Salesforce lacks is created, one that differs is updated, one that is as wanted is left alone
        var accounts = wantedAccounts
            .Select(w => (w.EntityId, ExternalId: w.EntityId, Fields: SalesforceCalls.Changes(w.Fields, Now(accountsNow, w.EntityId))))
            .Where(x => x.Fields.Count > 0).ToList();
        var profiles = wantedProfiles
            .Select(w => (w.EntityId, w.ExternalId, Fields: SalesforceCalls.Changes(w.Fields, Now(profilesNow, w.ExternalId))))
            .Where(x => x.Fields.Count > 0).ToList();

        // WRITE. Accounts first, and a profile only if its account was saved
        await salesforce.UpsertAsync(AccountObject, accounts, failures, ct);
        await salesforce.UpsertAsync(ProfileObject, profiles.Where(p => !failures.ContainsKey(p.EntityId)), failures, ct);

        // A delete removes the Profile, then the Account (children first). A record Salesforce lacks is already deleted
        await salesforce.DeleteAsync(deletes.Where(e => profilesNow.ContainsKey(ProfileKey(e)))
            .Select(e => (e.EntityId, profilesNow[ProfileKey(e)].GetProperty("Id").GetString()!)), failures, ct);
        await salesforce.DeleteAsync(deletes.Where(e => accountsNow.ContainsKey(e.EntityId) && !failures.ContainsKey(e.EntityId))
            .Select(e => (e.EntityId, accountsNow[e.EntityId].GetProperty("Id").GetString()!)), failures, ct);

        return failures;
    }

    // ---- the mapping: event -> the fields Salesforce names ----

    /// <summary>The Profile's own key, or the Account's when the event doesn't give one.</summary>
    private static string ProfileKey(ReadEvent<AccountEvent> e)
    {
        return e.Event.Account?.Profile?.ExternalId ?? e.EntityId;
    }

    private static JsonElement? Now(Dictionary<string, JsonElement> found, string key)
    {
        return found.TryGetValue(key, out var row) ? row : null;
    }

    private static Dictionary<string, object?> AccountFields(AccountPayload a, string? recordTypeId)
    {
        return new Dictionary<string, object?>
        {
            ["Name"] = a.Name,
            ["Rating"] = a.Rating,
            ["Industry"] = a.Industry,
            ["Description"] = a.Description,
            ["AnnualRevenue"] = a.AnnualRevenue,
            ["NumberOfEmployees"] = a.NumberOfEmployees,
            ["Phone"] = a.Phone,
            ["Website"] = a.Website,
            ["Active__c"] = a.IsActive,
            // A list in the event, a ';'-separated multi-select picklist in Salesforce
            ["Tags__c"] = a.Tags is null ? null : string.Join(';', a.Tags),
            ["Customer_Since__c"] = a.CustomerSince,
            ["Last_Contacted__c"] = a.LastContactedAt,
            ["RecordTypeId"] = recordTypeId
        };
    }

    private static Dictionary<string, object?> ProfileFields(ReadEvent<AccountEvent> e, string? recordTypeId)
    {
        var p = e.Event.Account!.Profile!;
        return new Dictionary<string, object?>
        {
            ["Name"] = p.Name,
            ["AccountId__c"] = e.EntityId, // the Profile belongs to the event's Account
            ["Tier__c"] = p.Tier,
            ["Score__c"] = p.Score,
            ["Discount__c"] = p.DiscountPercent,
            ["Notes__c"] = p.Notes,
            ["Opt_In__c"] = p.OptIn,
            ["Start_Date__c"] = p.StartDate,
            ["RecordTypeId"] = recordTypeId
        };
    }
}
