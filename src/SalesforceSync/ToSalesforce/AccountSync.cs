using System.Text.Json;
using SalesforceSync.Data.Inbox;
using SalesforceSync.Infrastructure.Salesforce;
using SalesforceSync.Sync;

namespace SalesforceSync.ToSalesforce;

/// <summary>
/// Account + its Profile__c, same batch (Accounts first). Profile names its Account by external id (Text foreign key).
/// Event without <c>profile</c> → only Account written.
/// Flow (claim, parse, record, ...) = <see cref="SalesforceBatchSyncService"/>; event handling = <see cref="Send"/>.
/// </summary>
public sealed class AccountSync(SalesforceBatchSyncService batch, SalesforceCalls salesforce, IRepository<AccountInboxRow> inbox) : IEntitySync
{
    public const string Type = AccountEvent.Type;

    /// <summary>Also the parent the relationship and contact syncs read.</summary>
    public static readonly SalesforceObject AccountObject = SalesforceObject.Of<AccountRecord>();
    private static readonly SalesforceObject ProfileObject = SalesforceObject.Of<ProfileRecord>();

    /// <summary>Developer name of the record type new Accounts get.</summary>
    private const string AccountRecordType = "Business";

    private static readonly string RecordTypeIdField = SalesforceFields.Name<AccountRecord>(r => r.RecordTypeId);

    public string EntityType => Type;

    public Task<int> SyncBatchAsync(CancellationToken ct)
    {
        return batch.SyncBatchAsync(AccountEvent.Reader, inbox, Send, ct);
    }

    private async Task<Dictionary<string, List<Failure>>> Send(IReadOnlyList<ReadEvent<AccountEvent>> events, CancellationToken ct)
    {
        var failures = new Dictionary<string, List<Failure>>();
        var deletes = events.Where(e => e.Event.Meta?.IsDelete() == true).ToList();
        var upserts = SalesforceCalls.KeepValid(events.Except(deletes), e => e.Event.Account is not null, "account is required", failures);

        var wantedAccounts = upserts.Select(WantedAccount).ToList();
        var wantedProfiles = upserts.Where(HasProfile).Select(WantedProfile).ToList();

        var accountsNow = await salesforce.ReadCurrentAsync(AccountObject, wantedAccounts, events.Select(e => e.EntityId), ct);
        var profilesNow = await salesforce.ReadCurrentAsync(ProfileObject, wantedProfiles, events.Select(ProfileKey), ct);
        wantedAccounts = await WithRecordTypeIfNewAsync(wantedAccounts, accountsNow, failures, ct);

        await salesforce.UpsertAsync(AccountObject, SalesforceCalls.Changed(wantedAccounts, accountsNow), failures, ct);
        await salesforce.UpsertAsync(ProfileObject, ProfilesOfSavedAccounts(SalesforceCalls.Changed(wantedProfiles, profilesNow), failures), failures, ct);

        await salesforce.DeleteAsync(SalesforceCalls.Existing(deletes, profilesNow, ProfileKey), failures, ct);
        await salesforce.DeleteAsync(SalesforceCalls.Existing(AccountsWithProfileDeleted(deletes, failures), accountsNow), failures, ct);

        return failures;
    }

    private static WantedRecord WantedAccount(ReadEvent<AccountEvent> e)
    {
        return new WantedRecord(e.EntityId, e.EntityId, AccountFields(e.Event.Account!));
    }

    /// <summary>
    /// Accounts Salesforce doesn't have yet get the record type (needed only to create one). If the org lacks it, they
    /// fail (retried) and are left out; their profiles then wait for them.
    /// </summary>
    private async Task<List<WantedRecord>> WithRecordTypeIfNewAsync(List<WantedRecord> accounts, Dictionary<string, JsonElement> accountsNow,
        Dictionary<string, List<Failure>> failures, CancellationToken ct)
    {
        var created = accounts.Where(a => !accountsNow.ContainsKey(a.ExternalId)).ToList();
        if (created.Count == 0)
        {
            return accounts;
        }

        var recordTypeId = await salesforce.RecordTypeIdAsync(AccountObject, AccountRecordType, ct);
        if (recordTypeId is null)
        {
            foreach (var a in created)
            {
                SalesforceCalls.Fail(failures, a.EntityId, SalesforceErrors.InvalidRecordType, $"the Account record type {AccountRecordType} is not in the org");
            }

            return [.. accounts.Except(created)];
        }

        return [.. accounts.Select(a => accountsNow.ContainsKey(a.ExternalId) ? a : a with { Fields = new(a.Fields) { [RecordTypeIdField] = recordTypeId } })];
    }

    /// <summary>An event without a profile leaves Salesforce's profile as it is.</summary>
    private static bool HasProfile(ReadEvent<AccountEvent> e)
    {
        return e.Event.Account!.Profile is not null;
    }

    private static WantedRecord WantedProfile(ReadEvent<AccountEvent> e)
    {
        return new WantedRecord(e.EntityId, ProfileKey(e), ProfileFields(e));
    }

    /// <summary>A profile names its account, so it is written only if the account was saved.</summary>
    private static IEnumerable<(string Key, string ExternalId, Dictionary<string, object?> Fields)> ProfilesOfSavedAccounts(
        IEnumerable<(string Key, string ExternalId, Dictionary<string, object?> Fields)> profiles, Dictionary<string, List<Failure>> failures)
    {
        return profiles.Where(p => !failures.ContainsKey(p.Key));
    }

    /// <summary>Children first: an account is deleted only if deleting its profile didn't fail.</summary>
    private static IEnumerable<ReadEvent<AccountEvent>> AccountsWithProfileDeleted(IEnumerable<ReadEvent<AccountEvent>> deletes, Dictionary<string, List<Failure>> failures)
    {
        return deletes.Where(e => !failures.ContainsKey(e.EntityId));
    }

    private static string ProfileKey(ReadEvent<AccountEvent> e)
    {
        return e.Event.Account?.Profile?.ExternalId ?? e.EntityId;
    }

    private static Dictionary<string, object?> AccountFields(AccountPayload a)
    {
        return SalesforceFields.Of(new AccountRecord
        {
            Name = a.Name,
            Rating = a.Rating,
            Industry = a.Industry,
            Description = a.Description,
            AnnualRevenue = a.AnnualRevenue,
            NumberOfEmployees = a.NumberOfEmployees,
            Phone = a.Phone,
            Website = a.Website,
            Active = a.IsActive,
            // list in event, ';'-separated multi-select picklist in Salesforce
            Tags = a.Tags is null ? null : string.Join(';', a.Tags),
            CustomerSince = a.CustomerSince,
            LastContacted = a.LastContactedAt
        });
    }

    private static Dictionary<string, object?> ProfileFields(ReadEvent<AccountEvent> e)
    {
        var p = e.Event.Account!.Profile!;
        return SalesforceFields.Of(new ProfileRecord
        {
            Name = p.Name,
            AccountId = e.EntityId,
            Tier = p.Tier,
            Score = p.Score,
            Discount = p.DiscountPercent,
            Notes = p.Notes,
            OptIn = p.OptIn,
            StartDate = p.StartDate
        });
    }
}
