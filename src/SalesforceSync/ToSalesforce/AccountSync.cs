using SalesforceSync.Data.Inbox;
using SalesforceSync.Sync;

namespace SalesforceSync.ToSalesforce;

/// <summary>
/// Account + its Profile__c, written in the same batch (Accounts first). The Profile names its Account by the Account's
/// external id (a Text foreign key).
/// Typed event -> mapping -> sObjects -> records. An event without a <c>profile</c> writes only the Account.
/// The mapping from the event (<see cref="AccountEvent"/>) to the sObjects (<c>AccountSObjects.cs</c>) is here.
/// </summary>
public sealed class AccountSync(SalesforceBatchSyncService engine, IRepository<AccountInboxRow> inbox)
    : EntitySync<AccountEvent, AccountInboxRow>(engine, inbox)
{
    public const string Type = AccountEvent.Type;

    protected override EventReader<AccountEvent> Reader => AccountEvent.Reader;

    /// <summary>
    /// A deleted event deletes the Profile, then the Account (children first): an org whose automation refuses to
    /// delete an Account that records still name accepts that order. Salesforce itself refuses nothing for Text
    /// keys; the contacts' and relationships' own deletes come through their inboxes.
    /// Any other event upserts the Account and, if the event has one, its Profile.
    /// </summary>
    public override SyncUnit Map(string accountKey, AccountEvent e)
    {
        if (e.Meta?.IsDelete() == true)
        {
            return new SyncUnit([SObjectRecord.Deleting<ProfileSObject>("prof", e.Account?.Profile?.ExternalId ?? accountKey),
                SObjectRecord.Deleting<AccountSObject>("acct", accountKey)]);
        }

        var payload = e.Account ?? throw new ArgumentException("account is required");
        var records = new List<SObjectRecord> { SObjectRecord.From("acct", ToAccount(payload)) };

        if (payload.Profile is not null)
        {
            var profile = ToProfile(payload.Profile);
            // What the profile takes from its account: its id (unless it has its own) and the Account it belongs to
            profile.ExternalId ??= accountKey;
            profile.AccountId = accountKey;
            records.Add(SObjectRecord.From("prof", profile));
        }
        return new SyncUnit(records);
    }

    public override IEnumerable<SoqlQuery> Queries(IReadOnlyCollection<SyncUnit> units)
    {
        return [SoqlQuery.RecordsOf<AccountSObject>(units), SoqlQuery.RecordsOf<ProfileSObject>(units)];
    }

    /// <summary>
    /// Inbox payload (the source system's shape and names) -> Salesforce sObject (the org's shape and names). Written by
    /// hand, one line per field, so a renamed or retyped field is a compile error, not a silent mismatch. Values
    /// pass through as they are, nulls included (null = empty). The mapping converts only where the two sides differ in type.
    /// </summary>
    public AccountSObject ToAccount(AccountPayload payload)
    {
        return new()
        {
            ExternalId = payload.ExternalId,
            Name = payload.Name,
            Rating = payload.Rating,
            Industry = payload.Industry,
            Description = payload.Description,
            AnnualRevenue = payload.AnnualRevenue,
            NumberOfEmployees = payload.NumberOfEmployees,
            Phone = payload.Phone,
            Website = payload.Website,
            Active = payload.IsActive,
            // A list in the event, a ';'-separated multi-select picklist in Salesforce
            Tags = payload.Tags is null ? null : string.Join(';', payload.Tags),
            CustomerSince = payload.CustomerSince,
            LastContacted = payload.LastContactedAt
        };
    }

    /// <summary>
    /// The profile on its own. <see cref="Map"/>, which has both, fills in what it takes from its account (the default
    /// external id and the Account's key).
    /// </summary>
    public ProfileSObject ToProfile(ProfilePayload payload)
    {
        return new()
        {
            ExternalId = payload.ExternalId,
            Name = payload.Name,
            Tier = payload.Tier,
            Score = payload.Score,
            Discount = payload.DiscountPercent,
            Notes = payload.Notes,
            OptIn = payload.OptIn,
            StartDate = payload.StartDate
        };
    }
}
