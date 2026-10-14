using SalesforceSync.Inbox;
using SalesforceSync.Inbox.Payloads;
using SalesforceSync.Salesforce.SObjects;
using SalesforceSync.Sync.Mapping;

namespace SalesforceSync.Sync;

/// <summary>
/// Account + its Profile__c, written in the same request (Accounts first); the Profile looks up its Account.
/// Typed event -> mappers -> sObjects -> records. An event without a <c>profile</c> writes only the Account.
/// </summary>
public sealed class AccountSyncService(
    SalesforceBatchUpsertService engine,
    IRepository<AccountInboxRow> inbox,
    ISObjectMapper<AccountPayload, AccountSObject> accounts,
    ISObjectMapper<ProfilePayload, ProfileSObject> profiles)
    : EntitySyncService<AccountEvent, AccountInboxRow>(engine, inbox, CreateStrategy(accounts, profiles), DefaultTopic)
{
    public const string Type = "account";
    public const string DefaultTopic = "account-changes";

    /// <summary>The strategy the engine runs, with the mappers it maps through (injected, so built per instance).</summary>
    public static UpsertStrategy<AccountEvent> CreateStrategy(
        ISObjectMapper<AccountPayload, AccountSObject> accounts,
        ISObjectMapper<ProfilePayload, ProfileSObject> profiles)
        => new(Type, e => e.Account?.ExternalId, e => e.Meta?.Timestamp(), e => e.Meta?.Kind(),
            (key, e) => Map(key, e, accounts, profiles), Queries);

    public static UpsertUnit Map(string accountKey, AccountEvent e,
        ISObjectMapper<AccountPayload, AccountSObject> accounts,
        ISObjectMapper<ProfilePayload, ProfileSObject> profiles)
    {
        var payload = e.Account ?? throw new ArgumentException("account is required");
        var records = new List<SObjectRecord> { SObjectRecord.From("acct", accounts.Map(payload)) };

        if (payload.Profile is not null)
        {
            var profile = profiles.Map(payload.Profile);
            // What the profile takes from its account: its id unless it has its own, and the Account it belongs to
            profile.ExternalId ??= accountKey;
            profile.AccountExternalId = accountKey;
            records.Add(SObjectRecord.From("prof", profile));
        }
        return new UpsertUnit(records);
    }

    public static IEnumerable<SoqlQuery> Queries(IReadOnlyCollection<UpsertUnit> units) =>
    [
        SoqlQuery.RecordsOf<AccountSObject>(units),
        SoqlQuery.RecordsOf<ProfileSObject>(units)
    ];
}
