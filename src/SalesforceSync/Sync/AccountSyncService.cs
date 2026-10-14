using SalesforceSync.Inbox.Payloads;
using SalesforceSync.Salesforce.SObjects;
using SalesforceSync.Sync.Mapping;

namespace SalesforceSync.Sync;

/// <summary>
/// Account + its Profile__c, written together (one graph); the Profile looks up its Account.
/// Typed event -> mappers -> sObjects -> records. An event without a <c>profile</c> writes only the Account.
/// </summary>
public sealed class AccountSyncService(
    SalesforceBatchUpsertService upsert,
    ISObjectMapper<AccountPayload, AccountSObject> accounts,
    ISObjectMapper<ProfilePayload, ProfileSObject> profiles) : ISalesforceEntitySync
{
    public const string Type = "account";

    private readonly UpsertStrategy<AccountEvent> _strategy = CreateStrategy(accounts, profiles);

    public string EntityType => Type;

    public Task<int> SyncBatchAsync(CancellationToken ct) => upsert.UpsertBatchAsync(_strategy, ct);

    /// <summary>The strategy the engine runs, with the mappers it maps through (injected, so built per instance).</summary>
    public static UpsertStrategy<AccountEvent> CreateStrategy(
        ISObjectMapper<AccountPayload, AccountSObject> accounts,
        ISObjectMapper<ProfilePayload, ProfileSObject> profiles)
        => new(Type, (key, e) => Map(key, e, accounts, profiles), Queries);

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
