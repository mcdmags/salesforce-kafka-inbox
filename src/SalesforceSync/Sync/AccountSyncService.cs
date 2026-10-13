using System.Text.Json;

namespace SalesforceSync.Sync;

/// <summary>
/// Inbox payload of an account change. Field maps use Salesforce API names; absent fields are unchanged.
/// <code>{ "version": 7, "profileExternalId": "PRF-1", "account": { "Name": "Acme" }, "profile": { "Tier__c": "Gold" } }</code>
/// </summary>
public sealed record AccountChangedEvent(
    string? ProfileExternalId,
    Dictionary<string, JsonElement>? Account,
    Dictionary<string, JsonElement>? Profile);

/// <summary>Account + its Profile__c, written together (one graph); the Profile looks up its Account.</summary>
public sealed class AccountSyncService(SalesforceBatchUpsertService upsert) : ISalesforceEntitySync
{
    public const string Type = "account";
    public const string AccountObject = "Account";
    public const string ProfileObject = "Profile__c";
    public const string ExternalId = "External_Id__c";

    public static readonly UpsertStrategy<AccountChangedEvent> Strategy = new(Type, Map, Queries);

    public string EntityType => Type;

    public Task<int> SyncBatchAsync(CancellationToken ct) => upsert.UpsertBatchAsync(Strategy, ct);

    public static UpsertUnit Map(string accountKey, AccountChangedEvent e) => new(
    [
        new SObjectRecord("acct", AccountObject, ExternalId, accountKey, SObjectFields.From(e.Account)),
        new SObjectRecord("prof", ProfileObject, ExternalId, e.ProfileExternalId ?? accountKey, SObjectFields.From(e.Profile),
            [new SObjectLookup("Account__c", AccountObject, ExternalId, accountKey)])
    ]);

    public static IEnumerable<SoqlQuery> Queries(IReadOnlyCollection<UpsertUnit> units) =>
    [
        new SoqlQuery(AccountObject, ExternalId, SoqlQuery.ExternalIdsOf(units, AccountObject)),
        new SoqlQuery(ProfileObject, ExternalId, SoqlQuery.ExternalIdsOf(units, ProfileObject))
    ];
}
