using System.Text.Json;

namespace SalesforceSync.Sync;

/// <summary>
/// Inbox payload of an account-to-account relationship change (entity key = the relationship's external id).
/// <code>{ "version": 3, "accountExternalId": "ACC-1", "relatedAccountExternalId": "ACC-2", "fields": { "Type__c": "Parent" } }</code>
/// </summary>
public sealed record AccountRelationshipChangedEvent(
    string? AccountExternalId,
    string? RelatedAccountExternalId,
    Dictionary<string, JsonElement>? Fields);

/// <summary>
/// Account_Relationship__c between two Accounts, looked up by their external ids. If either Account isn't in
/// Salesforce yet, the relationship is retried until the account sync has created it.
/// </summary>
public sealed class AccountRelationshipSyncService(SalesforceBatchUpsertService upsert) : ISalesforceEntitySync
{
    public const string Type = "account-relationship";
    public const string RelationshipObject = "Account_Relationship__c";

    public static readonly UpsertStrategy<AccountRelationshipChangedEvent> Strategy = new(Type, Map, Queries);

    public string EntityType => Type;

    public Task<int> SyncBatchAsync(CancellationToken ct) => upsert.UpsertBatchAsync(Strategy, ct);

    public static UpsertUnit Map(string relationshipKey, AccountRelationshipChangedEvent e)
    {
        if (string.IsNullOrWhiteSpace(e.AccountExternalId) || string.IsNullOrWhiteSpace(e.RelatedAccountExternalId))
            throw new ArgumentException("accountExternalId and relatedAccountExternalId are required");

        return new UpsertUnit(
        [
            new SObjectRecord("rel", RelationshipObject, AccountSyncService.ExternalId, relationshipKey, SObjectFields.From(e.Fields),
            [
                new SObjectLookup("Account__c", AccountSyncService.AccountObject, AccountSyncService.ExternalId, e.AccountExternalId),
                new SObjectLookup("Related_Account__c", AccountSyncService.AccountObject, AccountSyncService.ExternalId, e.RelatedAccountExternalId)
            ])
        ]);
    }

    public static IEnumerable<SoqlQuery> Queries(IReadOnlyCollection<UpsertUnit> units) =>
    [
        new SoqlQuery(RelationshipObject, AccountSyncService.ExternalId, SoqlQuery.ExternalIdsOf(units, RelationshipObject)),
        new SoqlQuery(AccountSyncService.AccountObject, AccountSyncService.ExternalId, SoqlQuery.LookupIdsOf(units, AccountSyncService.AccountObject))
    ];
}
