using System.Text.Json;
using System.Text.Json.Serialization;

namespace SalesforceSync.Sync;

/// <summary>
/// Inbox event for an account-to-account relationship (entity type "accountRelationship", key =
/// accountRelationship.externalId).
/// <code>{ "meta": { "time": "2026-10-14T01:02:03.456Z" },
///   "accountRelationship": { "externalId": "REL-1", "accountExternalId": "ACC-1", "relatedAccountExternalId": "ACC-2", "Type__c": "Parent" } }</code>
/// Mapping assumptions (change them here and in <see cref="AccountRelationshipSyncService.Map"/>):
/// <list type="bullet">
/// <item><c>accountExternalId</c> and <c>relatedAccountExternalId</c> are the external ids of the two Accounts
/// (Account__c, Related_Account__c). Both are required; an event without them is dead-lettered (INVALID_EVENT).</item>
/// <item>Every other property except <c>externalId</c> is an Account_Relationship__c field, named by its Salesforce
/// API name.</item>
/// </list>
/// </summary>
public sealed record AccountRelationshipEvent(AccountRelationshipData? AccountRelationship);

/// <summary>The <c>accountRelationship</c> object of an <see cref="AccountRelationshipEvent"/>.</summary>
public sealed record AccountRelationshipData
{
    public string? ExternalId { get; init; }
    public string? AccountExternalId { get; init; }
    public string? RelatedAccountExternalId { get; init; }

    /// <summary>Every other property: Account_Relationship__c fields by API name.</summary>
    [JsonExtensionData]
    public Dictionary<string, JsonElement>? Fields { get; init; }
}

/// <summary>
/// Account_Relationship__c between two Accounts, looked up by their external ids. If either Account isn't in
/// Salesforce yet, the relationship is retried until the account sync has created it.
/// </summary>
public sealed class AccountRelationshipSyncService(SalesforceBatchUpsertService upsert) : ISalesforceEntitySync
{
    public const string Type = "accountRelationship";
    public const string RelationshipObject = "Account_Relationship__c";

    public static readonly UpsertStrategy<AccountRelationshipEvent> Strategy = new(Type, Map, Queries);

    public string EntityType => Type;

    public Task<int> SyncBatchAsync(CancellationToken ct) => upsert.UpsertBatchAsync(Strategy, ct);

    public static UpsertUnit Map(string relationshipKey, AccountRelationshipEvent e)
    {
        var rel = e.AccountRelationship;
        if (string.IsNullOrWhiteSpace(rel?.AccountExternalId) || string.IsNullOrWhiteSpace(rel.RelatedAccountExternalId))
            throw new ArgumentException("accountExternalId and relatedAccountExternalId are required");

        return new UpsertUnit(
        [
            new SObjectRecord("rel", RelationshipObject, AccountSyncService.ExternalId, relationshipKey, SObjectFields.From(rel.Fields),
            [
                new SObjectLookup("Account__c", AccountSyncService.AccountObject, AccountSyncService.ExternalId, rel.AccountExternalId),
                new SObjectLookup("Related_Account__c", AccountSyncService.AccountObject, AccountSyncService.ExternalId, rel.RelatedAccountExternalId)
            ])
        ]);
    }

    public static IEnumerable<SoqlQuery> Queries(IReadOnlyCollection<UpsertUnit> units) =>
    [
        new SoqlQuery(RelationshipObject, AccountSyncService.ExternalId, SoqlQuery.ExternalIdsOf(units, RelationshipObject)),
        new SoqlQuery(AccountSyncService.AccountObject, AccountSyncService.ExternalId, SoqlQuery.LookupIdsOf(units, AccountSyncService.AccountObject))
    ];
}
