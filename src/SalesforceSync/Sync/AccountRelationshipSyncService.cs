using SalesforceSync.Inbox.Payloads;
using SalesforceSync.Salesforce.SObjects;
using SalesforceSync.Sync.Mapping;

namespace SalesforceSync.Sync;

/// <summary>
/// Account_Relationship__c between two Accounts, looked up by their external ids. If either Account isn't in
/// Salesforce yet, the relationship is retried until the account sync has created it.
/// </summary>
public sealed class AccountRelationshipSyncService(
    SalesforceBatchUpsertService engine,
    ISObjectMapper<AccountRelationshipPayload, AccountRelationshipSObject> relationships)
    : EntitySyncService<AccountRelationshipEvent>(engine, CreateStrategy(relationships), DefaultTopic)
{
    public const string Type = "accountRelationship";
    public const string InboxTable = "sf_account_relationship_inbox";
    public const string DefaultTopic = "account-relationship-changes";

    /// <summary>The strategy the engine runs, with the mapper it maps through (injected, so built per instance).</summary>
    public static UpsertStrategy<AccountRelationshipEvent> CreateStrategy(
        ISObjectMapper<AccountRelationshipPayload, AccountRelationshipSObject> relationships)
        => new(Type, InboxTable, e => e.AccountRelationship?.ExternalId, e => e.Meta?.Timestamp(), e => e.Meta?.Kind(),
            (key, e) => Map(key, e, relationships), Queries);

    public static UpsertUnit Map(string relationshipKey, AccountRelationshipEvent e,
        ISObjectMapper<AccountRelationshipPayload, AccountRelationshipSObject> relationships)
    {
        var payload = e.AccountRelationship;
        if (string.IsNullOrWhiteSpace(payload?.AccountExternalId) || string.IsNullOrWhiteSpace(payload.RelatedAccountExternalId))
            throw new ArgumentException("accountExternalId and relatedAccountExternalId are required");

        return new UpsertUnit([SObjectRecord.From("rel", relationships.Map(payload))]);
    }

    // What to read: the relationships themselves (to diff) and the Accounts they point at (for their Ids)
    public static IEnumerable<SoqlQuery> Queries(IReadOnlyCollection<UpsertUnit> units) =>
    [
        SoqlQuery.RecordsOf<AccountRelationshipSObject>(units),
        SoqlQuery.LookupTargetsOf<AccountSObject>(units)
    ];
}
