using SalesforceSync.Data.Inbox;
using SalesforceSync.Sync;

namespace SalesforceSync.ToSalesforce;

/// <summary>
/// Account_Relationship__c between two Accounts, named by their external ids (Text foreign keys). If either Account
/// isn't in Salesforce yet, nothing is sent and the relationship is retried until the account sync creates it.
/// Like <see cref="AccountSync"/>, the mapping is here and the sObject next to it.
/// </summary>
public sealed class AccountRelationshipSync(SalesforceBatchSyncService engine, IRepository<AccountRelationshipInboxRow> inbox)
    : EntitySync<AccountRelationshipEvent, AccountRelationshipInboxRow>(engine, inbox)
{
    public const string Type = AccountRelationshipEvent.Type;

    protected override EventReader<AccountRelationshipEvent> Reader => AccountRelationshipEvent.Reader;

    /// <summary>
    /// A deleted event deletes the relationship by its key alone. Any other event upserts it, which needs both Accounts' keys.
    /// </summary>
    public override SyncUnit Map(string relationshipKey, AccountRelationshipEvent e)
    {
        if (e.Meta?.IsDelete() == true)
        {
            return new SyncUnit([SObjectRecord.Deleting<AccountRelationshipSObject>("rel", relationshipKey)]);
        }

        var payload = e.AccountRelationship;
        return string.IsNullOrWhiteSpace(payload?.AccountExternalId) || string.IsNullOrWhiteSpace(payload.RelatedAccountExternalId)
            ? throw new ArgumentException("accountExternalId and relatedAccountExternalId are required")
            : new SyncUnit([SObjectRecord.From("rel", ToRelationship(payload))]);
    }

    // Read the relationships (to diff) and the Accounts they point at (to know they exist)
    public override IEnumerable<SoqlQuery> Queries(IReadOnlyCollection<SyncUnit> units)
    {
        return [SoqlQuery.RecordsOf<AccountRelationshipSObject>(units), SoqlQuery.ParentsOf<AccountSObject>(units)];
    }

    /// <summary>Payload -> sObject, one line per field (see <see cref="AccountSync.ToAccount"/>).</summary>
    public AccountRelationshipSObject ToRelationship(AccountRelationshipPayload payload)
    {
        return new()
        {
            ExternalId = payload.ExternalId,
            Name = payload.Name,
            AccountId = payload.AccountExternalId,
            RelatedAccountId = payload.RelatedAccountExternalId,
            Type = payload.Type,
            StartDate = payload.StartDate,
            Active = payload.IsActive
        };
    }
}
