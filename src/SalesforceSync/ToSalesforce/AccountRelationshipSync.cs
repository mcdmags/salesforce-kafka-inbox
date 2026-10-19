using SalesforceSync.Data.Inbox;
using SalesforceSync.Sync;

namespace SalesforceSync.ToSalesforce;

/// <summary>
/// Account_Relationship__c between two Accounts, named by their external ids (Text foreign keys). If either Account
/// isn't in Salesforce yet, nothing is sent and the relationship is retried until the account sync creates it.
/// </summary>
public sealed class AccountRelationshipSync(SalesforceBatchSyncService batch, RecordWriter writer, IRepository<AccountRelationshipInboxRow> inbox) : IEntitySync
{
    public const string Type = AccountRelationshipEvent.Type;

    public string EntityType => Type;

    public Task<int> SyncBatchAsync(CancellationToken ct)
    {
        return batch.SyncBatchAsync(AccountRelationshipEvent.Reader, inbox, Send, ct);
    }

    /// <summary>What this type does with a batch: map its events to records and let the writer send them.</summary>
    private Task<Dictionary<string, List<Failure>>> Send(IReadOnlyList<ReadEvent<AccountRelationshipEvent>> events, CancellationToken ct)
    {
        return writer.WriteAsync(events, Map, AccountRelationshipInboxRow.MaxValuesPerQuery, AccountRelationshipInboxRow.MaxRecordsPerWrite, ct);
    }

    /// <summary>
    /// A deleted event deletes the relationship by its key alone. Any other event upserts it, which needs both Accounts' keys.
    /// </summary>
    private static IReadOnlyList<Rec> Map(string relationshipKey, AccountRelationshipEvent e)
    {
        if (e.Meta?.IsDelete() == true)
        {
            return [Rec.Deleting("Account_Relationship__c", relationshipKey)];
        }

        var r = e.AccountRelationship;
        return string.IsNullOrWhiteSpace(r?.AccountExternalId) || string.IsNullOrWhiteSpace(r.RelatedAccountExternalId)
            ? throw new ArgumentException("accountExternalId and relatedAccountExternalId are required")
            :
            [
                new Rec("Account_Relationship__c", relationshipKey, new Dictionary<string, object?>
                {
                    ["Name"] = r.Name,
                    ["AccountId__c"] = r.AccountExternalId,
                    ["RelatedAccountId__c"] = r.RelatedAccountExternalId,
                    ["Type__c"] = r.Type,
                    ["Start_Date__c"] = r.StartDate,
                    ["Active__c"] = r.IsActive
                }, RecordType: "Standard", Parents:
                [
                    new Parent("AccountId__c", "Account", r.AccountExternalId),
                    new Parent("RelatedAccountId__c", "Account", r.RelatedAccountExternalId)
                ])
            ];
    }
}
