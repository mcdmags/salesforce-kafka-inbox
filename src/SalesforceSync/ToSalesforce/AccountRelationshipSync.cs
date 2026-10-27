using SalesforceSync.Data.Inbox;
using SalesforceSync.Sync;

namespace SalesforceSync.ToSalesforce;

/// <summary>
/// Account_Relationship__c between two Accounts, named by external ids (Text foreign keys). Either Account missing in
/// Salesforce → nothing sent, retried until account sync creates it.
/// </summary>
public sealed class AccountRelationshipSync(SalesforceBatchSyncService batch, SalesforceCalls salesforce, IRepository<AccountRelationshipInboxRow> inbox)
    : IEntitySync
{
    public const string Type = AccountRelationshipEvent.Type;

    private static readonly SalesforceObject RelationshipObject = SalesforceObject.Of<AccountRelationshipRecord>();

    public string EntityType => Type;

    public Task<int> SyncBatchAsync(CancellationToken ct)
    {
        return batch.SyncBatchAsync(AccountRelationshipEvent.Reader, inbox, Send, ct);
    }

    private async Task<Failures> Send(IReadOnlyList<ReadEvent<AccountRelationshipEvent>> events, CancellationToken ct)
    {
        var failures = new Failures();
        var deletes = events.Where(e => e.Event.Meta?.IsDelete() == true).ToList();
        var upserts = SalesforceCalls.KeepValid(events.Except(deletes), HasBothAccounts, "accountExternalId and relatedAccountExternalId are required", failures);

        var accounts = await salesforce.ReadAsync(AccountSync.AccountObject, [], upserts.SelectMany(AccountsOf), ct);
        upserts = SalesforceCalls.WithParentsInSalesforce(upserts, AccountsOf, accounts, failures);

        var desired = upserts.Select(Desired).ToList();
        var current = await salesforce.ReadAsync(RelationshipObject, desired.FieldNames(), events.Select(e => e.EntityId), ct);

        await salesforce.UpsertChangedAsync(RelationshipObject, desired, current, failures, ct);
        await salesforce.DeleteAsync(SalesforceCalls.RecordsToDelete(deletes, current), failures, ct);

        return failures;
    }

    private static bool HasBothAccounts(ReadEvent<AccountRelationshipEvent> e)
    {
        return !string.IsNullOrWhiteSpace(e.Event.AccountRelationship?.AccountExternalId)
               && !string.IsNullOrWhiteSpace(e.Event.AccountRelationship.RelatedAccountExternalId);
    }

    private static string[] AccountsOf(ReadEvent<AccountRelationshipEvent> e)
    {
        return [e.Event.AccountRelationship!.AccountExternalId!, e.Event.AccountRelationship.RelatedAccountExternalId!];
    }

    private static DesiredRecord Desired(ReadEvent<AccountRelationshipEvent> e)
    {
        var r = e.Event.AccountRelationship!;
        return new DesiredRecord(e.EntityId, e.EntityId, SalesforceFields.Of(new AccountRelationshipRecord
        {
            Name = r.Name,
            AccountId = r.AccountExternalId,
            RelatedAccountId = r.RelatedAccountExternalId,
            Type = r.Type,
            StartDate = r.StartDate,
            Active = r.IsActive
        }));
    }
}
