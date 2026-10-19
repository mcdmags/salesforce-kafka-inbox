using SalesforceSync.Data.Inbox;
using SalesforceSync.Infrastructure.Salesforce;
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

    private const string RelationshipObject = "Account_Relationship__c";

    public string EntityType => Type;

    public Task<int> SyncBatchAsync(CancellationToken ct)
    {
        return batch.SyncBatchAsync(AccountRelationshipEvent.Reader, inbox, Send, ct);
    }

    private async Task<Dictionary<string, List<Failure>>> Send(IReadOnlyList<ReadEvent<AccountRelationshipEvent>> events, CancellationToken ct)
    {
        var failures = new Dictionary<string, List<Failure>>();
        var deletes = events.Where(e => e.Event.Meta?.IsDelete() == true).ToList();
        var upserts = events.Except(deletes).ToList();

        // Upsert needs both Account keys + record type
        foreach (var e in upserts.Where(e => string.IsNullOrWhiteSpace(e.Event.AccountRelationship?.AccountExternalId)
                                             || string.IsNullOrWhiteSpace(e.Event.AccountRelationship.RelatedAccountExternalId)))
        {
            SalesforceCalls.Fail(failures, e.EntityId, SalesforceErrors.InvalidEvent, "accountExternalId and relatedAccountExternalId are required", permanent: true);
        }

        upserts = [.. upserts.Where(e => !failures.ContainsKey(e.EntityId))];
        var recordType = await salesforce.RecordTypeIdAsync(RelationshipObject, "Standard", ct);
        if (recordType is null)
        {
            foreach (var e in upserts)
            {
                SalesforceCalls.Fail(failures, e.EntityId, SalesforceErrors.InvalidRecordType, "the record type is not in the org");
            }

            upserts = [];
        }

        // Wanted record per upsert, Salesforce field names
        var wanted = upserts.Select(e =>
        {
            var r = e.Event.AccountRelationship!;
            return (e.EntityId, r.AccountExternalId, r.RelatedAccountExternalId, Fields: new Dictionary<string, object?>
            {
                ["Name"] = r.Name,
                ["AccountId__c"] = r.AccountExternalId,
                ["RelatedAccountId__c"] = r.RelatedAccountExternalId,
                ["Type__c"] = r.Type,
                ["Start_Date__c"] = r.StartDate,
                ["Active__c"] = r.IsActive,
                ["RecordTypeId"] = recordType
            });
        }).ToList();

        // READ: relationships (to compare; Ids for deletes) + target Accounts (must exist)
        var now = await salesforce.ReadAsync(RelationshipObject, wanted.SelectMany(w => w.Fields.Keys), events.Select(e => e.EntityId), ct);
        var accounts = await salesforce.ReadAsync("Account", [], wanted.SelectMany(w => new[] { w.AccountExternalId!, w.RelatedAccountExternalId! }), ct);

        // DECIDE: Account not in Salesforce yet → wait; else create, update or skip
        var sends = new List<(string Key, string ExternalId, Dictionary<string, object?> Fields)>();
        foreach (var (key, account, related, fields) in wanted)
        {
            if (!accounts.ContainsKey(account!) || !accounts.ContainsKey(related!))
            {
                SalesforceCalls.Fail(failures, key, SalesforceErrors.DependencyNotSynced, $"Account {account} or {related} is not in Salesforce yet");
            }
            else if (SalesforceCalls.Changes(fields, now.TryGetValue(key, out var row) ? row : null) is { Count: > 0 } changes)
            {
                sends.Add((key, key, changes));
            }
        }

        // WRITE. Delete of a record Salesforce lacks = already deleted
        await salesforce.UpsertAsync(RelationshipObject, sends, failures, ct);
        await salesforce.DeleteAsync(deletes.Where(e => now.ContainsKey(e.EntityId))
            .Select(e => (e.EntityId, now[e.EntityId].GetProperty("Id").GetString()!)), failures, ct);

        return failures;
    }
}
