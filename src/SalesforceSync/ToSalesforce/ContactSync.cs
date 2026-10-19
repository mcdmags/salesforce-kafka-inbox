using SalesforceSync.Data.Inbox;
using SalesforceSync.Infrastructure.Salesforce;
using SalesforceSync.Sync;

namespace SalesforceSync.ToSalesforce;

/// <summary>
/// Contact of an Account, named by Account's external id (Text foreign key). Account missing in Salesforce → nothing
/// sent, retried until account sync creates it.
/// </summary>
public sealed class ContactSync(SalesforceBatchSyncService batch, SalesforceCalls salesforce, IRepository<ContactInboxRow> inbox) : IEntitySync
{
    public const string Type = ContactEvent.Type;

    private const string ContactObject = "Contact";

    public string EntityType => Type;

    public Task<int> SyncBatchAsync(CancellationToken ct)
    {
        return batch.SyncBatchAsync(ContactEvent.Reader, inbox, Send, ct);
    }

    private async Task<Dictionary<string, List<Failure>>> Send(IReadOnlyList<ReadEvent<ContactEvent>> events, CancellationToken ct)
    {
        var failures = new Dictionary<string, List<Failure>>();
        var deletes = events.Where(e => e.Event.Meta?.IsDelete() == true).ToList();
        var upserts = events.Except(deletes).ToList();

        // Upsert needs Account key + record type
        foreach (var e in upserts.Where(e => string.IsNullOrWhiteSpace(e.Event.Contact?.AccountExternalId)))
        {
            SalesforceCalls.Fail(failures, e.EntityId, SalesforceErrors.InvalidEvent, "accountExternalId is required", permanent: true);
        }

        upserts = [.. upserts.Where(e => !failures.ContainsKey(e.EntityId))];
        var recordType = await salesforce.RecordTypeIdAsync(ContactObject, "Standard", ct);
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
            var c = e.Event.Contact!;
            return (e.EntityId, c.AccountExternalId, Fields: new Dictionary<string, object?>
            {
                ["AccountId__c"] = c.AccountExternalId,
                ["FirstName"] = c.FirstName,
                ["LastName"] = c.LastName,
                ["Email"] = c.Email,
                ["Phone"] = c.Phone,
                ["RecordTypeId"] = recordType
            });
        }).ToList();

        // READ: contacts (to compare; Ids for deletes) + target Accounts (must exist)
        var now = await salesforce.ReadAsync(ContactObject, wanted.SelectMany(w => w.Fields.Keys), events.Select(e => e.EntityId), ct);
        var accounts = await salesforce.ReadAsync("Account", [], wanted.Select(w => w.AccountExternalId!), ct);

        // DECIDE: Account not in Salesforce yet → wait; else create, update or skip
        var sends = new List<(string Key, string ExternalId, Dictionary<string, object?> Fields)>();
        foreach (var (key, account, fields) in wanted)
        {
            if (!accounts.ContainsKey(account!))
            {
                SalesforceCalls.Fail(failures, key, SalesforceErrors.DependencyNotSynced, $"Account {account} is not in Salesforce yet");
            }
            else if (SalesforceCalls.Changes(fields, now.TryGetValue(key, out var row) ? row : null) is { Count: > 0 } changes)
            {
                sends.Add((key, key, changes));
            }
        }

        // WRITE. Delete of a record Salesforce lacks = already deleted
        await salesforce.UpsertAsync(ContactObject, sends, failures, ct);
        await salesforce.DeleteAsync(deletes.Where(e => now.ContainsKey(e.EntityId))
            .Select(e => (e.EntityId, now[e.EntityId].GetProperty("Id").GetString()!)), failures, ct);

        return failures;
    }
}
