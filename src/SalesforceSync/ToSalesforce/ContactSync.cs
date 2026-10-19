using SalesforceSync.Data.Inbox;
using SalesforceSync.Sync;

namespace SalesforceSync.ToSalesforce;

/// <summary>
/// Contact of an Account, named by the Account's external id (a Text foreign key). If the Account isn't in Salesforce
/// yet, nothing is sent and the contact is retried until the account sync creates it.
/// </summary>
public sealed class ContactSync(SalesforceBatchSyncService batch, RecordWriter writer, IRepository<ContactInboxRow> inbox) : IEntitySync
{
    public const string Type = ContactEvent.Type;

    public string EntityType => Type;

    public Task<int> SyncBatchAsync(CancellationToken ct)
    {
        return batch.SyncBatchAsync(ContactEvent.Reader, inbox, Send, ct);
    }

    /// <summary>What this type does with a batch: map its events to records and let the writer send them.</summary>
    private Task<Dictionary<string, List<Failure>>> Send(IReadOnlyList<ReadEvent<ContactEvent>> events, CancellationToken ct)
    {
        return writer.WriteAsync(events, Map, ContactInboxRow.MaxValuesPerQuery, ContactInboxRow.MaxRecordsPerWrite, ct);
    }

    /// <summary>
    /// A deleted event deletes the contact by its key alone. Any other event upserts it, which needs its Account's key.
    /// </summary>
    private static IReadOnlyList<Rec> Map(string contactKey, ContactEvent e)
    {
        if (e.Meta?.IsDelete() == true)
        {
            return [Rec.Deleting("Contact", contactKey)];
        }

        var c = e.Contact;
        return string.IsNullOrWhiteSpace(c?.AccountExternalId)
            ? throw new ArgumentException("accountExternalId is required")
            :
            [
                new Rec("Contact", contactKey, new Dictionary<string, object?>
                {
                    ["AccountId__c"] = c.AccountExternalId,
                    ["FirstName"] = c.FirstName,
                    ["LastName"] = c.LastName,
                    ["Email"] = c.Email,
                    ["Phone"] = c.Phone
                }, RecordType: "Standard", Parents: [new Parent("AccountId__c", "Account", c.AccountExternalId)])
            ];
    }
}
