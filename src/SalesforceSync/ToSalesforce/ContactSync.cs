using SalesforceSync.Data.Inbox;
using SalesforceSync.Sync;

namespace SalesforceSync.ToSalesforce;

/// <summary>
/// Contact of an Account, named by Account's external id (Text foreign key). Account missing in Salesforce → nothing
/// sent, retried until account sync creates it.
/// </summary>
public sealed class ContactSync(SalesforceBatchSyncService batch, SalesforceCalls salesforce, IRepository<ContactInboxRow> inbox) : IEntitySync
{
    public const string Type = ContactEvent.Type;

    private static readonly SalesforceObject ContactObject = SalesforceObject.Of<ContactRecord>();

    public string EntityType => Type;

    public Task<int> SyncBatchAsync(CancellationToken ct)
    {
        return batch.SyncBatchAsync(ContactEvent.Reader, inbox, Send, ct);
    }

    private async Task<Dictionary<string, List<Failure>>> Send(IReadOnlyList<ReadEvent<ContactEvent>> events, CancellationToken ct)
    {
        var failures = new Dictionary<string, List<Failure>>();
        var deletes = events.Where(e => e.Event.Meta?.IsDelete() == true).ToList();
        var upserts = SalesforceCalls.KeepValid(events.Except(deletes), HasAccount, "accountExternalId is required", failures);

        var accounts = await salesforce.ReadAsync(AccountSync.AccountObject, [], upserts.SelectMany(AccountOf), ct);
        upserts = SalesforceCalls.WithParentsInSalesforce(upserts, AccountOf, accounts, failures);

        var wanted = upserts.Select(Wanted).ToList();
        var now = await salesforce.ReadCurrentAsync(ContactObject, wanted, events.Select(e => e.EntityId), ct);

        await salesforce.UpsertAsync(ContactObject, SalesforceCalls.Changed(wanted, now), failures, ct);
        await salesforce.DeleteAsync(SalesforceCalls.Existing(deletes, now), failures, ct);

        return failures;
    }

    private static bool HasAccount(ReadEvent<ContactEvent> e)
    {
        return !string.IsNullOrWhiteSpace(e.Event.Contact?.AccountExternalId);
    }

    private static string[] AccountOf(ReadEvent<ContactEvent> e)
    {
        return [e.Event.Contact!.AccountExternalId!];
    }

    private static WantedRecord Wanted(ReadEvent<ContactEvent> e)
    {
        var c = e.Event.Contact!;
        return new WantedRecord(e.EntityId, e.EntityId, SalesforceFields.Of(new ContactRecord
        {
            AccountId = c.AccountExternalId,
            FirstName = c.FirstName,
            LastName = c.LastName,
            Email = c.Email,
            Phone = c.Phone
        }));
    }
}
