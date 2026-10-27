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

    private async Task<Failures> Send(IReadOnlyList<ReadEvent<ContactEvent>> events, CancellationToken ct)
    {
        var failures = new Failures();
        var deletes = events.Where(e => e.Event.Meta?.IsDelete() == true).ToList();
        var upserts = SalesforceCalls.KeepValid(events.Except(deletes), HasAccount, "accountExternalId is required", failures);

        var accounts = await salesforce.ReadAsync(AccountSync.AccountObject, [], upserts.SelectMany(AccountOf), ct);
        upserts = SalesforceCalls.WithParentsInSalesforce(upserts, AccountOf, accounts, failures);

        var desired = upserts.Select(Desired).ToList();
        var current = await salesforce.ReadAsync(ContactObject, desired.FieldNames(), events.Select(e => e.EntityId), ct);

        await salesforce.UpsertChangedAsync(ContactObject, desired, current, failures, ct);
        await salesforce.DeleteAsync(SalesforceCalls.RecordsToDelete(deletes, current), failures, ct);

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

    private static DesiredRecord Desired(ReadEvent<ContactEvent> e)
    {
        var c = e.Event.Contact!;
        return new DesiredRecord(e.EntityId, e.EntityId, SalesforceFields.Of(new ContactRecord
        {
            AccountId = c.AccountExternalId,
            FirstName = c.FirstName,
            LastName = c.LastName,
            Email = c.Email,
            Phone = c.Phone
        }));
    }
}
