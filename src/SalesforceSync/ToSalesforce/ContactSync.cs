using SalesforceSync.Data.Inbox;
using SalesforceSync.Sync;

namespace SalesforceSync.ToSalesforce;

/// <summary>
/// Contact of an Account, named by the Account's external id (a Text foreign key). If the Account isn't in Salesforce
/// yet, nothing is sent and the contact is retried until the account sync creates it. Like
/// <see cref="AccountSync"/>, the mapping is here and the sObject next to it.
/// </summary>
public sealed class ContactSync : IEntitySync
{
    public const string Type = ContactEvent.Type;

    private readonly SalesforceBatchSyncService _engine;
    private readonly IRepository<ContactInboxRow> _inbox;
    private readonly SyncStrategy<ContactEvent> _strategy;

    public ContactSync(SalesforceBatchSyncService engine, IRepository<ContactInboxRow> inbox)
    {
        _engine = engine;
        _inbox = inbox;
        _strategy = new(ContactEvent.Reader, Map, Queries);
    }

    public string EntityType => Type;

    public Task<int> SyncBatchAsync(CancellationToken ct)
    {
        return _engine.SyncBatchAsync(_strategy, _inbox, ct);
    }

    /// <summary>
    /// A deleted event deletes the contact by its key alone. Any other event upserts it, which needs its Account's key.
    /// </summary>
    public SyncUnit Map(string contactKey, ContactEvent e)
    {
        if (e.Meta?.IsDelete() == true)
        {
            return new SyncUnit([SObjectRecord.Deleting<ContactSObject>("contact", contactKey)]);
        }

        var payload = e.Contact;
        return string.IsNullOrWhiteSpace(payload?.AccountExternalId)
            ? throw new ArgumentException("accountExternalId is required")
            : new SyncUnit([SObjectRecord.From("contact", ToContact(payload))]);
    }

    // Read the contacts (to diff) and the Accounts they point at (to know they exist)
    public IEnumerable<SoqlQuery> Queries(IReadOnlyCollection<SyncUnit> units)
    {
        return [SoqlQuery.RecordsOf<ContactSObject>(units), SoqlQuery.ParentsOf<AccountSObject>(units)];
    }

    /// <summary>Payload -> sObject, one line per field (see <see cref="AccountSync.ToAccount"/>).</summary>
    public ContactSObject ToContact(ContactPayload payload)
    {
        return new()
        {
            ExternalId = payload.ExternalId,
            AccountId = payload.AccountExternalId,
            FirstName = payload.FirstName,
            LastName = payload.LastName,
            Email = payload.Email,
            Phone = payload.Phone
        };
    }
}
