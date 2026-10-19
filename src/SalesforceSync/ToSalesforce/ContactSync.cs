using SalesforceSync.Data.Inbox;
using SalesforceSync.Sync;

namespace SalesforceSync.ToSalesforce;

/// <summary>
/// Contact of an Account, named by the Account's external id (a Text foreign key). If the Account isn't in Salesforce
/// yet, nothing is sent and the contact is retried until the account sync creates it. Like
/// <see cref="AccountSync"/>, the mapping is here and the sObject next to it.
/// </summary>
public sealed class ContactSync(SalesforceBatchSyncService engine, IRepository<ContactInboxRow> inbox)
    : EntitySync<ContactEvent, ContactInboxRow>(engine, inbox)
{
    public const string Type = ContactEvent.Type;

    protected override EventReader<ContactEvent> Reader => ContactEvent.Reader;

    /// <summary>
    /// A deleted event deletes the contact by its key alone. Any other event upserts it, which needs its Account's key.
    /// </summary>
    public override SyncUnit Map(string contactKey, ContactEvent e)
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
    public override IEnumerable<SoqlQuery> Queries(IReadOnlyCollection<SyncUnit> units)
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
