using SalesforceSync.Data.Inbox;
using SalesforceSync.Sync;

namespace SalesforceSync.Persistence;

/// <summary>
/// The contact events an Account aggregate calls for Knows the aggregate and the event, nothing of Salesforce (that is <see cref="ToSalesforce.ContactSync"/>).
/// </summary>
public sealed class ContactIngest(IRepository<ContactInboxRow> inbox) : AggregateIngest<ContactEvent, ContactInboxRow>(inbox)
{
    protected override EventReader<ContactEvent> Reader => ContactEvent.Reader;

    /// <summary>The contacts of the aggregate that are new or changed, and a delete for each that went.</summary>
    internal override IEnumerable<ContactEvent> Project(AggregateChange change)
    {
        return AggregateProjection.ChildEvents(change, s => s.Contacts, c => c.ExternalId,
            (c, accountKey) => c.AccountExternalId = accountKey, id => new ContactPayload { ExternalId = id },
            (meta, c) => new ContactEvent(meta, c));
    }
}
