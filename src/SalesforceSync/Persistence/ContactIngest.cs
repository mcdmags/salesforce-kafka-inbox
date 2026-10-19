using SalesforceSync.Data.Inbox;
using SalesforceSync.Sync;

namespace SalesforceSync.Persistence;

/// <summary>
/// Contact events an Account aggregate calls for. Knows aggregate + event, not Salesforce
/// (<see cref="ToSalesforce.ContactSync"/>).
/// </summary>
public sealed class ContactIngest(IRepository<ContactInboxRow> inbox) : IAggregateIngest
{
    public string EntityType => ContactEvent.Type;

    public Task<IReadOnlyList<string>> IngestAsync(AggregateChange change, CancellationToken ct)
    {
        return InboxIngest.StoreAsync(Project(change), ContactEvent.Reader, inbox, ct);
    }

    /// <summary>New or changed contacts, plus a delete per removed one.</summary>
    internal IEnumerable<ContactEvent> Project(AggregateChange change)
    {
        return AggregateProjection.ChildEvents(change, s => s.Contacts, c => c.ExternalId,
            (c, accountKey) => c.AccountExternalId = accountKey, id => new ContactPayload { ExternalId = id },
            (meta, c) => new ContactEvent(meta, c));
    }
}
