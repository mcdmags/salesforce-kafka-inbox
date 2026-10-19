namespace SalesforceSync.Sync;

/// <summary>
/// The contact events an Account aggregate calls for (see <see cref="IAggregateProjection"/>). The ingest half of this type: it
/// knows the aggregate and the event, nothing of Salesforce (that is <see cref="ContactSync"/>).
/// </summary>
public sealed class ContactProjection : IAggregateProjection
{
    public string EntityType => ContactEvent.Type;

    public IEnumerable<string> ProjectJson(AggregateChange change)
    {
        return Project(change).Select(AggregateProjection.Serialize);
    }

    /// <summary>The contacts of the aggregate that are new or changed, and a delete for each that went.</summary>
    public IEnumerable<ContactEvent> Project(AggregateChange change)
    {
        return AggregateProjection.ChildEvents(change, s => s.Contacts, c => c.ExternalId,
            (c, accountKey) => c.AccountExternalId = accountKey, id => new ContactPayload { ExternalId = id },
            (meta, c) => new ContactEvent(meta, c));
    }
}
