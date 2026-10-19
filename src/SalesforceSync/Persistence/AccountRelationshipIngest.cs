using SalesforceSync.Data.Inbox;
using SalesforceSync.Sync;

namespace SalesforceSync.Persistence;

/// <summary>
/// The accountRelationship events an Account aggregate calls for Knows the aggregate and the event, nothing of Salesforce (that is <see cref="ToSalesforce.AccountRelationshipSync"/>).
/// </summary>
public sealed class AccountRelationshipIngest(IRepository<AccountRelationshipInboxRow> inbox) : AggregateIngest<AccountRelationshipEvent, AccountRelationshipInboxRow>(inbox)
{
    protected override EventReader<AccountRelationshipEvent> Reader => AccountRelationshipEvent.Reader;

    /// <summary>The relationships of the aggregate that are new or changed, and a delete for each that went.</summary>
    internal override IEnumerable<AccountRelationshipEvent> Project(AggregateChange change)
    {
        return AggregateProjection.ChildEvents(change, s => s.AccountRelationships, r => r.ExternalId,
            (r, accountKey) => r.AccountExternalId = accountKey, id => new AccountRelationshipPayload { ExternalId = id },
            (meta, r) => new AccountRelationshipEvent(meta, r));
    }
}
