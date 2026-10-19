using SalesforceSync.Data.Inbox;
using SalesforceSync.Sync;

namespace SalesforceSync.Persistence;

/// <summary>
/// The accountRelationship events an Account aggregate calls for. Knows the aggregate and the event, nothing of Salesforce (that is <see cref="ToSalesforce.AccountRelationshipSync"/>).
/// </summary>
public sealed class AccountRelationshipIngest(IRepository<AccountRelationshipInboxRow> inbox) : IAggregateIngest
{
    public string EntityType => AccountRelationshipEvent.Type;

    public Task<IReadOnlyList<string>> IngestAsync(AggregateChange change, CancellationToken ct)
    {
        return InboxIngest.StoreAsync(Project(change), AccountRelationshipEvent.Reader, inbox, ct);
    }

    /// <summary>The relationships of the aggregate that are new or changed, and a delete for each that went.</summary>
    internal IEnumerable<AccountRelationshipEvent> Project(AggregateChange change)
    {
        return AggregateProjection.ChildEvents(change, s => s.AccountRelationships, r => r.ExternalId,
            (r, accountKey) => r.AccountExternalId = accountKey, id => new AccountRelationshipPayload { ExternalId = id },
            (meta, r) => new AccountRelationshipEvent(meta, r));
    }
}
