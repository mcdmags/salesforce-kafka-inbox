using SalesforceSync.Data.Inbox;
using SalesforceSync.Sync;

namespace SalesforceSync.Persistence;

/// <summary>
/// accountRelationship events an Account aggregate calls for. Knows aggregate + event, not Salesforce
/// (<see cref="ToSalesforce.AccountRelationshipSync"/>).
/// </summary>
public sealed class AccountRelationshipIngest(IRepository<AccountRelationshipInboxRow> inbox) : IAggregateIngest
{
    public string EntityType => AccountRelationshipEvent.Type;

    public Task<IReadOnlyList<string>> IngestAsync(AggregateChange change, CancellationToken ct)
    {
        return InboxIngest.StoreAsync(Project(change), AccountRelationshipEvent.Reader, inbox, ct);
    }

    /// <summary>New or changed relationships, plus a delete per removed one.</summary>
    internal IEnumerable<AccountRelationshipEvent> Project(AggregateChange change)
    {
        return AggregateProjection.ChildEvents(change, s => s.AccountRelationships, r => r.ExternalId,
            (r, accountKey) => r.AccountExternalId = accountKey, id => new AccountRelationshipPayload { ExternalId = id },
            (meta, r) => new AccountRelationshipEvent(meta, r));
    }
}
