namespace SalesforceSync.Sync;

/// <summary>
/// The accountRelationship events an Account aggregate calls for (see <see cref="IAggregateProjection"/>). The ingest half of this type: it
/// knows the aggregate and the event, nothing of Salesforce (that is <see cref="AccountRelationshipSync"/>).
/// </summary>
public sealed class AccountRelationshipProjection : IAggregateProjection
{
    public string EntityType => AccountRelationshipEvent.Type;

    public IEnumerable<string> ProjectJson(AggregateChange change)
    {
        return Project(change).Select(AggregateProjection.Serialize);
    }

    /// <summary>The relationships of the aggregate that are new or changed, and a delete for each that went.</summary>
    public IEnumerable<AccountRelationshipEvent> Project(AggregateChange change)
    {
        return AggregateProjection.ChildEvents(change, s => s.AccountRelationships, r => r.ExternalId,
            (r, accountKey) => r.AccountExternalId = accountKey, id => new AccountRelationshipPayload { ExternalId = id },
            (meta, r) => new AccountRelationshipEvent(meta, r));
    }
}
