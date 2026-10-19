namespace SalesforceSync.Sync;

/// <summary>
/// The account events an Account aggregate calls for (see <see cref="IAggregateProjection"/>). The ingest half of this type: it
/// knows the aggregate and the event, nothing of Salesforce (that is <see cref="AccountSync"/>).
/// </summary>
public sealed class AccountProjection : IAggregateProjection
{
    public string EntityType => AccountEvent.Type;

    public IEnumerable<string> ProjectJson(AggregateChange change)
    {
        return Project(change).Select(AggregateProjection.Serialize);
    }

    /// <summary>
    /// The Account's event: the Account (with its Profile) when it changed or Before is missing; its delete, with its
    /// Profile if that has a key of its own, when it is gone.
    /// </summary>
    public IEnumerable<AccountEvent> Project(AggregateChange change)
    {
        if (change.IsDelete)
        {
            var account = new AccountPayload { ExternalId = change.AccountKey };
            if (change.DeletedFrom.Profile?.ExternalId is { } profileKey)
            {
                account.Profile = new ProfilePayload { ExternalId = profileKey };
            }

            yield return new AccountEvent(change.MetaOf("deleted"), account);
        }
        else if (change.Before is null || AggregateProjection.Serialize<AccountPayload>(change.Before) != AggregateProjection.Serialize<AccountPayload>(change.After!))
        {
            yield return new AccountEvent(change.MetaOf(change.Before is null ? "inserted" : "updated"), change.After);
        }
    }
}
