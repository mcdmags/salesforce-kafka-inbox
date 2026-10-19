using SalesforceSync.Data.Inbox;
using SalesforceSync.Sync;

namespace SalesforceSync.Persistence;

/// <summary>
/// Account events an Account aggregate calls for. Knows aggregate + event, not Salesforce
/// (<see cref="ToSalesforce.AccountSync"/>).
/// </summary>
public sealed class AccountIngest(IRepository<AccountInboxRow> inbox) : IAggregateIngest
{
    public string EntityType => AccountEvent.Type;

    public Task<IReadOnlyList<string>> IngestAsync(AggregateChange change, CancellationToken ct)
    {
        return InboxIngest.StoreAsync(Project(change), AccountEvent.Reader, inbox, ct);
    }

    /// <summary>
    /// Account event: Account (with Profile) when changed or Before missing; delete (with Profile if it has own key)
    /// when gone.
    /// </summary>
    internal IEnumerable<AccountEvent> Project(AggregateChange change)
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
