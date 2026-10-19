using SalesforceSync.Data.Inbox;
using SalesforceSync.Sync;

namespace SalesforceSync.Persistence;

/// <summary>
/// The account events an Account aggregate calls for Knows the aggregate and the event, nothing of Salesforce (that is <see cref="ToSalesforce.AccountSync"/>).
/// </summary>
public sealed class AccountIngest(IRepository<AccountInboxRow> inbox) : AggregateIngest<AccountEvent, AccountInboxRow>(inbox)
{
    protected override EventReader<AccountEvent> Reader => AccountEvent.Reader;

    /// <summary>
    /// The Account's event: the Account (with its Profile) when it changed or Before is missing; its delete, with its
    /// Profile if that has a key of its own, when it is gone.
    /// </summary>
    internal override IEnumerable<AccountEvent> Project(AggregateChange change)
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
