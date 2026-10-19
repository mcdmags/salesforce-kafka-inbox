using SalesforceSync.Data.Inbox;
using SalesforceSync.Sync;

namespace SalesforceSync.ToSalesforce;

/// <summary>
/// Account + its Profile__c, written in the same batch (Accounts first). The Profile names its Account by the Account's
/// external id (a Text foreign key). An event without a <c>profile</c> writes only the Account. All this type says is
/// which records an event asks for (<see cref="Map"/>); the writer sends them.
/// </summary>
public sealed class AccountSync(SalesforceBatchSyncService batch, RecordWriter writer, IRepository<AccountInboxRow> inbox) : IEntitySync
{
    public const string Type = AccountEvent.Type;

    public string EntityType => Type;

    public Task<int> SyncBatchAsync(CancellationToken ct)
    {
        return batch.SyncBatchAsync(AccountEvent.Reader, inbox, Send, ct);
    }

    /// <summary>What this type does with a batch: map its events to records and let the writer send them.</summary>
    private Task<Dictionary<string, List<Failure>>> Send(IReadOnlyList<ReadEvent<AccountEvent>> events, CancellationToken ct)
    {
        return writer.WriteAsync(events, Map, AccountInboxRow.MaxValuesPerQuery, AccountInboxRow.MaxRecordsPerWrite, ct);
    }

    /// <summary>
    /// A deleted event deletes the Profile, then the Account (children first): an org whose automation refuses to
    /// delete an Account that records still name accepts that order. Salesforce itself refuses nothing for Text
    /// keys; the contacts' and relationships' own deletes come through their inboxes.
    /// Any other event upserts the Account and, if the event has one, its Profile.
    /// </summary>
    private static IReadOnlyList<Rec> Map(string accountKey, AccountEvent e)
    {
        if (e.Meta?.IsDelete() == true)
        {
            return [Rec.Deleting("Profile__c", e.Account?.Profile?.ExternalId ?? accountKey), Rec.Deleting("Account", accountKey)];
        }

        var a = e.Account ?? throw new ArgumentException("account is required");
        List<Rec> records =
        [
            new Rec("Account", accountKey, new Dictionary<string, object?>
            {
                ["Name"] = a.Name,
                ["Rating"] = a.Rating,
                ["Industry"] = a.Industry,
                ["Description"] = a.Description,
                ["AnnualRevenue"] = a.AnnualRevenue,
                ["NumberOfEmployees"] = a.NumberOfEmployees,
                ["Phone"] = a.Phone,
                ["Website"] = a.Website,
                ["Active__c"] = a.IsActive,
                // A list in the event, a ';'-separated multi-select picklist in Salesforce
                ["Tags__c"] = a.Tags is null ? null : string.Join(';', a.Tags),
                ["Customer_Since__c"] = a.CustomerSince,
                ["Last_Contacted__c"] = a.LastContactedAt
            }, RecordType: "Business")
        ];

        if (a.Profile is { } p)
        {
            // Its id defaults to the account's, and it belongs to the event's account. Its Account is written in the
            // same batch, before it, so it has no parent to wait for
            records.Add(new Rec("Profile__c", p.ExternalId ?? accountKey, new Dictionary<string, object?>
            {
                ["Name"] = p.Name,
                ["AccountId__c"] = accountKey,
                ["Tier__c"] = p.Tier,
                ["Score__c"] = p.Score,
                ["Discount__c"] = p.DiscountPercent,
                ["Notes__c"] = p.Notes,
                ["Opt_In__c"] = p.OptIn,
                ["Start_Date__c"] = p.StartDate
            }, RecordType: "Standard"));
        }

        return records;
    }
}
