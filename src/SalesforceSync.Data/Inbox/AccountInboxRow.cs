namespace SalesforceSync.Data.Inbox;

/// <summary>A row of <c>sf_account_inbox</c>: events of the account type.</summary>
public sealed class AccountInboxRow : InboxRow, IInboxRow
{
    // External ids per query of a batch's read (a batch is up to 100 entities)
    public static int MaxValuesPerQuery => 100;
    // A margin under Salesforce's 200
    public static int MaxRecordsPerWrite => 180;
}
