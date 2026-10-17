namespace SalesforceSync.Data.Inbox;

/// <summary>A row of <c>sf_account_inbox</c>: events of the account type.</summary>
public sealed class AccountInboxRow : InboxRow, IInboxRow
{
    // A full batch's read in one Composite request (5 queries)
    public static int MaxValuesPerQuery => 100;
    // A margin under Salesforce's 200
    public static int MaxRecordsPerWrite => 180;
}
