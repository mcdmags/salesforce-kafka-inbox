namespace SalesforceSync.Data.Inbox;

/// <summary>A row of <c>sf_account_relationship_inbox</c>: events of the account relationship type.</summary>
public sealed class AccountRelationshipInboxRow : InboxRow, IInboxRow
{
    // A full batch's read in one Composite request: 5 queries, up to 3 ids per relationship (itself, two Accounts)
    public static int MaxValuesPerQuery => 100;
    // A margin under Salesforce's 200
    public static int MaxRecordsPerWrite => 180;
}
