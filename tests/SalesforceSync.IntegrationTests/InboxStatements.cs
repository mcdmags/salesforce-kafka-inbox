using SalesforceSync.Data.Inbox;

namespace SalesforceSync.IntegrationTests;

/// <summary>The repository's statements for one inbox table, to play a worker by hand.</summary>
public sealed class InboxStatements
{
    private InboxStatements(string table)
    {
        Table = table;
        Store = For(InboxRepository<AccountInboxRow>.StoreSql);
        Claim = For(InboxRepository<AccountInboxRow>.ClaimSql);
        Done = For(InboxRepository<AccountInboxRow>.DoneSql);
        Fail = For(InboxRepository<AccountInboxRow>.FailSql);
        EndLease = For(InboxRepository<AccountInboxRow>.EndLeaseSql);
        return;

        string For(string template)
        {
            return template.Replace("{inbox}", table);
        }
    }

    public string Table { get; }
    public string Store { get; }
    public string Claim { get; }
    public string Done { get; }
    public string Fail { get; }
    public string EndLease { get; }

    public static InboxStatements For(string table)
    {
        return new(table);
    }
}
