using Microsoft.Extensions.Options;
using SalesforceSync.Data.Inbox;
using SalesforceSync.FakeSalesforce;
using SalesforceSync.Infrastructure.Salesforce;
using SalesforceSync.ToSalesforce;

namespace SalesforceSync.Tests;

/// <summary>
/// Three syncs over in-memory inboxes and a mock org: event synced as a worker syncs a claimed row (claim, parse,
/// read, decide, write, record), no database.
/// </summary>
internal sealed class SyncHarness
{
    public SyncHarness(FakeSalesforceStore org, SalesforceStep.NotPaused? limiter = null)
    {
        Org = org;
        var http = SalesforceStep.Http(new FakeSalesforceHandler(org));
        Calls = SalesforceStep.Calls(http, Options.Create(new SalesforceOptions()));
        var flow = SalesforceStep.Flow(limiter);
        Account = new AccountSync(flow, Calls, Accounts);
        Relationship = new AccountRelationshipSync(flow, Calls, Relationships);
        Contact = new ContactSync(flow, Calls, Contacts);
    }

    public SyncHarness() : this(new FakeSalesforceStore(new FakeSalesforceOptions { EnforceDailyLimit = false }))
    {
    }

    public FakeSalesforceStore Org { get; }
    public SalesforceCalls Calls { get; }
    public InMemoryInbox<AccountInboxRow> Accounts { get; } = new();
    public InMemoryInbox<AccountRelationshipInboxRow> Relationships { get; } = new();
    public InMemoryInbox<ContactInboxRow> Contacts { get; } = new();
    public AccountSync Account { get; }
    public AccountRelationshipSync Relationship { get; }
    public ContactSync Contact { get; }

    /// <summary>Stores an event in the type's inbox, syncs a batch.</summary>
    /// <returns>Rows processed.</returns>
    public Task<int> SyncAsync(string type, string key, string json)
    {
        var time = Events.TimeOf(json);
        return type switch
        {
            AccountSync.Type => SyncAsync(Account, Accounts, key, time, json),
            AccountRelationshipSync.Type => SyncAsync(Relationship, Relationships, key, time, json),
            _ => SyncAsync(Contact, Contacts, key, time, json)
        };
    }

    private static async Task<int> SyncAsync<TRow>(IEntitySync sync, InMemoryInbox<TRow> inbox, string key, DateTimeOffset time, string json)
        where TRow : InboxRow, IInboxRow, new()
    {
        await inbox.AddAsync(key, time, json, CancellationToken.None);
        return await sync.SyncBatchAsync(CancellationToken.None);
    }
}