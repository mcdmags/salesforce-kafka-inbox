using SalesforceSync.ToSalesforce;

namespace SalesforceSync.Tests;

public sealed class AccountRelationshipSyncTests
{
    private readonly SyncHarness _sync = new();

    private Task<int> SyncAsync(string json)
    {
        return _sync.SyncAsync(AccountRelationshipSync.Type, "REL-1", json);
    }

    private async Task AccountsAreInSalesforceAsync(params string[] keys)
    {
        foreach (var key in keys)
        {
            await _sync.SyncAsync(AccountSync.Type, key, Events.Account(key, 1, "inserted"));
        }
    }

    [Fact]
    public async Task A_relationship_between_two_accounts_in_salesforce_is_created()
    {
        await AccountsAreInSalesforceAsync("ACC-1", "ACC-2");

        await SyncAsync(Events.Relationship("REL-1", "ACC-1", "ACC-2", 1, "inserted", r => r["type"] = "Parent"));

        var relationship = _sync.Org.FindByExternalId("Account_Relationship__c", "REL-1")!;
        (relationship["AccountId__c"]!.GetValue<string>(), relationship["RelatedAccountId__c"]!.GetValue<string>(), relationship["Type__c"]!.GetValue<string>())
            .Should().Be(("ACC-1", "ACC-2", "Parent"));
        _sync.Relationships.Synced.Should().Equal("REL-1");
    }

    [Theory]
    [InlineData("ACC-1", "ACC-MISSING")]
    [InlineData("ACC-MISSING", "ACC-2")]
    public async Task A_relationship_whose_account_is_not_in_salesforce_yet_waits_for_it_and_is_sent_once_it_is(string account, string related)
    {
        await AccountsAreInSalesforceAsync("ACC-1", "ACC-2");
        await SyncAsync(Events.Relationship("REL-1", account, related, 1, "inserted"));

        _sync.Relationships.Failed.Should().ContainSingle().Which.Error.Should().Contain("DEPENDENCY_NOT_SYNCED").And.Contain("ACC-MISSING");
        _sync.Org.FindByExternalId("Account_Relationship__c", "REL-1").Should().BeNull("nothing is sent while a parent is missing");

        await AccountsAreInSalesforceAsync("ACC-MISSING");
        await SyncAsync(Events.Relationship("REL-1", account, related, 2, "inserted"));

        _sync.Org.FindByExternalId("Account_Relationship__c", "REL-1").Should().NotBeNull();
        _sync.Relationships.Synced.Should().Equal("REL-1");
    }

    [Theory]
    [InlineData("", "ACC-2")]
    [InlineData("ACC-1", "")]
    [InlineData(" ", " ")]
    public async Task A_relationship_without_both_accounts_is_dead(string account, string related)
    {
        await SyncAsync(Events.Relationship("REL-1", account, related, 1));

        _sync.Relationships.Dead.Should().ContainSingle().Which.Error.Should().StartWith("INVALID_EVENT").And.Contain("accountExternalId");
    }

    [Fact]
    public async Task A_change_updates_only_what_differs_and_a_delete_removes_it()
    {
        await AccountsAreInSalesforceAsync("ACC-1", "ACC-2");
        await SyncAsync(Events.Relationship("REL-1", "ACC-1", "ACC-2", 1, "inserted", r => r["type"] = "Parent"));

        await SyncAsync(Events.Relationship("REL-1", "ACC-1", "ACC-2", 2, "updated", r => r["type"] = "Child"));
        var update = _sync.Org.WriteRequests[^1]["body"]!["records"]!.AsArray().Single()!.AsObject();
        update.Select(p => p.Key).Should().BeEquivalentTo(["attributes", "ExternalId__c", "Type__c"]);

        await SyncAsync(Events.Delete("accountRelationship", "REL-1", 3));
        _sync.Org.FindByExternalId("Account_Relationship__c", "REL-1").Should().BeNull();
        _sync.Relationships.Synced.Should().HaveCount(3);
    }

    [Fact]
    public async Task A_batch_reads_the_relationships_and_their_accounts_once_each()
    {
        await AccountsAreInSalesforceAsync("ACC-1", "ACC-2");
        var before = _sync.Org.GetStats();

        await SyncAsync(Events.Relationship("REL-1", "ACC-1", "ACC-2", 1, "inserted"));

        var after = _sync.Org.GetStats();
        (after.Queries - before.Queries - (after.RecordTypeQueries - before.RecordTypeQueries)).Should().Be(2, "the relationships, and the Accounts they name");
        (after.UpsertRequests - before.UpsertRequests).Should().Be(1);
    }
}
