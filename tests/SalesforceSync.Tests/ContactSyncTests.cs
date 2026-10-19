using SalesforceSync.ToSalesforce;

namespace SalesforceSync.Tests;

/// <summary>Contacts of an Account, from an inbox event to the mock org.</summary>
public sealed class ContactSyncTests
{
    private readonly SyncHarness _sync = new();

    private Task<int> SyncAsync(string json)
    {
        return _sync.SyncAsync(ContactSync.Type, "CON-1", json);
    }

    private Task AccountIsInSalesforceAsync()
    {
        return _sync.SyncAsync(AccountSync.Type, "ACC-1", Events.Account("ACC-1", 1, "inserted"));
    }

    [Fact]
    public async Task A_contact_of_an_account_in_salesforce_is_created_with_its_account()
    {
        await AccountIsInSalesforceAsync();

        await SyncAsync(Events.Contact("CON-1", "ACC-1", 1, "inserted", c => c["firstName"] = "Ada"));

        var contact = _sync.Org.FindByExternalId("Contact", "CON-1")!;
        (contact["AccountId__c"]!.GetValue<string>(), contact["FirstName"]!.GetValue<string>(), contact["LastName"]!.GetValue<string>())
            .Should().Be(("ACC-1", "Ada", "Lovelace"));
        _sync.Contacts.Synced.Should().Equal("CON-1");
    }

    [Fact]
    public async Task A_contact_whose_account_is_not_in_salesforce_yet_waits_for_it_and_is_sent_once_it_is()
    {
        await SyncAsync(Events.Contact("CON-1", "ACC-1", 1, "inserted"));

        _sync.Contacts.Failed.Should().ContainSingle().Which.Error.Should().Contain("DEPENDENCY_NOT_SYNCED");
        _sync.Org.FindByExternalId("Contact", "CON-1").Should().BeNull();

        await AccountIsInSalesforceAsync();
        await SyncAsync(Events.Contact("CON-1", "ACC-1", 2, "inserted"));

        _sync.Org.FindByExternalId("Contact", "CON-1").Should().NotBeNull();
        _sync.Contacts.Synced.Should().Equal("CON-1");
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public async Task A_contact_without_its_account_is_dead(string account)
    {
        await SyncAsync(Events.Contact("CON-1", account, 1));

        _sync.Contacts.Dead.Should().ContainSingle().Which.Error.Should().StartWith("INVALID_EVENT").And.Contain("accountExternalId");
    }

    [Fact]
    public async Task A_contact_without_a_last_name_is_dead_for_the_org_requires_one()
    {
        await AccountIsInSalesforceAsync();

        await SyncAsync(Events.Contact("CON-1", "ACC-1", 1, with: c => c.Remove("lastName")));

        _sync.Contacts.Dead.Should().ContainSingle().Which.Error.Should().Contain("REQUIRED_FIELD_MISSING");
    }

    [Fact]
    public async Task A_change_updates_only_what_differs_and_a_delete_removes_the_contact()
    {
        await AccountIsInSalesforceAsync();
        await SyncAsync(Events.Contact("CON-1", "ACC-1", 1, "inserted"));

        await SyncAsync(Events.Contact("CON-1", "ACC-1", 2, with: c => c["email"] = "ada@example.com"));
        var update = _sync.Org.WriteRequests[^1]["body"]!["records"]!.AsArray().Single()!.AsObject();
        update.Select(p => p.Key).Should().BeEquivalentTo(["attributes", "ExternalId__c", "Email"]);

        await SyncAsync(Events.Delete("contact", "CON-1", 3));
        _sync.Org.FindByExternalId("Contact", "CON-1").Should().BeNull();
    }

    [Fact]
    public async Task The_contacts_of_a_batch_are_one_upsert_and_one_read_of_their_accounts()
    {
        await AccountIsInSalesforceAsync();
        foreach (var key in new[] { "CON-2", "CON-3" })
        {
            await _sync.Contacts.AddAsync(key, Events.TimeOf(Events.Contact(key, "ACC-1", 1)), Events.Contact(key, "ACC-1", 1), CancellationToken.None);
        }

        var before = _sync.Org.GetStats();
        await SyncAsync(Events.Contact("CON-1", "ACC-1", 1, "inserted"));

        var after = _sync.Org.GetStats();
        (after.UpsertRequests - before.UpsertRequests).Should().Be(1, "three contacts, one call");
        _sync.Contacts.Synced.Should().BeEquivalentTo("CON-1", "CON-2", "CON-3");
    }
}
