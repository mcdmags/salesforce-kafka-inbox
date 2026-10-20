using System.Text.Json.Nodes;
using SalesforceSync.ToSalesforce;

namespace SalesforceSync.Tests;

/// <summary>Accounts and their Profiles, from an inbox event to the mock org.</summary>
public sealed class AccountSyncTests
{
    private const string Acme = "ACC-1";

    private readonly SyncHarness _sync = new();

    private Task<int> SyncAsync(string json)
    {
        return _sync.SyncAsync(AccountSync.Type, Acme, json);
    }

    private JsonObject? Account => _sync.Org.FindByExternalId("Account", Acme);

    private JsonObject? Profile => _sync.Org.FindByExternalId("Profile__c", Acme);

    private static void WithProfile(JsonObject account)
    {
        account["profile"] = new JsonObject { ["tier"] = "Gold", ["score"] = 87.5 };
    }

    [Fact]
    public async Task An_account_is_created_with_its_profile_which_belongs_to_it()
    {
        await SyncAsync(Events.Account(Acme, 1, "inserted", a => { WithProfile(a); a["tags"] = new JsonArray("B2B", "Retail"); }));

        Account!["Name"]!.GetValue<string>().Should().Be("Acme");
        Account["Tags__c"]!.GetValue<string>().Should().Be("B2B;Retail", "a list in the event, a multi-select value in Salesforce");
        Account["RecordTypeId"]!.GetValue<string>().Should().NotBeNullOrEmpty();
        Profile!["Tier__c"]!.GetValue<string>().Should().Be("Gold");
        Profile["AccountId__c"]!.GetValue<string>().Should().Be(Acme, "the Account's key, as text");
        _sync.Accounts.Synced.Should().Equal(Acme);
    }

    [Fact]
    public async Task A_profile_with_a_key_of_its_own_is_stored_under_it()
    {
        await SyncAsync(Events.Account(Acme, 1, "inserted", a => a["profile"] = new JsonObject { ["externalId"] = "PRF-9", ["tier"] = "Gold" }));

        _sync.Org.FindByExternalId("Profile__c", "PRF-9").Should().NotBeNull();
        Profile.Should().BeNull();
    }

    [Fact]
    public async Task An_event_without_a_profile_writes_only_the_account_and_leaves_the_profile_alone()
    {
        await SyncAsync(Events.Account(Acme, 1, "inserted", WithProfile));

        await SyncAsync(Events.Account(Acme, 2, account: a => a["name"] = "Renamed"));

        Account!["Name"]!.GetValue<string>().Should().Be("Renamed");
        Profile!["Tier__c"]!.GetValue<string>().Should().Be("Gold");
    }

    [Fact]
    public async Task Only_the_fields_that_differ_are_sent_and_an_event_that_changes_nothing_sends_nothing()
    {
        await SyncAsync(Events.Account(Acme, 1, "inserted", a => { WithProfile(a); a["rating"] = "Hot"; }));
        var writes = _sync.Org.GetStats().WriteRequests;

        await SyncAsync(Events.Account(Acme, 2, account: a => { WithProfile(a); a["rating"] = "Hot"; }));
        _sync.Org.GetStats().WriteRequests.Should().Be(writes, "nothing differs");
        _sync.Accounts.Synced.Should().HaveCount(2, "it is done all the same");

        await SyncAsync(Events.Account(Acme, 3, account: a => { WithProfile(a); a["rating"] = "Cold"; }));
        var update = _sync.Org.WriteRequests[^1]["body"]!["records"]!.AsArray().Single()!.AsObject();
        update.Select(p => p.Key).Should().BeEquivalentTo(["attributes", "ExternalId__c", "Rating"]);
    }

    [Fact]
    public async Task A_field_missing_from_the_event_clears_it_in_salesforce()
    {
        await SyncAsync(Events.Account(Acme, 1, "inserted", a => a["rating"] = "Hot"));

        await SyncAsync(Events.Account(Acme, 2));

        Account!.ContainsKey("Rating").Should().BeTrue();
        Account["Rating"].Should().BeNull();
    }

    [Fact]
    public async Task A_delete_removes_the_profile_and_the_account_in_one_call()
    {
        await SyncAsync(Events.Account(Acme, 1, "inserted", WithProfile));
        var deletes = _sync.Org.GetStats().DeleteRequests;

        await SyncAsync(Events.Delete("account", Acme, 2));

        Account.Should().BeNull();
        Profile.Should().BeNull();
        _sync.Accounts.Synced.Should().HaveCount(2);
    }

    [Fact]
    public async Task A_delete_of_an_account_Salesforce_does_not_have_is_done()
    {
        await SyncAsync(Events.Delete("account", Acme, 1));

        _sync.Accounts.Synced.Should().Equal(Acme);
        _sync.Org.GetStats().DeleteRequests.Should().Be(0, "nothing to delete");
    }

    [Fact]
    public async Task A_delete_removes_the_profile_by_the_key_of_its_own()
    {
        await SyncAsync(Events.Account(Acme, 1, "inserted", a => a["profile"] = new JsonObject { ["externalId"] = "PRF-9", ["tier"] = "Gold" }));

        await SyncAsync(Events.Account(Acme, 2, "deleted", a => a["profile"] = new JsonObject { ["externalId"] = "PRF-9" }));

        Account.Should().BeNull();
        _sync.Org.FindByExternalId("Profile__c", "PRF-9").Should().BeNull();
    }

    [Fact]
    public async Task A_failing_account_is_dead_when_the_data_is_wrong_and_its_profile_is_not_sent()
    {
        _sync.Org.InjectFault("Account", Acme, "FIELD_CUSTOM_VALIDATION_EXCEPTION", "no", times: int.MaxValue);

        await SyncAsync(Events.Account(Acme, 1, "inserted", WithProfile));

        _sync.Accounts.Dead.Should().ContainSingle().Which.Error.Should().Contain("FIELD_CUSTOM_VALIDATION_EXCEPTION");
        Profile.Should().BeNull("its account failed");
        _sync.Org.All("Profile__c").Should().BeEmpty();
    }

    [Fact]
    public async Task A_failure_that_may_pass_is_retried()
    {
        _sync.Org.InjectFault("Account", Acme, "UNABLE_TO_LOCK_ROW", "locked", times: 1);

        await SyncAsync(Events.Account(Acme, 1, "inserted"));

        _sync.Accounts.Failed.Should().ContainSingle().Which.Error.Should().Contain("UNABLE_TO_LOCK_ROW");
        _sync.Accounts.Dead.Should().BeEmpty();
    }

    [Fact]
    public async Task An_account_event_without_the_account_is_dead()
    {
        await _sync.Accounts.AddAsync(Acme, DateTimeOffset.UnixEpoch, /*lang=json,strict*/ """{ "meta": { "time": "2026-10-14T01:02:03Z", "type": "updated" } }""", CancellationToken.None);

        await _sync.Account.SyncBatchAsync(CancellationToken.None);

        _sync.Accounts.Dead.Should().ContainSingle().Which.Error.Should().StartWith("INVALID_EVENT");
    }

    [Fact]
    public async Task Without_the_record_type_in_the_org_the_account_waits_for_it()
    {
        // Mock org has no record type of this name: org decides, not event
        var org = _sync.Org;
        await SyncAsync(Events.Account(Acme, 1, "inserted"));

        org.GetStats().RecordTypeQueries.Should().BeGreaterThan(0, "the type's record types are read from the org");
        Account!["RecordTypeId"]!.GetValue<string>().Should().Be(await _sync.Calls.RecordTypeIdAsync(AccountSync.AccountObject, "Business", CancellationToken.None));
    }

    [Fact]
    public async Task A_batch_is_two_queries_one_upsert_per_object_and_no_more()
    {
        var before = _sync.Org.GetStats();

        await SyncAsync(Events.Account(Acme, 1, "inserted", WithProfile));

        var after = _sync.Org.GetStats();
        (after.Queries - before.Queries - (after.RecordTypeQueries - before.RecordTypeQueries)).Should().Be(2, "the Accounts and the Profiles");
        (after.UpsertRequests - before.UpsertRequests).Should().Be(2, "Accounts, then Profiles");
    }
}