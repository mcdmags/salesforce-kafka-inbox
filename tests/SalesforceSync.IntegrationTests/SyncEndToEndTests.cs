using System.Text.Json.Nodes;

namespace SalesforceSync.IntegrationTests;

/// <summary>
/// Kafka -> KafkaInboxConsumer -> sf_inbox (Postgres) -> InboxWorker -> mock Salesforce, all running under Aspire.
/// Each test uses its own account ids so tests don't interfere through shared state.
/// </summary>
[Collection(AppHostCollection.Name)]
public sealed class SyncEndToEndTests(AppHostFixture app)
{
    [Fact]
    public async Task New_account_creates_Account_and_linked_Profile()
    {
        var acct = AppHostFixture.NewAccountId();

        await app.PublishAsync(acct, 1,
            new JsonObject { ["Name"] = "Acme", ["Rating"] = "Hot" },
            new JsonObject { ["Tier__c"] = "Gold", ["Score__c"] = 42 });

        var inbox = await app.WaitForInboxSettledAsync(acct, expectedRows: 1);
        Assert.Equal(new[] { (1L, "done") }, inbox);

        var account = await app.GetRecordAsync("Account", acct);
        Assert.NotNull(account);
        Assert.Equal("Acme", (string?)account["Name"]);
        Assert.Equal("Hot", (string?)account["Rating"]);
        Assert.Equal(1m, (decimal?)account["Sync_Version__c"]);

        var profile = await app.GetRecordAsync("Profile__c", AppHostFixture.ProfileIdFor(acct));
        Assert.NotNull(profile);
        Assert.Equal("Gold", (string?)profile["Tier__c"]);
        Assert.Equal(42m, (decimal?)profile["Score__c"]);
        Assert.Equal((string?)account["Id"], (string?)profile["Account__c"]); // @{acct.id} reference resolved
    }

    [Fact]
    public async Task Partial_update_patches_changed_fields_and_keeps_the_rest()
    {
        var acct = AppHostFixture.NewAccountId();

        await app.PublishAsync(acct, 1,
            new JsonObject { ["Name"] = "Globex", ["Rating"] = "Hot", ["Description"] = "original" },
            new JsonObject { ["Tier__c"] = "Silver" });
        await app.WaitForInboxSettledAsync(acct, expectedRows: 1);

        await app.PublishAsync(acct, 2,
            new JsonObject { ["Rating"] = "Warm" },
            new JsonObject());
        var inbox = await app.WaitForInboxSettledAsync(acct, expectedRows: 2);
        Assert.All(inbox, r => Assert.Equal("done", r.Status));

        var account = await app.GetRecordAsync("Account", acct);
        Assert.NotNull(account);
        Assert.Equal("Warm", (string?)account["Rating"]);
        Assert.Equal("original", (string?)account["Description"]);
        Assert.Equal("Globex", (string?)account["Name"]);
        Assert.Equal(2m, (decimal?)account["Sync_Version__c"]);

        // Only the Account changed, so the Profile was not written and keeps its original version
        var profile = await app.GetRecordAsync("Profile__c", AppHostFixture.ProfileIdFor(acct));
        Assert.NotNull(profile);
        Assert.Equal("Silver", (string?)profile["Tier__c"]);
        Assert.Equal(1m, (decimal?)profile["Sync_Version__c"]);
    }

    [Fact]
    public async Task Burst_of_updates_lands_the_newest_merged_state()
    {
        var acct = AppHostFixture.NewAccountId();

        await app.PublishAsync(acct, 1, new JsonObject { ["Name"] = "Initech", ["Rating"] = "Hot" }, new JsonObject { ["Tier__c"] = "Bronze" });
        await app.PublishAsync(acct, 2, new JsonObject { ["Rating"] = "Warm" }, new JsonObject { ["Score__c"] = 10 });
        await app.PublishAsync(acct, 3, new JsonObject { ["Description"] = "from v3" }, new JsonObject { ["Tier__c"] = "Gold" });
        await app.PublishAsync(acct, 4, new JsonObject { ["Rating"] = "Cold" }, new JsonObject());
        await app.PublishAsync(acct, 5, new JsonObject(), new JsonObject { ["Score__c"] = 99 });

        var inbox = await app.WaitForInboxSettledAsync(acct, expectedRows: 5);
        Assert.All(inbox, r => Assert.Contains(r.Status, new[] { "done", "superseded" }));

        var account = await app.GetRecordAsync("Account", acct);
        Assert.NotNull(account);
        Assert.Equal("Initech", (string?)account["Name"]);
        Assert.Equal("Cold", (string?)account["Rating"]);
        Assert.Equal("from v3", (string?)account["Description"]);

        var profile = await app.GetRecordAsync("Profile__c", AppHostFixture.ProfileIdFor(acct));
        Assert.NotNull(profile);
        Assert.Equal("Gold", (string?)profile["Tier__c"]);
        Assert.Equal(99m, (decimal?)profile["Score__c"]);
        Assert.Equal(5m, (decimal?)profile["Sync_Version__c"]); // v5 only touched the Profile
    }

    [Fact]
    public async Task Stale_event_is_superseded_and_does_not_overwrite_newer_data()
    {
        var acct = AppHostFixture.NewAccountId();

        await app.PublishAsync(acct, 5, new JsonObject { ["Name"] = "Umbrella", ["Rating"] = "Hot" }, new JsonObject { ["Tier__c"] = "Platinum" });
        await app.WaitForInboxSettledAsync(acct, expectedRows: 1);

        // An older version arrives late (replay / out-of-order producer)
        await app.PublishAsync(acct, 3, new JsonObject { ["Rating"] = "Cold" }, new JsonObject { ["Tier__c"] = "Bronze" });
        var inbox = await app.WaitForInboxSettledAsync(acct, expectedRows: 2);
        Assert.Equal(new[] { (3L, "superseded"), (5L, "done") }, inbox);

        var account = await app.GetRecordAsync("Account", acct);
        Assert.NotNull(account);
        Assert.Equal("Hot", (string?)account["Rating"]);
        Assert.Equal(5m, (decimal?)account["Sync_Version__c"]);

        var profile = await app.GetRecordAsync("Profile__c", AppHostFixture.ProfileIdFor(acct));
        Assert.NotNull(profile);
        Assert.Equal("Platinum", (string?)profile["Tier__c"]);
    }

    [Fact]
    public async Task Unchanged_event_completes_without_writing_to_Salesforce()
    {
        var acct = AppHostFixture.NewAccountId();
        var account = new JsonObject { ["Name"] = "Hooli", ["Rating"] = "Warm" };
        var profile = new JsonObject { ["Tier__c"] = "Gold", ["Score__c"] = 7 };

        await app.PublishAsync(acct, 1, account, profile);
        await app.WaitForInboxSettledAsync(acct, expectedRows: 1);

        // Same data, newer version: diff is empty, so no PATCH and Sync_Version__c is not bumped
        await app.PublishAsync(acct, 2, (JsonObject)account.DeepClone(), (JsonObject)profile.DeepClone());
        var inbox = await app.WaitForInboxSettledAsync(acct, expectedRows: 2);
        Assert.All(inbox, r => Assert.Equal("done", r.Status));

        var stored = await app.GetRecordAsync("Account", acct);
        Assert.NotNull(stored);
        Assert.Equal(1m, (decimal?)stored["Sync_Version__c"]);
        var storedProfile = await app.GetRecordAsync("Profile__c", AppHostFixture.ProfileIdFor(acct));
        Assert.NotNull(storedProfile);
        Assert.Equal(1m, (decimal?)storedProfile["Sync_Version__c"]);
    }

    [Fact]
    public async Task Mock_org_reports_API_usage_for_the_traffic()
    {
        var acct = AppHostFixture.NewAccountId();
        var before = await app.GetStatsAsync();

        await app.PublishAsync(acct, 1, new JsonObject { ["Name"] = "Stark" }, new JsonObject { ["Tier__c"] = "Gold" });
        await app.WaitForInboxSettledAsync(acct, expectedRows: 1);

        var after = await app.GetStatsAsync();
        Assert.True(after.GetProperty("apiCalls").GetInt64() >= before.GetProperty("apiCalls").GetInt64() + 3,
            "expected at least 2 queries + 1 composite graph call");
        Assert.True(after.GetProperty("graphsSucceeded").GetInt64() > before.GetProperty("graphsSucceeded").GetInt64());
        Assert.Equal(0, after.GetProperty("graphsFailed").GetInt64());
    }
}
