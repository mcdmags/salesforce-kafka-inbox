using System.Net.Http.Json;
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
        Assert.False(account.ContainsKey("Sync_Version__c")); // nothing beyond the event's own fields is stored

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

        // The second write carried only the field that differed from Salesforce
        var accountWrites = await app.GetHistoryAsync("Account", acct);
        Assert.Equal(2, accountWrites.Count);
        Assert.Equal(["Rating"], accountWrites[1].Select(kv => kv.Key));

        // Nothing changed on the Profile, so it was not written again
        var profile = await app.GetRecordAsync("Profile__c", AppHostFixture.ProfileIdFor(acct));
        Assert.NotNull(profile);
        Assert.Equal("Silver", (string?)profile["Tier__c"]);
        Assert.Single(await app.GetHistoryAsync("Profile__c", AppHostFixture.ProfileIdFor(acct)));
    }

    [Fact]
    public async Task Diff_is_against_the_record_in_Salesforce_not_the_previous_event()
    {
        var acct = AppHostFixture.NewAccountId();
        var account = new JsonObject { ["Name"] = "Wayne", ["Rating"] = "Hot", ["Description"] = "synced" };

        await app.PublishAsync(acct, 1, account, new JsonObject { ["Tier__c"] = "Gold" });
        await app.WaitForInboxSettledAsync(acct, expectedRows: 1);

        // Someone edits the record directly in Salesforce
        using (var edit = await app.Salesforce.PatchAsJsonAsync($"/__fake/records/Account/{acct}",
                   new JsonObject { ["Rating"] = "Cold", ["Description"] = "edited in Salesforce" }))
            edit.EnsureSuccessStatusCode();

        // The next event carries the same account data as v1; Salesforce differs in two fields, so both are written back
        await app.PublishAsync(acct, 2, (JsonObject)account.DeepClone(), new JsonObject { ["Tier__c"] = "Gold" });
        await app.WaitForInboxSettledAsync(acct, expectedRows: 2);

        var stored = await app.GetRecordAsync("Account", acct);
        Assert.NotNull(stored);
        Assert.Equal("Hot", (string?)stored["Rating"]);
        Assert.Equal("synced", (string?)stored["Description"]);

        var writes = await app.GetHistoryAsync("Account", acct);
        Assert.Equal(3, writes.Count); // create, the manual edit, the sync's correction
        Assert.Equal(["Description", "Rating"], writes[2].Select(kv => kv.Key).Order());
        Assert.Single(await app.GetHistoryAsync("Profile__c", AppHostFixture.ProfileIdFor(acct)));
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
        Assert.All(inbox, r => Assert.Equal("done", r.Status));

        var account = await app.GetRecordAsync("Account", acct);
        Assert.NotNull(account);
        Assert.Equal("Initech", (string?)account["Name"]);
        Assert.Equal("Cold", (string?)account["Rating"]);
        Assert.Equal("from v3", (string?)account["Description"]);

        var profile = await app.GetRecordAsync("Profile__c", AppHostFixture.ProfileIdFor(acct));
        Assert.NotNull(profile);
        Assert.Equal("Gold", (string?)profile["Tier__c"]);
        Assert.Equal(99m, (decimal?)profile["Score__c"]);
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

        // sf_sync_state already holds v5 for the account, so v3 never reached Salesforce
        var account = await app.GetRecordAsync("Account", acct);
        Assert.NotNull(account);
        Assert.Equal("Hot", (string?)account["Rating"]);
        Assert.Single(await app.GetHistoryAsync("Account", acct));

        var profile = await app.GetRecordAsync("Profile__c", AppHostFixture.ProfileIdFor(acct));
        Assert.NotNull(profile);
        Assert.Equal("Platinum", (string?)profile["Tier__c"]);
        Assert.Single(await app.GetHistoryAsync("Profile__c", AppHostFixture.ProfileIdFor(acct)));
    }

    [Fact]
    public async Task Unchanged_event_completes_without_writing_to_Salesforce()
    {
        var acct = AppHostFixture.NewAccountId();
        var account = new JsonObject { ["Name"] = "Hooli", ["Rating"] = "Warm" };
        var profile = new JsonObject { ["Tier__c"] = "Gold", ["Score__c"] = 7 };

        await app.PublishAsync(acct, 1, account, profile);
        await app.WaitForInboxSettledAsync(acct, expectedRows: 1);

        // Same data, newer version: every field matches Salesforce, so nothing is written
        await app.PublishAsync(acct, 2, (JsonObject)account.DeepClone(), (JsonObject)profile.DeepClone());
        var inbox = await app.WaitForInboxSettledAsync(acct, expectedRows: 2);
        Assert.All(inbox, r => Assert.Equal("done", r.Status));

        Assert.Single(await app.GetHistoryAsync("Account", acct));
        Assert.Single(await app.GetHistoryAsync("Profile__c", AppHostFixture.ProfileIdFor(acct)));
    }

    [Fact]
    public async Task A_batch_costs_one_composite_read_and_one_graph_write()
    {
        // Warm-up: the first batch after start also describes Account and Profile__c (then cached for 15 minutes)
        var warm = AppHostFixture.NewAccountId();
        await app.PublishAsync(warm, 1, new JsonObject { ["Name"] = "Warm" }, new JsonObject());
        await app.WaitForInboxSettledAsync(warm, expectedRows: 1);

        var acct = AppHostFixture.NewAccountId();
        var before = await app.GetStatsAsync();

        await app.PublishAsync(acct, 1, new JsonObject { ["Name"] = "Stark" }, new JsonObject { ["Tier__c"] = "Gold" });
        await app.WaitForInboxSettledAsync(acct, expectedRows: 1);

        var after = await app.GetStatsAsync();
        long Delta(string counter) => after.GetProperty(counter).GetInt64() - before.GetProperty(counter).GetInt64();
        Assert.Equal(2, Delta("apiCalls"));          // was 3 with separate queries
        Assert.Equal(1, Delta("compositeRequests")); // Account + Profile__c queries in one call
        Assert.Equal(1, Delta("graphRequests"));
        Assert.Equal(0, Delta("describes"));
        Assert.Equal(0, after.GetProperty("graphsFailed").GetInt64());

        // The worker accepts compressed responses
        var transport = await app.Salesforce.GetFromJsonAsync<JsonObject>("/__fake/transport");
        Assert.True((long)transport!["acceptGzip"]! > 0);
    }
}
