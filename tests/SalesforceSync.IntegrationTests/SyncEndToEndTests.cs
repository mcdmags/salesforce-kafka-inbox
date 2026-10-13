using System.Net.Http.Json;
using System.Text.Json.Nodes;

namespace SalesforceSync.IntegrationTests;

/// <summary>
/// Kafka -> KafkaInboxConsumer -> sf_inbox (Postgres) -> InboxWorker -> mock Salesforce, with Postgres and Kafka in
/// containers and the worker and mock org in-process. Each test uses its own account ids (AutoFixture) so tests don't
/// interfere through shared state.
/// </summary>
[Collection(SyncCollection.Name)]
public sealed class SyncEndToEndTests(SyncFixture app)
{
    [Fact]
    public async Task New_account_creates_Account_and_linked_Profile()
    {
        var acct = SyncFixture.NewAccountId();
        var name = Any.String("Acme ");
        var score = Any.Create<int>();

        await app.PublishAsync(acct, 1,
            new JsonObject { ["Name"] = name, ["Rating"] = "Hot" },
            new JsonObject { ["Tier__c"] = "Gold", ["Score__c"] = score });

        var inbox = await app.WaitForInboxSettledAsync(acct, expectedRows: 1);
        inbox.Should().Equal((1L, "done"));

        var account = await app.GetRecordAsync("Account", acct);
        account.Should().NotBeNull();
        ((string?)account!["Name"]).Should().Be(name);
        ((string?)account["Rating"]).Should().Be("Hot");
        account.Should().NotContainKey("Sync_Version__c", "nothing beyond the event's own fields is stored");

        var profile = await app.GetRecordAsync("Profile__c", SyncFixture.ProfileIdFor(acct));
        profile.Should().NotBeNull();
        ((string?)profile!["Tier__c"]).Should().Be("Gold");
        ((decimal?)profile["Score__c"]).Should().Be(score);
        ((string?)profile["Account__c"]).Should().Be((string?)account["Id"], "the graph's reference to the new Account is resolved to its Id");
    }

    [Fact]
    public async Task Partial_update_patches_changed_fields_and_keeps_the_rest()
    {
        var acct = SyncFixture.NewAccountId();
        var (name, description) = (Any.String("Globex "), Any.String("original "));

        await app.PublishAsync(acct, 1,
            new JsonObject { ["Name"] = name, ["Rating"] = "Hot", ["Description"] = description },
            new JsonObject { ["Tier__c"] = "Silver" });
        await app.WaitForInboxSettledAsync(acct, expectedRows: 1);

        await app.PublishAsync(acct, 2,
            new JsonObject { ["Rating"] = "Warm" },
            new JsonObject());
        var inbox = await app.WaitForInboxSettledAsync(acct, expectedRows: 2);
        inbox.Should().OnlyContain(r => r.Status == "done");

        var account = await app.GetRecordAsync("Account", acct);
        account.Should().NotBeNull();
        ((string?)account!["Rating"]).Should().Be("Warm");
        ((string?)account["Description"]).Should().Be(description);
        ((string?)account["Name"]).Should().Be(name);

        var accountWrites = await app.GetHistoryAsync("Account", acct);
        accountWrites.Should().HaveCount(2);
        accountWrites[1].Select(kv => kv.Key).Should().Equal(["Rating"], "the second write carries only the field that differed from Salesforce");

        var profile = await app.GetRecordAsync("Profile__c", SyncFixture.ProfileIdFor(acct));
        profile.Should().NotBeNull();
        ((string?)profile!["Tier__c"]).Should().Be("Silver");
        (await app.GetHistoryAsync("Profile__c", SyncFixture.ProfileIdFor(acct)))
            .Should().ContainSingle("nothing changed on the Profile, so it was not written again");
    }

    [Fact]
    public async Task Diff_is_against_the_record_in_Salesforce_not_the_previous_event()
    {
        var acct = SyncFixture.NewAccountId();
        var account = new JsonObject { ["Name"] = Any.String("Wayne "), ["Rating"] = "Hot", ["Description"] = "synced" };

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
        stored.Should().NotBeNull();
        ((string?)stored!["Rating"]).Should().Be("Hot");
        ((string?)stored["Description"]).Should().Be("synced");

        var writes = await app.GetHistoryAsync("Account", acct);
        writes.Should().HaveCount(3, "the create, the manual edit and the sync's correction were written");
        writes[2].Select(kv => kv.Key).Should().BeEquivalentTo(["Description", "Rating"]);
        (await app.GetHistoryAsync("Profile__c", SyncFixture.ProfileIdFor(acct))).Should().ContainSingle();
    }

    [Fact]
    public async Task Burst_of_updates_lands_the_newest_merged_state()
    {
        var acct = SyncFixture.NewAccountId();
        var name = Any.String("Initech ");

        await app.PublishAsync(acct, 1, new JsonObject { ["Name"] = name, ["Rating"] = "Hot" }, new JsonObject { ["Tier__c"] = "Bronze" });
        await app.PublishAsync(acct, 2, new JsonObject { ["Rating"] = "Warm" }, new JsonObject { ["Score__c"] = 10 });
        await app.PublishAsync(acct, 3, new JsonObject { ["Description"] = "from v3" }, new JsonObject { ["Tier__c"] = "Gold" });
        await app.PublishAsync(acct, 4, new JsonObject { ["Rating"] = "Cold" }, new JsonObject());
        await app.PublishAsync(acct, 5, new JsonObject(), new JsonObject { ["Score__c"] = 99 });

        var inbox = await app.WaitForInboxSettledAsync(acct, expectedRows: 5);
        inbox.Should().OnlyContain(r => r.Status == "done");

        var account = await app.GetRecordAsync("Account", acct);
        account.Should().NotBeNull();
        ((string?)account!["Name"]).Should().Be(name);
        ((string?)account["Rating"]).Should().Be("Cold");
        ((string?)account["Description"]).Should().Be("from v3");

        var profile = await app.GetRecordAsync("Profile__c", SyncFixture.ProfileIdFor(acct));
        profile.Should().NotBeNull();
        ((string?)profile!["Tier__c"]).Should().Be("Gold");
        ((decimal?)profile["Score__c"]).Should().Be(99m);
    }

    [Fact]
    public async Task Stale_event_is_superseded_and_does_not_overwrite_newer_data()
    {
        var acct = SyncFixture.NewAccountId();

        await app.PublishAsync(acct, 5, new JsonObject { ["Name"] = Any.String("Umbrella "), ["Rating"] = "Hot" }, new JsonObject { ["Tier__c"] = "Platinum" });
        await app.WaitForInboxSettledAsync(acct, expectedRows: 1);

        // An older version arrives late (replay / out-of-order producer)
        await app.PublishAsync(acct, 3, new JsonObject { ["Rating"] = "Cold" }, new JsonObject { ["Tier__c"] = "Bronze" });
        var inbox = await app.WaitForInboxSettledAsync(acct, expectedRows: 2);
        inbox.Should().Equal((3L, "superseded"), (5L, "done"));

        // sf_sync_state already holds v5 for the account, so v3 never reached Salesforce
        var account = await app.GetRecordAsync("Account", acct);
        account.Should().NotBeNull();
        ((string?)account!["Rating"]).Should().Be("Hot");
        (await app.GetHistoryAsync("Account", acct)).Should().ContainSingle("the stale event must not be written");

        var profile = await app.GetRecordAsync("Profile__c", SyncFixture.ProfileIdFor(acct));
        profile.Should().NotBeNull();
        ((string?)profile!["Tier__c"]).Should().Be("Platinum");
        (await app.GetHistoryAsync("Profile__c", SyncFixture.ProfileIdFor(acct))).Should().ContainSingle("the stale event must not be written");
    }

    [Fact]
    public async Task Redelivered_event_is_stored_once_and_an_event_without_a_time_offset_is_skipped()
    {
        var acct = SyncFixture.NewAccountId();
        var v1 = SyncFixture.AccountEvent(acct, 1, new JsonObject { ["Name"] = Any.String("Acme "), ["Rating"] = "Hot" }, new JsonObject());
        var sameTime = SyncFixture.AccountEvent(acct, 1, new JsonObject { ["Rating"] = "Warm" }, new JsonObject());
        var noOffset = (JsonObject)v1.DeepClone();
        noOffset["meta"]!["time"] = ((string)v1["meta"]!["time"]!).TrimEnd('Z');
        var description = Any.String("v2 ");

        await app.PublishRawAsync(acct, v1.ToJsonString());
        await app.PublishRawAsync(acct, v1.ToJsonString());            // Kafka redelivery
        await app.PublishRawAsync(acct, sameTime.ToJsonString());      // another event with the same time: kept
        await app.PublishRawAsync(acct, noOffset.ToJsonString());      // logged and skipped
        await app.PublishAsync(acct, 2, new JsonObject { ["Description"] = description }, new JsonObject());

        // Same partition, so by the time v2 is in the inbox every earlier message has been handled
        var inbox = await app.WaitForInboxSettledAsync(acct, expectedRows: 3);
        inbox.Select(r => r.Version).Should().Equal(1L, 1L, 2L);
        inbox.Take(2).Should().OnlyContain(r => r.Status == "done" || r.Status == "superseded",
            "the same-time event is done when merged with the first, superseded if the first had already synced");
        inbox[2].Status.Should().Be("done");

        var account = await app.GetRecordAsync("Account", acct);
        account.Should().NotBeNull();
        ((string?)account!["Description"]).Should().Be(description);
    }

    [Fact]
    public async Task Unchanged_event_completes_without_writing_to_Salesforce()
    {
        var acct = SyncFixture.NewAccountId();
        var account = new JsonObject { ["Name"] = Any.String("Hooli "), ["Rating"] = "Warm" };
        var profile = new JsonObject { ["Tier__c"] = "Gold", ["Score__c"] = Any.Create<int>() };

        await app.PublishAsync(acct, 1, account, profile);
        await app.WaitForInboxSettledAsync(acct, expectedRows: 1);

        // Same data, newer version: every field matches Salesforce, so nothing is written
        await app.PublishAsync(acct, 2, (JsonObject)account.DeepClone(), (JsonObject)profile.DeepClone());
        var inbox = await app.WaitForInboxSettledAsync(acct, expectedRows: 2);
        inbox.Should().OnlyContain(r => r.Status == "done");

        (await app.GetHistoryAsync("Account", acct)).Should().ContainSingle("an unchanged event must not write the Account");
        (await app.GetHistoryAsync("Profile__c", SyncFixture.ProfileIdFor(acct))).Should().ContainSingle("an unchanged event must not write the Profile");
    }

    [Fact]
    public async Task A_batch_costs_one_composite_read_and_one_graph_write()
    {
        // Warm-up: the first batch after start also describes Account and Profile__c (then cached for 15 minutes)
        var warm = SyncFixture.NewAccountId();
        await app.PublishAsync(warm, 1, new JsonObject { ["Name"] = Any.String("Warm ") }, new JsonObject());
        await app.WaitForInboxSettledAsync(warm, expectedRows: 1);

        var acct = SyncFixture.NewAccountId();
        var before = await app.GetStatsAsync();

        await app.PublishAsync(acct, 1, new JsonObject { ["Name"] = Any.String("Stark ") }, new JsonObject { ["Tier__c"] = "Gold" });
        await app.WaitForInboxSettledAsync(acct, expectedRows: 1);

        var after = await app.GetStatsAsync();
        long Delta(string counter) => after.GetProperty(counter).GetInt64() - before.GetProperty(counter).GetInt64();
        Delta("apiCalls").Should().Be(2, "a batch is one composite read plus one graph write (it was 3 with separate queries)");
        Delta("compositeRequests").Should().Be(1, "the Account and Profile__c queries go in one composite call");
        Delta("graphRequests").Should().Be(1);
        Delta("describes").Should().Be(0, "Describe results are cached");
        after.GetProperty("graphsFailed").GetInt64().Should().Be(0);

        var transport = await app.Salesforce.GetFromJsonAsync<JsonObject>("/__fake/transport");
        ((long)transport!["acceptGzip"]!).Should().BePositive("the worker accepts compressed responses");
    }
}
