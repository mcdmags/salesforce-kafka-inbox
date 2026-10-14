using System.Net.Http.Json;
using System.Text.Json.Nodes;

namespace SalesforceSync.IntegrationTests;

/// <summary>
/// Kafka -> KafkaInboxConsumer -> sf_account_inbox (Postgres) -> InboxWorker -> mock Salesforce, with Postgres and Kafka in
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
            new JsonObject { ["name"] = name, ["rating"] = "Hot" },
            new JsonObject { ["tier"] = "Gold", ["score"] = score });

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
        ((string?)profile["Account__c"]).Should().Be((string?)account["Id"], "the reference to the new Account is resolved to its Id");
    }

    [Fact]
    public async Task Full_record_with_one_changed_field_sends_only_that_field()
    {
        var acct = SyncFixture.NewAccountId();
        var (name, description) = (Any.String("Globex "), Any.String("original "));

        await app.PublishAsync(acct, 1,
            new JsonObject { ["name"] = name, ["rating"] = "Hot", ["description"] = description },
            new JsonObject { ["tier"] = "Silver" });
        await app.WaitForInboxSettledAsync(acct, expectedRows: 1);

        // The next event is the full record again; only the rating is different
        await app.PublishAsync(acct, 2,
            new JsonObject { ["name"] = name, ["rating"] = "Warm", ["description"] = description },
            new JsonObject { ["tier"] = "Silver" });
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
    public async Task Field_that_becomes_null_or_absent_is_cleared()
    {
        var acct = SyncFixture.NewAccountId();
        var name = Any.String("Soylent ");

        await app.PublishAsync(acct, 1,
            new JsonObject { ["name"] = name, ["rating"] = "Hot", ["description"] = "to be cleared", ["isActive"] = true },
            new JsonObject { ["tier"] = "Gold", ["score"] = 7 });
        await app.WaitForInboxSettledAsync(acct, expectedRows: 1);

        // rating is null, description and isActive are absent; the profile no longer has a tier
        await app.PublishAsync(acct, 2,
            new JsonObject { ["name"] = name, ["rating"] = null },
            new JsonObject { ["score"] = 7 });
        var inbox = await app.WaitForInboxSettledAsync(acct, expectedRows: 2);
        inbox.Should().OnlyContain(r => r.Status == "done");

        var account = await app.GetRecordAsync("Account", acct);
        account.Should().NotBeNull();
        ((string?)account!["Name"]).Should().Be(name);
        account["Rating"].Should().BeNull("an explicit null clears the field");
        account["Description"].Should().BeNull("a field the full record no longer has is cleared");
        ((bool?)account["Active__c"]).Should().BeFalse("an empty checkbox is false, never null");

        var accountWrites = await app.GetHistoryAsync("Account", acct);
        accountWrites.Should().HaveCount(2);
        accountWrites[1].Select(kv => kv.Key).Should().BeEquivalentTo(["Rating", "Description", "Active__c"],
            "only the fields that had a value are cleared");

        var profile = await app.GetRecordAsync("Profile__c", SyncFixture.ProfileIdFor(acct));
        profile.Should().NotBeNull();
        profile!["Tier__c"].Should().BeNull();
        ((decimal?)profile["Score__c"]).Should().Be(7m);
    }

    [Fact]
    public async Task Event_without_a_profile_leaves_the_Profile_alone()
    {
        var acct = SyncFixture.NewAccountId();
        var name = Any.String("Tyrell ");

        await app.PublishAsync(acct, 1, new JsonObject { ["name"] = name, ["rating"] = "Hot" }, new JsonObject { ["tier"] = "Gold" });
        await app.WaitForInboxSettledAsync(acct, expectedRows: 1);

        var withoutProfile = SyncFixture.AccountEvent(acct, 2, new JsonObject { ["name"] = name, ["rating"] = "Cold" }, []);
        withoutProfile["account"]!.AsObject().Remove("profile");
        await app.PublishRawAsync(acct, withoutProfile.ToJsonString());
        await app.WaitForInboxSettledAsync(acct, expectedRows: 2);

        ((string?)(await app.GetRecordAsync("Account", acct))!["Rating"]).Should().Be("Cold");
        var profile = await app.GetRecordAsync("Profile__c", SyncFixture.ProfileIdFor(acct));
        ((string?)profile!["Tier__c"]).Should().Be("Gold", "no profile in the event means the Profile isn't touched, not that it is emptied");
        (await app.GetHistoryAsync("Profile__c", SyncFixture.ProfileIdFor(acct))).Should().ContainSingle();
    }

    [Fact]
    public async Task Diff_is_against_the_record_in_Salesforce_not_the_previous_event()
    {
        var acct = SyncFixture.NewAccountId();
        var account = new JsonObject { ["name"] = Any.String("Wayne "), ["rating"] = "Hot", ["description"] = "synced" };

        await app.PublishAsync(acct, 1, account, new JsonObject { ["tier"] = "Gold" });
        await app.WaitForInboxSettledAsync(acct, expectedRows: 1);

        // Someone edits the record directly in Salesforce
        using (var edit = await app.Salesforce.PatchAsJsonAsync($"/__fake/records/Account/{acct}",
                   new JsonObject { ["Rating"] = "Cold", ["Description"] = "edited in Salesforce" }))
        {
            edit.EnsureSuccessStatusCode();
        }

        // The next event carries the same account data as v1; Salesforce differs in two fields, so both are written back
        await app.PublishAsync(acct, 2, (JsonObject)account.DeepClone(), new JsonObject { ["tier"] = "Gold" });
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
    public async Task Burst_of_updates_lands_the_newest_full_record()
    {
        var acct = SyncFixture.NewAccountId();
        var name = Any.String("Initech ");

        await app.PublishAsync(acct, 1, new JsonObject { ["name"] = "first name", ["rating"] = "Hot" }, new JsonObject { ["tier"] = "Bronze" });
        await app.PublishAsync(acct, 2, new JsonObject { ["name"] = name, ["rating"] = "Warm", ["industry"] = "Energy" }, new JsonObject { ["tier"] = "Silver", ["score"] = 10 });
        await app.PublishAsync(acct, 3, new JsonObject { ["name"] = name, ["rating"] = "Warm", ["description"] = "from v3" }, new JsonObject { ["tier"] = "Gold", ["notes"] = "from v3" });
        await app.PublishAsync(acct, 4, new JsonObject { ["name"] = name, ["rating"] = "Cold", ["description"] = "from v4" }, new JsonObject { ["tier"] = "Gold" });
        await app.PublishAsync(acct, 5, new JsonObject { ["name"] = name, ["rating"] = "Cold", ["description"] = "from v5" }, new JsonObject { ["tier"] = "Gold", ["score"] = 99 });

        var inbox = await app.WaitForInboxSettledAsync(acct, expectedRows: 5);
        inbox.Should().OnlyContain(r => r.Status == "done" || r.Status == "superseded",
            "an event claimed together with a newer one of its account is superseded, the rest are written");
        inbox[^1].Should().Be((5L, "done"));

        // Exactly the v5 record, however the five events were batched: nothing of an older event survives
        var account = await app.GetRecordAsync("Account", acct);
        account.Should().NotBeNull();
        ((string?)account!["Name"]).Should().Be(name);
        ((string?)account["Rating"]).Should().Be("Cold");
        ((string?)account["Description"]).Should().Be("from v5");
        account["Industry"].Should().BeNull("v5 has no industry");

        var profile = await app.GetRecordAsync("Profile__c", SyncFixture.ProfileIdFor(acct));
        profile.Should().NotBeNull();
        ((string?)profile!["Tier__c"]).Should().Be("Gold");
        ((decimal?)profile["Score__c"]).Should().Be(99m);
        profile["Notes__c"].Should().BeNull("v5 has no notes");
    }

    [Fact]
    public async Task Stale_event_is_superseded_and_does_not_overwrite_newer_data()
    {
        var acct = SyncFixture.NewAccountId();

        var name = Any.String("Umbrella ");
        await app.PublishAsync(acct, 5, new JsonObject { ["name"] = name, ["rating"] = "Hot" }, new JsonObject { ["tier"] = "Platinum" });
        await app.WaitForInboxSettledAsync(acct, expectedRows: 1);

        // An older version arrives late (replay / out-of-order producer)
        await app.PublishAsync(acct, 3, new JsonObject { ["name"] = name, ["rating"] = "Cold" }, new JsonObject { ["tier"] = "Bronze" });
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
    public async Task Redelivered_event_is_stored_again_and_changes_nothing()
    {
        var acct = SyncFixture.NewAccountId();
        var v1 = SyncFixture.AccountEvent(acct, 1, new JsonObject { ["name"] = Any.String("Acme "), ["rating"] = "Hot" }, []);

        await app.PublishRawAsync(acct, v1.ToJsonString());
        await app.WaitForInboxSettledAsync(acct, expectedRows: 1);
        await app.PublishRawAsync(acct, v1.ToJsonString());            // Kafka redelivery, after the first was synced
        await app.PublishRawAsync(acct, v1.ToJsonString());            // and again

        // Nothing makes a row unique, so each delivery is a row; none is newer than what was synced
        var inbox = await app.WaitForInboxSettledAsync(acct, expectedRows: 3);
        inbox.Should().Equal((1L, "done"), (1L, "superseded"), (1L, "superseded"));
        (await app.GetHistoryAsync("Account", acct)).Should().ContainSingle("a redelivery must not write to Salesforce");
    }

    [Fact]
    public async Task Redelivery_claimed_together_with_the_original_is_written_once()
    {
        var acct = SyncFixture.NewAccountId();
        var v1 = SyncFixture.AccountEvent(acct, 1, new JsonObject { ["name"] = Any.String("Acme ") }, []);

        await app.InsertIntoInboxAsync(acct, v1, v1);

        var inbox = await app.WaitForInboxSettledAsync(acct, expectedRows: 2);
        inbox.Select(r => r.Status).Should().BeEquivalentTo(["done", "superseded"]);
        (await app.GetHistoryAsync("Account", acct)).Should().ContainSingle();
    }

    [Fact]
    public async Task Event_without_a_time_offset_or_a_valid_type_or_for_another_entity_type_is_skipped_at_ingest()
    {
        var acct = SyncFixture.NewAccountId();
        var name = Any.String("Acme ");
        var valid = SyncFixture.AccountEvent(acct, 2, new JsonObject { ["name"] = name }, []);
        var noOffset = SyncFixture.AccountEvent(acct, 1, new JsonObject { ["name"] = "never stored" }, []);
        noOffset["meta"]!["time"] = ((string)noOffset["meta"]!["time"]!).TrimEnd('Z');
        var relationship = new JsonObject
        {
            ["meta"] = SyncFixture.Meta(1),
            ["accountRelationship"] = new JsonObject { ["externalId"] = acct, ["accountExternalId"] = "A", ["relatedAccountExternalId"] = "B" }
        };

        // All under one key, so one partition: by the time the valid event is stored, the others have been handled
        await app.PublishRawAsync(acct, noOffset.ToJsonString());      // no explicit offset
        await app.PublishRawAsync(acct, relationship.ToJsonString());  // a relationship event on the account topic
        await app.PublishRawAsync(acct, "not json");
        await app.PublishRawAsync(acct, WithType(valid, null).ToJsonString());       // no meta.type
        await app.PublishRawAsync(acct, WithType(valid, "deleted").ToJsonString());  // deletes are not synced
        await app.PublishRawAsync(acct, valid.ToJsonString());

        (await app.WaitForInboxSettledAsync(acct, expectedRows: 1)).Should().Equal((2L, "done"));
        (await app.GetInboxAsync(acct, SyncFixture.Relationships)).Should().BeEmpty("a topic's events are read as that topic's type only");
        ((string?)(await app.GetRecordAsync("Account", acct))!["Name"]).Should().Be(name);
    }

    [Fact]
    public async Task The_Kafka_key_is_not_used_and_arrival_order_does_not_matter()
    {
        var acct = SyncFixture.NewAccountId();
        JsonObject Version(int v)
        {
            return SyncFixture.AccountEvent(acct, v, new JsonObject { ["name"] = $"v{v}", ["description"] = $"v{v}" }, new JsonObject { ["score"] = v });
        }

        // One account's events under different keys and none (so on different partitions), newest neither first nor last
        await app.PublishRawAsync(Any.String("other-key-"), Version(2).ToJsonString());
        await app.PublishRawAsync(null, Version(5).ToJsonString());
        await app.PublishRawAsync(Any.String("other-key-"), Version(1).ToJsonString());
        await app.PublishRawAsync(acct, Version(4).ToJsonString());
        await app.PublishRawAsync(null, Version(3).ToJsonString());

        var inbox = await app.WaitForInboxSettledAsync(acct, expectedRows: 5);
        inbox.Should().OnlyContain(r => r.Status == "done" || r.Status == "superseded");
        inbox[^1].Should().Be((5L, "done"), "the newest meta.time is the one that must be written");

        ((string?)(await app.GetRecordAsync("Account", acct))!["Description"]).Should().Be("v5");
        var written = (await app.GetHistoryAsync("Account", acct)).Select(w => long.Parse(((string)w["Description"]!)[1..])).ToList();
        written.Should().BeInAscendingOrder().And.OnlyHaveUniqueItems("an older event must never be written after a newer one");
        ((decimal?)(await app.GetRecordAsync("Profile__c", SyncFixture.ProfileIdFor(acct)))!["Score__c"]).Should().Be(5m);
    }

    [Fact]
    public async Task Older_event_claimed_together_with_a_newer_one_is_superseded_in_the_same_claim()
    {
        var acct = SyncFixture.NewAccountId();
        var newer = SyncFixture.AccountEvent(acct, 9, new JsonObject { ["name"] = "newer" }, []);
        var older = SyncFixture.AccountEvent(acct, 4, new JsonObject { ["name"] = "older" }, []);

        // The newer event is stored first, the older one after it; one claim takes both
        await app.InsertIntoInboxAsync(acct, newer, older);

        (await app.WaitForInboxSettledAsync(acct, expectedRows: 2)).Should().Equal((4L, "superseded"), (9L, "done"));
        (await app.GetHistoryAsync("Account", acct)).Should().ContainSingle("only the newest event of a claim is written")
            .Which["Name"]!.GetValue<string>().Should().Be("newer");
    }

    [Fact]
    public async Task A_then_B_then_A_ends_at_A()
    {
        var acct = SyncFixture.NewAccountId();
        var a = new JsonObject { ["name"] = "A", ["rating"] = "Hot" };
        var b = new JsonObject { ["name"] = "B", ["rating"] = "Cold" };

        await app.PublishAsync(acct, 1, a, []);
        await app.PublishAsync(acct, 2, b, []);
        await app.PublishAsync(acct, 3, (JsonObject)a.DeepClone(), []); // the same content as the first event

        var inbox = await app.WaitForInboxSettledAsync(acct, expectedRows: 3);
        inbox.Should().OnlyContain(r => r.Status == "done" || r.Status == "superseded", "the third event must not be dropped as a duplicate of the first");
        inbox[^1].Should().Be((3L, "done"));
        var account = await app.GetRecordAsync("Account", acct);
        ((string?)account!["Name"], (string?)account["Rating"]).Should().Be(("A", "Hot"));
    }

    [Fact]
    public async Task Unchanged_event_completes_without_writing_to_Salesforce()
    {
        var acct = SyncFixture.NewAccountId();
        var account = new JsonObject { ["name"] = Any.String("Hooli "), ["rating"] = "Warm" };
        var profile = new JsonObject { ["tier"] = "Gold", ["score"] = Any.Create<int>() };

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
    public async Task A_batch_costs_one_composite_read_and_one_composite_write()
    {
        // Warm-up: the first batch after start also describes Account and Profile__c (then cached for 15 minutes)
        var warm = SyncFixture.NewAccountId();
        await app.PublishAsync(warm, 1, new JsonObject { ["name"] = Any.String("Warm ") }, []);
        await app.WaitForInboxSettledAsync(warm, expectedRows: 1);

        var acct = SyncFixture.NewAccountId();
        var before = await app.GetStatsAsync();

        await app.PublishAsync(acct, 1, new JsonObject { ["name"] = Any.String("Stark ") }, new JsonObject { ["tier"] = "Gold" });
        await app.WaitForInboxSettledAsync(acct, expectedRows: 1);

        var after = await app.GetStatsAsync();
        long Delta(string counter)
        {
            return after.GetProperty(counter).GetInt64() - before.GetProperty(counter).GetInt64();
        }

        Delta("apiCalls").Should().Be(2, "a batch is one composite read plus one composite write");
        Delta("compositeRequests").Should().Be(2);
        Delta("queries").Should().Be(2, "the Account and Profile__c queries go in the one read");
        Delta("writeRequests").Should().Be(1, "the Account and the Profile are created in the one write");
        Delta("recordsSucceeded").Should().Be(2);
        Delta("recordsFailed").Should().Be(0);
        Delta("describes").Should().Be(0, "Describe results are cached");

        var transport = await app.Salesforce.GetFromJsonAsync<JsonObject>("/__fake/transport");
        ((long)transport!["acceptGzip"]!).Should().BePositive("the worker accepts compressed responses");
    }

    /// <summary>A copy of the event with another meta.type (null = none).</summary>
    private static JsonObject WithType(JsonObject evt, string? type)
    {
        var copy = (JsonObject)evt.DeepClone();
        if (type is null)
        {
            copy["meta"]!.AsObject().Remove("type");
        }
        else
        {
            copy["meta"]!["type"] = type;
        }

        return copy;
    }
}