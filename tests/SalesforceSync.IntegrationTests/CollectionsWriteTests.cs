using System.Text.Json.Nodes;

namespace SalesforceSync.IntegrationTests;

/// <summary>
/// What is sent to Salesforce and what per-record results do to an entity: the records go as Collections upserts by
/// external id, one call per object, parents first (the example objects name their parents by Text foreign keys); each
/// record saved or rejected on its own (allOrNone false).
/// </summary>
[Collection(SyncCollection.Name)]
public sealed class CollectionsWriteTests(SyncFixture app)
{
    private const string AccountUpsert = "/services/data/v62.0/composite/sobjects/Account/ExternalId__c";
    private const string ProfileUpsert = "/services/data/v62.0/composite/sobjects/Profile__c/ExternalId__c";

    [Fact]
    public async Task New_Account_and_Profile_are_two_upserts_the_Account_first_and_the_Profile_carrying_the_Accounts_key()
    {
        var acct = SyncFixture.NewAccountId();

        await app.PublishAsync(acct, 1, new JsonObject { ["name"] = Any.String("Acme ") }, new JsonObject { ["tier"] = "Gold" });
        (await app.WaitForInboxSettledAsync(acct, version: 1)).Should().Be((1L, "done"));

        var writes = await app.GetWriteRequestsAsync(acct);
        writes.Select(w => ((string?)w["method"], (string?)w["url"])).Should().Equal(("PATCH", AccountUpsert), ("PATCH", ProfileUpsert));
        writes.Should().OnlyContain(w => !w["body"]!["allOrNone"]!.GetValue<bool>());
        var profile = Records(writes[1]).Single(r => (string?)r["ExternalId__c"] == SyncFixture.ProfileIdFor(acct));
        ((string?)profile["AccountId__c"]).Should().Be(acct, "the Account's key as text, no reference");

        ((string?)(await app.GetRecordAsync("Profile__c", SyncFixture.ProfileIdFor(acct)))!["AccountId__c"]).Should().Be(acct);
    }

    [Fact]
    public async Task Account_saved_but_Profile_failed_is_retried_and_the_entity_is_done_only_with_both()
    {
        var acct = SyncFixture.NewAccountId();
        // Retried after about 1 s, 2 s and 4 s: long enough to look at the state in between
        await app.InjectFaultAsync("Profile__c", SyncFixture.ProfileIdFor(acct), "UNABLE_TO_LOCK_ROW", times: 3);

        await app.PublishAsync(acct, 1, new JsonObject { ["name"] = Any.String("Acme ") }, new JsonObject { ["tier"] = "Gold" });

        await Eventually.ValueAsync(() => app.GetRecordAsync("Account", acct), a => a is not null, "the Account to be created");
        await using (var conn = await app.Db.OpenConnectionAsync())
        {
            var (status, code) = await conn.QuerySingleAsync<(string, string?)>(
                $"SELECT {SyncFixture.StatusSql}, last_error_code FROM {SyncFixture.InboxTable()} WHERE entity_id = @acct", new { acct });
            status.Should().Be("pending", "the entity is not synced while one of its records is missing");
            code.Should().BeOneOf("UNABLE_TO_LOCK_ROW", null);
        }
        (await app.GetRecordAsync("Profile__c", SyncFixture.ProfileIdFor(acct))).Should().BeNull();

        (await app.WaitForInboxSettledAsync(acct, version: 1)).Should().Be((1L, "done"));
        (await app.GetInboxRowAsync(acct))!.AttemptCount.Should().BePositive();
        (await app.GetHistoryAsync("Account", acct)).Should().ContainSingle("the retries had nothing left to write on the Account");
        ((string?)(await app.GetRecordAsync("Profile__c", SyncFixture.ProfileIdFor(acct)))!["AccountId__c"]).Should().Be(acct);
    }

    [Fact]
    public async Task Failing_Account_is_dead_lettered_alone_and_its_Profile_rejected_with_it_while_others_are_saved()
    {
        var (bad, good) = (SyncFixture.NewAccountId(), SyncFixture.NewAccountId());
        await app.InjectFaultAsync("Account", bad, "FIELD_CUSTOM_VALIDATION_EXCEPTION", times: int.MaxValue);

        await app.PublishAsync(bad, 1, new JsonObject { ["name"] = Any.String("Bad ") }, new JsonObject { ["tier"] = "Gold" });
        await app.PublishAsync(good, 1, new JsonObject { ["name"] = Any.String("Good ") }, new JsonObject { ["tier"] = "Silver" });

        (await app.WaitForInboxSettledAsync(bad, version: 1)).Should().Be((1L, "dead"));
        (await app.GetInboxRowAsync(bad))!.ErrorCode.Should().Be("FIELD_CUSTOM_VALIDATION_EXCEPTION",
            "the Account's own error, not the Profile's missing parent");
        (await app.GetRecordAsync("Account", bad)).Should().BeNull();
        (await app.GetRecordAsync("Profile__c", SyncFixture.ProfileIdFor(bad))).Should().BeNull();
        (await app.GetWriteRequestsAsync(SyncFixture.ProfileIdFor(bad))).Should().BeEmpty(
            "the Profile is not sent: its Account failed");

        (await app.WaitForInboxSettledAsync(good, version: 1)).Should().Be((1L, "done"));
        ((string?)(await app.GetRecordAsync("Profile__c", SyncFixture.ProfileIdFor(good)))!["AccountId__c"]).Should().Be(good);
    }

    [Fact]
    public async Task An_insert_overwritten_by_an_update_before_it_is_claimed_becomes_one_create_with_the_newest_data()
    {
        var acct = SyncFixture.NewAccountId();

        await app.InsertIntoInboxAsync(acct,
            SyncFixture.AccountEvent(acct, 1, new JsonObject { ["name"] = "v1", ["rating"] = "Hot" }, []),
            SyncFixture.AccountEvent(acct, 2, new JsonObject { ["name"] = "v2" }, []));

        (await app.WaitForInboxSettledAsync(acct, version: 2)).Should().Be((2L, "done"));
        var write = (await AccountWritesAsync(acct)).Should().ContainSingle().Subject;
        Records(write).Single().Should().ContainKey("Name").And.NotContainKey("Rating",
            "the newest event says updated, but Salesforce doesn't have the record: it is created from the full record");
        var history = (await app.GetHistoryAsync("Account", acct)).Should().ContainSingle().Subject;
        ((string?)history["Name"]).Should().Be("v2");
        history.ContainsKey("Rating").Should().BeFalse("nothing of the overwritten insert is written");
    }

    [Fact]
    public async Task Redelivered_insert_of_a_record_that_changed_since_updates_only_what_differs()
    {
        var acct = SyncFixture.NewAccountId();
        var v1 = SyncFixture.AccountEvent(acct, 1, new JsonObject { ["name"] = "v1", ["rating"] = "Hot" }, []);
        await app.InsertIntoInboxAsync(acct, v1);
        await app.WaitForInboxSettledAsync(acct, version: 1);

        // The same data again, as a later "inserted" (a source that re-sends its inserts)
        var again = (JsonObject)v1.DeepClone();
        again["meta"] = SyncFixture.Meta(2, "inserted");
        again["account"]!["rating"] = "Cold";
        await app.InsertIntoInboxAsync(acct, again);

        (await app.WaitForInboxSettledAsync(acct, version: 2)).Should().Be((2L, "done"));
        var writes = await AccountWritesAsync(acct);
        writes.Should().HaveCount(2);
        ((string?)writes[1]["method"], (string?)writes[1]["url"]).Should().Be(("PATCH", AccountUpsert),
            "only the Account differs (the Profile is unchanged): one upsert by external id");
        Records(writes[1]).Single().Select(kv => kv.Key).Should().BeEquivalentTo(["attributes", "ExternalId__c", "Rating"],
            "the existing record is found by its external id and gets only what differs");
        (await app.GetHistoryAsync("Account", acct))[1].Select(kv => kv.Key).Should().Equal("Rating");
    }

    [Fact]
    public async Task A_change_of_the_Account_alone_is_one_upsert_and_the_unchanged_Profile_and_its_key_are_not_written()
    {
        var acct = SyncFixture.NewAccountId();
        await app.PublishAsync(acct, 1, new JsonObject { ["name"] = "Acme", ["rating"] = "Hot" }, new JsonObject { ["tier"] = "Gold" });
        await app.WaitForInboxSettledAsync(acct, version: 1);

        await app.PublishAsync(acct, 2, new JsonObject { ["name"] = "Acme", ["rating"] = "Cold" }, new JsonObject { ["tier"] = "Gold" });
        await app.WaitForInboxSettledAsync(acct, version: 2);

        var writes = await app.GetWriteRequestsAsync(acct);
        writes.Select(w => (string?)w["url"]).Should().Equal([AccountUpsert, ProfileUpsert, AccountUpsert],
            "the creates, then the Account's change as an upsert of its own");
        Records(writes[2]).Single().Select(kv => kv.Key).Should().BeEquivalentTo("attributes", "ExternalId__c", "Rating");
        (await app.GetHistoryAsync("Profile__c", SyncFixture.ProfileIdFor(acct))).Should().ContainSingle("the Profile is not written");
    }

    [Fact]
    public async Task A_change_of_the_Account_and_its_Profile_is_two_upserts_Account_first()
    {
        var acct = SyncFixture.NewAccountId();
        await app.PublishAsync(acct, 1, new JsonObject { ["name"] = "Acme", ["rating"] = "Hot" }, new JsonObject { ["tier"] = "Gold" });
        await app.WaitForInboxSettledAsync(acct, version: 1);

        await app.PublishAsync(acct, 2, new JsonObject { ["name"] = "Acme", ["rating"] = "Cold" }, new JsonObject { ["tier"] = "Silver" });
        (await app.WaitForInboxSettledAsync(acct, version: 2)).Should().Be((2L, "done"));

        var writes = await app.GetWriteRequestsAsync(acct, SyncFixture.ProfileIdFor(acct));
        writes.Should().HaveCount(4, "the creates, then the changes: an upsert per object each time");
        var change = writes[2..];
        change.Select(w => ((string?)w["method"], (string?)w["url"])).Should().Equal(("PATCH", AccountUpsert), ("PATCH", ProfileUpsert));
        Records(change[0]).Single(r => (string?)r["ExternalId__c"] == acct).Select(kv => kv.Key).Should()
            .BeEquivalentTo("attributes", "ExternalId__c", "Rating");
        Records(change[1]).Single(r => (string?)r["ExternalId__c"] == SyncFixture.ProfileIdFor(acct)).Select(kv => kv.Key).Should()
            .BeEquivalentTo("attributes", "ExternalId__c", "Tier__c");
        ((string?)(await app.GetRecordAsync("Profile__c", SyncFixture.ProfileIdFor(acct)))!["Tier__c"]).Should().Be("Silver");
    }

    /// <summary>The upserts of Accounts that wrote <paramref name="acct"/> (its Profile's carry its key too).</summary>
    private async Task<List<JsonObject>> AccountWritesAsync(string acct)
    {
        return [.. (await app.GetWriteRequestsAsync(acct)).Where(w => (string?)w["url"] == AccountUpsert)];
    }

    private static List<JsonObject> Records(JsonNode collection)
    {
        return [.. collection["body"]!["records"]!.AsArray().Select(r => r!.AsObject())];
    }
}