using System.Text.Json.Nodes;
using Dapper;

namespace SalesforceSync.IntegrationTests;

/// <summary>
/// What is sent to Salesforce and what per-record results do to an entity: one Composite request of sObject
/// Collections creates and updates, each record saved or rejected on its own (allOrNone false, the default).
/// </summary>
[Collection(SyncCollection.Name)]
public sealed class CollectionsWriteTests(SyncFixture app)
{
    [Fact]
    public async Task New_Account_and_Profile_are_created_in_one_request_with_the_Profile_referring_to_its_Account()
    {
        var acct = SyncFixture.NewAccountId();

        await app.PublishAsync(acct, 1, new JsonObject { ["name"] = Any.String("Acme ") }, new JsonObject { ["tier"] = "Gold" });
        (await app.WaitForInboxSettledAsync(acct, expectedRows: 1)).Should().Equal((1L, "done"));

        var write = (await app.GetWriteRequestsAsync(acct)).Should().ContainSingle().Subject;
        write["allOrNone"]!.GetValue<bool>().Should().BeFalse();
        var subrequests = write["compositeRequest"]!.AsArray();
        subrequests.Select(s => ((string?)s!["method"], (string?)s["referenceId"], s["body"]!["allOrNone"]!.GetValue<bool>()))
            .Should().Equal(("POST", "Account_create", false), ("POST", "Profile__c_create_ref", false));
        var position = Records(subrequests[0]!).FindIndex(r => (string?)r["External_Id__c"] == acct);
        var profile = Records(subrequests[1]!).Single(r => (string?)r["External_Id__c"] == SyncFixture.ProfileIdFor(acct));
        ((string?)profile["Account__c"]).Should().Be($"@{{Account_create[{position}].id}}");

        ((string?)(await app.GetRecordAsync("Profile__c", SyncFixture.ProfileIdFor(acct)))!["Account__c"])
            .Should().Be((string?)(await app.GetRecordAsync("Account", acct))!["Id"]);
    }

    [Fact]
    public async Task Account_saved_but_Profile_failed_is_retried_and_the_watermark_waits_for_both()
    {
        var acct = SyncFixture.NewAccountId();
        // Retried after about 1 s, 2 s and 4 s: long enough to look at the state in between
        await app.InjectFaultAsync("Profile__c", SyncFixture.ProfileIdFor(acct), "UNABLE_TO_LOCK_ROW", times: 3);

        await app.PublishAsync(acct, 1, new JsonObject { ["name"] = Any.String("Acme ") }, new JsonObject { ["tier"] = "Gold" });

        await Eventually.ValueAsync(() => app.GetRecordAsync("Account", acct), a => a is not null, "the Account to be created");
        await using (var conn = await app.Db.OpenConnectionAsync())
        {
            var (status, code, watermarks) = await conn.QuerySingleAsync<(string, string?, long)>(
                $"""
                select status, last_error_code,
                       (select count(*) from sf_sync_state where entity_type = 'account' and entity_id = @acct)
                from {SyncFixture.InboxTable()} where entity_id = @acct
                """, new { acct });
            status.Should().Be("pending", "the entity is not synced while one of its records is missing");
            watermarks.Should().Be(0, "the watermark advances only when the Account and the Profile are both in");
            code.Should().BeOneOf("UNABLE_TO_LOCK_ROW", null);
        }
        (await app.GetRecordAsync("Profile__c", SyncFixture.ProfileIdFor(acct))).Should().BeNull();

        (await app.WaitForInboxSettledAsync(acct, expectedRows: 1)).Should().Equal((1L, "done"));
        (await app.GetInboxRowsAsync(acct))[0].RetryCount.Should().BePositive();
        ((string?)(await app.GetRecordAsync("Profile__c", SyncFixture.ProfileIdFor(acct)))!["Account__c"])
            .Should().Be((string?)(await app.GetRecordAsync("Account", acct))!["Id"]);
        (await app.GetHistoryAsync("Account", acct)).Should().ContainSingle("the retries had nothing left to write on the Account");
        await using var after = await app.Db.OpenConnectionAsync();
        (await after.ExecuteScalarAsync<long>(
            "select count(*) from sf_sync_state where entity_type = 'account' and entity_id = @acct", new { acct })).Should().Be(1);
    }

    [Fact]
    public async Task Failing_Account_is_dead_lettered_alone_without_its_Profile_while_others_are_saved()
    {
        var (bad, good) = (SyncFixture.NewAccountId(), SyncFixture.NewAccountId());
        await app.InjectFaultAsync("Account", bad, "FIELD_CUSTOM_VALIDATION_EXCEPTION", times: int.MaxValue);

        await app.PublishAsync(bad, 1, new JsonObject { ["name"] = Any.String("Bad ") }, new JsonObject { ["tier"] = "Gold" });
        await app.PublishAsync(good, 1, new JsonObject { ["name"] = Any.String("Good ") }, new JsonObject { ["tier"] = "Silver" });

        (await app.WaitForInboxSettledAsync(bad, expectedRows: 1)).Should().Equal((1L, "dead"));
        (await app.GetInboxRowsAsync(bad))[0].ErrorCode.Should().Be("FIELD_CUSTOM_VALIDATION_EXCEPTION",
            "the Account's own error, not the Profile's missing parent");
        (await app.GetRecordAsync("Account", bad)).Should().BeNull();
        (await app.GetRecordAsync("Profile__c", SyncFixture.ProfileIdFor(bad))).Should().BeNull();

        (await app.WaitForInboxSettledAsync(good, expectedRows: 1)).Should().Equal((1L, "done"));
        ((string?)(await app.GetRecordAsync("Profile__c", SyncFixture.ProfileIdFor(good)))!["Account__c"])
            .Should().Be((string?)(await app.GetRecordAsync("Account", good))!["Id"]);
    }

    [Fact]
    public async Task Inserted_and_updated_pending_together_become_one_create_with_the_newest_data()
    {
        var acct = SyncFixture.NewAccountId();

        await app.InsertIntoInboxAsync(acct,
            SyncFixture.AccountEvent(acct, 1, new JsonObject { ["name"] = "v1", ["rating"] = "Hot" }, new JsonObject()),
            SyncFixture.AccountEvent(acct, 2, new JsonObject { ["name"] = "v2" }, new JsonObject()));

        (await app.WaitForInboxSettledAsync(acct, expectedRows: 2)).Should().Equal((1L, "superseded"), (2L, "done"));
        var write = (await app.GetWriteRequestsAsync(acct)).Should().ContainSingle().Subject;
        ((string?)write["compositeRequest"]![0]!["method"]).Should().Be("POST",
            "the newest event says updated, but Salesforce doesn't have the record: it is created from the full record");
        var history = (await app.GetHistoryAsync("Account", acct)).Should().ContainSingle().Subject;
        ((string?)history["Name"]).Should().Be("v2");
        history.ContainsKey("Rating").Should().BeFalse("nothing of the superseded insert is written");
    }

    [Fact]
    public async Task Redelivered_insert_of_a_record_that_changed_since_updates_only_what_differs()
    {
        var acct = SyncFixture.NewAccountId();
        var v1 = SyncFixture.AccountEvent(acct, 1, new JsonObject { ["name"] = "v1", ["rating"] = "Hot" }, new JsonObject());
        await app.InsertIntoInboxAsync(acct, v1);
        await app.WaitForInboxSettledAsync(acct, expectedRows: 1);

        // The same data again, as a later "inserted" (a source that re-sends its inserts)
        var again = (JsonObject)v1.DeepClone();
        again["meta"] = SyncFixture.Meta(2, "inserted");
        again["account"]!["rating"] = "Cold";
        await app.InsertIntoInboxAsync(acct, again);

        (await app.WaitForInboxSettledAsync(acct, expectedRows: 2)).Should().OnlyContain(r => r.Status == "done");
        var writes = await app.GetWriteRequestsAsync(acct, (string)(await app.GetRecordAsync("Account", acct))!["Id"]!);
        writes.Should().HaveCount(2);
        ((string?)writes[1]["compositeRequest"]![0]!["method"]).Should().Be("PATCH", "no duplicate is created");
        (await app.GetHistoryAsync("Account", acct))[1].Select(kv => kv.Key).Should().Equal("Rating");
    }

    private static List<JsonObject> Records(JsonNode collection)
        => collection["body"]!["records"]!.AsArray().Select(r => r!.AsObject()).ToList();
}
