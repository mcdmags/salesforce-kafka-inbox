using System.Net.Http.Json;
using System.Text.Json.Nodes;
using SalesforceSync.FakeSalesforce;

namespace SalesforceSync.IntegrationTests;

/// <summary>
/// Contacts through the same engine in its other write mode: Kafka -> inbox -> sObject Collections upsert by external
/// id, without a read or a diff, the Account set by its external id.
/// </summary>
[Collection(SyncCollection.Name)]
public sealed class ContactTests(SyncFixture app)
{
    [Fact]
    public async Task Contact_is_upserted_with_its_Account_by_external_id_and_every_event_is_written_whole()
    {
        var (acct, contact) = (SyncFixture.NewAccountId(), SyncFixture.NewContactId());
        await app.PublishAsync(acct, 1, new JsonObject { ["name"] = Any.String("Acme ") }, []);
        await app.WaitForInboxSettledAsync(acct, 1);

        await app.PublishContactAsync(contact, 1, acct, new JsonObject { ["lastName"] = "Lovelace", ["phone"] = "+1 555 0101" });
        (await app.WaitForInboxSettledAsync(contact, 1, SyncFixture.Contacts)).Should().Equal((1L, "done"));

        var record = await app.GetRecordAsync("Contact", contact);
        record.Should().NotBeNull();
        ((string?)record!["AccountId"]).Should().Be((string?)(await app.GetRecordAsync("Account", acct))!["Id"]);
        ((string?)record["RecordTypeId"]).Should().Be(FakeSchema.RecordTypes["Contact"].Single(r => r.DeveloperName == "Standard").Id);
        var write = (await app.GetWriteRequestsAsync(contact)).Should().ContainSingle().Subject;
        ((string?)write["method"], (string?)write["url"]).Should().Be(("PATCH", "/services/data/v62.0/composite/sobjects/Contact/External_Id__c"));
        write["body"]!["allOrNone"]!.GetValue<bool>().Should().BeFalse();
        var sent = write["body"]!["records"]!.AsArray().Single()!;
        JsonNode.DeepEquals(sent["Account"], new JsonObject
        {
            ["attributes"] = new JsonObject { ["type"] = "Account" },
            ["External_Id__c"] = acct
        }).Should().BeTrue("the Account is named by its external id through the relationship: {0}", sent.ToJsonString());

        await app.PublishContactAsync(contact, 2, acct, new JsonObject { ["lastName"] = "Lovelace" });
        (await app.WaitForInboxSettledAsync(contact, 2, SyncFixture.Contacts)).Should().Equal((1L, "done"), (2L, "done"));
        var writes = await app.GetHistoryAsync("Contact", contact);
        writes.Should().HaveCount(2, "the same contact, updated by its external id");
        writes[1].Select(kv => kv.Key).Should().Contain(["LastName", "Phone", "AccountId"], "without a diff, the whole record is sent");
        (await app.GetRecordAsync("Contact", contact))!["Phone"].Should().BeNull("an empty field is cleared");
    }

    [Fact]
    public async Task Contact_that_arrives_before_its_Account_waits_for_it_then_syncs()
    {
        var (acct, contact) = (SyncFixture.NewAccountId(), SyncFixture.NewContactId());

        await app.PublishContactAsync(contact, 1, acct, new JsonObject { ["lastName"] = "Early" });

        // Salesforce finds no Account with that external id: retried, not dead-lettered
        var waiting = await Eventually.ValueAsync(
            () => app.GetInboxRowsAsync(contact, SyncFixture.Contacts),
            rows => rows.Count == 1 && rows[0].ErrorCode == "DEPENDENCY_NOT_SYNCED",
            "the contact to wait for its account");
        waiting[0].Status.Should().Be("pending", "a missing parent is retried, not dead-lettered");
        (await app.GetRecordAsync("Contact", contact)).Should().BeNull();

        await app.PublishAsync(acct, 1, new JsonObject { ["name"] = Any.String("Late ") }, []);

        var settled = await Eventually.ValueAsync(
            () => app.GetInboxRowsAsync(contact, SyncFixture.Contacts),
            rows => rows.Count == 1 && rows[0].Status != "pending",
            "the contact to sync once its account exists");
        settled[0].Status.Should().Be("done");
        ((string?)(await app.GetRecordAsync("Contact", contact))!["AccountId"])
            .Should().Be((string?)(await app.GetRecordAsync("Account", acct))!["Id"]);
    }

    [Fact]
    public async Task Contacts_Salesforce_would_reject_are_dead_lettered_alone()
    {
        var acct = SyncFixture.NewAccountId();
        var (noName, noAccount, good) = (SyncFixture.NewContactId(), SyncFixture.NewContactId(), SyncFixture.NewContactId());
        await app.PublishAsync(acct, 1, new JsonObject { ["name"] = Any.String("Acme ") }, []);
        await app.WaitForInboxSettledAsync(acct, 1);

        await app.PublishContactAsync(noName, 1, acct, new JsonObject { ["firstName"] = "Nameless" });
        await app.PublishContactAsync(noAccount, 1, "", new JsonObject { ["lastName"] = "Orphan" });
        await app.PublishContactAsync(good, 1, acct, new JsonObject { ["lastName"] = "Good" });

        (await app.WaitForInboxSettledAsync(noName, 1, SyncFixture.Contacts)).Should().Equal((1L, "dead"));
        (await app.GetInboxRowsAsync(noName, SyncFixture.Contacts))[0].ErrorCode.Should().Be("REQUIRED_FIELD_MISSING");
        (await app.WaitForInboxSettledAsync(noAccount, 1, SyncFixture.Contacts)).Should().Equal((1L, "dead"));
        (await app.GetInboxRowsAsync(noAccount, SyncFixture.Contacts))[0].ErrorCode.Should().Be("INVALID_EVENT");
        (await app.WaitForInboxSettledAsync(good, 1, SyncFixture.Contacts)).Should().Equal((1L, "done"));
        (await app.GetWriteRequestsAsync(noName, noAccount)).Should().BeEmpty("both failed before any call");
    }

    [Fact]
    public async Task Contact_whose_upsert_answer_is_lost_is_held_then_ends_at_the_newest_event()
    {
        var (acct, contact) = (SyncFixture.NewAccountId(), SyncFixture.NewContactId());
        await app.PublishAsync(acct, 1, new JsonObject { ["name"] = Any.String("Acme ") }, []);
        await app.WaitForInboxSettledAsync(acct, 1);
        using (var lose = await app.Salesforce.PostAsJsonAsync("/__fake/lost-responses",
                   new JsonObject { ["sObject"] = "Contact", ["externalId"] = contact, ["times"] = 1 }))
        {
            lose.EnsureSuccessStatusCode();
        }

        await app.PublishContactAsync(contact, 1, acct, new JsonObject { ["lastName"] = "v1" });
        var held = (await Eventually.ValueAsync(() => app.GetInboxRowsAsync(contact, SyncFixture.Contacts),
            rows => rows.Count == 1 && rows[0].ErrorCode == "UNKNOWN_OUTCOME", "the upsert's outcome to be recorded as unknown"))[0];
        held.Status.Should().Be("pending");
        held.RetryCount.Should().Be(0, "an unknown outcome doesn't spend a retry");

        await app.PublishContactAsync(contact, 2, acct, new JsonObject { ["lastName"] = "v2" });

        (await app.WaitForInboxSettledAsync(contact, 2, SyncFixture.Contacts)).Should().Equal((1L, "superseded"), (2L, "done"));
        (await app.GetHistoryAsync("Contact", contact)).Select(w => (string?)w["LastName"]).Should().Equal(["v1", "v2"],
            "the older record is never written after the newer one");
    }
}