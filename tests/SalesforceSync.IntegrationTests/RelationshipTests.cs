using System.Text.Json.Nodes;
using SalesforceSync.FakeSalesforce;

namespace SalesforceSync.IntegrationTests;

/// <summary>Account_Relationship__c through the same engine as accounts: Kafka -> inbox -> generic upsert.</summary>
[Collection(SyncCollection.Name)]
public sealed class RelationshipTests(SyncFixture app)
{
    [Fact]
    public async Task Relationship_links_two_synced_Accounts_and_updates_only_what_changed()
    {
        var (parent, child, rel) = (SyncFixture.NewAccountId(), SyncFixture.NewAccountId(), SyncFixture.NewRelationshipId());
        await app.PublishAsync(parent, 1, new JsonObject { ["name"] = Any.String("Parent ") }, []);
        await app.PublishAsync(child, 1, new JsonObject { ["name"] = Any.String("Child ") }, []);
        await app.WaitForInboxSettledAsync(parent, 1);
        await app.WaitForInboxSettledAsync(child, 1);

        await app.PublishRelationshipAsync(rel, 1, parent, child, new JsonObject { ["type"] = "Parent", ["isActive"] = true });
        (await app.WaitForInboxSettledAsync(rel, 1, SyncFixture.Relationships)).Should().Equal((1L, "done"));

        var write = (await app.GetWriteRequestsAsync(rel)).Should().ContainSingle("one upsert, the Accounts' keys as text").Subject;
        ((string?)write["url"]).Should().Be("/services/data/v62.0/composite/sobjects/Account_Relationship__c/ExternalId__c");
        var sent = write["body"]!["records"]!.AsArray().Single()!;
        ((string?)sent["AccountId__c"], (string?)sent["RelatedAccountId__c"]).Should().Be((parent, child));
        var record = await app.GetRecordAsync("Account_Relationship__c", rel);
        record.Should().NotBeNull();
        ((string?)record!["AccountId__c"], (string?)record["RelatedAccountId__c"]).Should().Be((parent, child));
        ((string?)record["Type__c"]).Should().Be("Parent");
        ((string?)record["RecordTypeId"]).Should().Be(
            FakeSchema.RecordTypes["Account_Relationship__c"].Single(r => r.DeveloperName == "Standard").Id);

        await app.PublishRelationshipAsync(rel, 2, parent, child, new JsonObject { ["type"] = "Partner", ["isActive"] = true });
        await app.WaitForInboxSettledAsync(rel, 2, SyncFixture.Relationships);
        var writes = await app.GetHistoryAsync("Account_Relationship__c", rel);
        writes.Should().HaveCount(2);
        writes[1].Select(kv => kv.Key).Should().Equal(["Type__c"], "only the changed field is sent: the unchanged keys are not");

        var other = SyncFixture.NewAccountId();
        await app.PublishAsync(other, 1, new JsonObject { ["name"] = Any.String("Other ") }, []);
        await app.WaitForInboxSettledAsync(other, 1);
        await app.PublishRelationshipAsync(rel, 3, parent, other, new JsonObject { ["type"] = "Partner", ["isActive"] = true });
        await app.WaitForInboxSettledAsync(rel, 3, SyncFixture.Relationships);
        (await app.GetHistoryAsync("Account_Relationship__c", rel))[2].Select(kv => kv.Key).Should().Equal(["RelatedAccountId__c"],
            "a changed key is written");
        ((string?)(await app.GetRecordAsync("Account_Relationship__c", rel))!["RelatedAccountId__c"]).Should().Be(other);
    }

    [Fact]
    public async Task Relationship_that_arrives_before_its_Accounts_waits_for_them_then_syncs()
    {
        var (parent, child, rel) = (SyncFixture.NewAccountId(), SyncFixture.NewAccountId(), SyncFixture.NewRelationshipId());

        await app.PublishRelationshipAsync(rel, 1, parent, child, new JsonObject { ["type"] = "Subsidiary" });

        // Retried (not dead-lettered) while the accounts aren't in Salesforce
        var waiting = await Eventually.ValueAsync(
            () => app.GetInboxRowsAsync(rel, SyncFixture.Relationships),
            rows => rows.Count == 1 && rows[0].ErrorCode == "DEPENDENCY_NOT_SYNCED",
            "the relationship to wait for its accounts");
        waiting[0].Status.Should().Be("pending", "a missing dependency is retried, not dead-lettered");
        (await app.GetRecordAsync("Account_Relationship__c", rel)).Should().BeNull();
        (await app.GetWriteRequestsAsync(rel)).Should().BeEmpty("the read found no Account: nothing is sent");

        await app.PublishAsync(parent, 1, new JsonObject { ["name"] = Any.String("Late parent ") }, []);
        await app.PublishAsync(child, 1, new JsonObject { ["name"] = Any.String("Late child ") }, []);

        var settled = await Eventually.ValueAsync(
            () => app.GetInboxRowsAsync(rel, SyncFixture.Relationships),
            rows => rows.Count == 1 && rows[0].Status != "pending",
            "the relationship to sync once its accounts exist");
        settled[0].Status.Should().Be("done");
        var record = await app.GetRecordAsync("Account_Relationship__c", rel);
        record.Should().NotBeNull();
        ((string?)record!["RelatedAccountId__c"]).Should().Be(child);
    }

    [Fact]
    public async Task Malformed_relationship_event_is_dead_lettered_alone()
    {
        var rel = SyncFixture.NewRelationshipId();

        await app.PublishRelationshipAsync(rel, 1, SyncFixture.NewAccountId(), "", []);

        var rows = await app.WaitForInboxSettledAsync(rel, 1, SyncFixture.Relationships);
        rows[0].Status.Should().Be("dead");
        (await app.GetInboxRowsAsync(rel, SyncFixture.Relationships))[0].ErrorCode.Should().Be("INVALID_EVENT");
        rows.Should().ContainSingle("it is stored in the relationship inbox, not the account one");
        (await app.GetInboxAsync(rel)).Should().BeEmpty();
    }
}