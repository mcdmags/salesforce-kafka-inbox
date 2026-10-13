using System.Text.Json.Nodes;

namespace SalesforceSync.IntegrationTests;

/// <summary>Account_Relationship__c through the same engine as accounts: Kafka -> inbox -> generic upsert.</summary>
[Collection(AppHostCollection.Name)]
public sealed class RelationshipTests(AppHostFixture app)
{
    [Fact]
    public async Task Relationship_links_two_synced_Accounts_and_updates_only_what_changed()
    {
        var (parent, child, rel) = (AppHostFixture.NewAccountId(), AppHostFixture.NewAccountId(), AppHostFixture.NewRelationshipId());
        await app.PublishAsync(parent, 1, new JsonObject { ["Name"] = "Parent" }, new JsonObject());
        await app.PublishAsync(child, 1, new JsonObject { ["Name"] = "Child" }, new JsonObject());
        await app.WaitForInboxSettledAsync(parent, 1);
        await app.WaitForInboxSettledAsync(child, 1);

        await app.PublishRelationshipAsync(rel, 1, parent, child, new JsonObject { ["Type__c"] = "Parent", ["Active__c"] = true });
        Assert.Equal(new[] { (1L, "done") }, await app.WaitForInboxSettledAsync(rel, 1, AppHostFixture.Relationships));

        var record = await app.GetRecordAsync("Account_Relationship__c", rel);
        Assert.NotNull(record);
        Assert.Equal((string?)(await app.GetRecordAsync("Account", parent))!["Id"], (string?)record["Account__c"]);
        Assert.Equal((string?)(await app.GetRecordAsync("Account", child))!["Id"], (string?)record["Related_Account__c"]);
        Assert.Equal("Parent", (string?)record["Type__c"]);

        await app.PublishRelationshipAsync(rel, 2, parent, child, new JsonObject { ["Type__c"] = "Partner", ["Active__c"] = true });
        await app.WaitForInboxSettledAsync(rel, 2, AppHostFixture.Relationships);
        var writes = await app.GetHistoryAsync("Account_Relationship__c", rel);
        Assert.Equal(2, writes.Count);
        Assert.Equal(["Type__c"], writes[1].Select(kv => kv.Key));
    }

    [Fact]
    public async Task Relationship_that_arrives_before_its_Accounts_waits_for_them_then_syncs()
    {
        var (parent, child, rel) = (AppHostFixture.NewAccountId(), AppHostFixture.NewAccountId(), AppHostFixture.NewRelationshipId());

        await app.PublishRelationshipAsync(rel, 1, parent, child, new JsonObject { ["Type__c"] = "Subsidiary" });

        // Retried (not dead-lettered) while the accounts aren't in Salesforce
        var waiting = await Eventually.ValueAsync(
            () => app.GetInboxRowsAsync(rel, AppHostFixture.Relationships),
            rows => rows.Count == 1 && rows[0].ErrorCode == "DEPENDENCY_NOT_SYNCED",
            "the relationship to wait for its accounts");
        Assert.Equal("pending", waiting[0].Status);
        Assert.Null(await app.GetRecordAsync("Account_Relationship__c", rel));

        await app.PublishAsync(parent, 1, new JsonObject { ["Name"] = "Late parent" }, new JsonObject());
        await app.PublishAsync(child, 1, new JsonObject { ["Name"] = "Late child" }, new JsonObject());

        var settled = await Eventually.ValueAsync(
            () => app.GetInboxRowsAsync(rel, AppHostFixture.Relationships),
            rows => rows.Count == 1 && rows[0].Status != "pending",
            "the relationship to sync once its accounts exist");
        Assert.Equal("done", settled[0].Status);
        var record = await app.GetRecordAsync("Account_Relationship__c", rel);
        Assert.NotNull(record);
        Assert.Equal((string?)(await app.GetRecordAsync("Account", child))!["Id"], (string?)record["Related_Account__c"]);
    }

    [Fact]
    public async Task Malformed_relationship_event_is_dead_lettered_alone()
    {
        var rel = AppHostFixture.NewRelationshipId();

        await app.PublishRelationshipAsync(rel, 1, AppHostFixture.NewAccountId(), "", new JsonObject());

        var rows = await app.WaitForInboxSettledAsync(rel, 1, AppHostFixture.Relationships);
        Assert.Equal("dead", rows[0].Status);
        Assert.Equal("INVALID_EVENT", (await app.GetInboxRowsAsync(rel, AppHostFixture.Relationships))[0].ErrorCode);
    }
}
