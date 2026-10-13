using System.Text.Json;
using SalesforceSync.Inbox;
using SalesforceSync.Sync;

namespace SalesforceSync.Tests;

/// <summary>
/// The raw event format: how an envelope maps to Salesforce records, how an entity's envelopes are merged, and
/// what the Kafka consumer admits into the inbox.
/// </summary>
public sealed class InboxEventTests
{
    private static readonly HashSet<string> Types = [AccountSyncService.Type, AccountRelationshipSyncService.Type];

    // ---------- Mapping ----------

    [Fact]
    public void Account_fields_are_everything_but_externalId_and_profile_and_the_profile_defaults_to_the_account_id()
    {
        var unit = AccountSyncService.Map("ACC-1", Read<AccountEvent>("""
            { "meta": { "time": "2026-10-14T01:02:03.456Z" },
              "account": { "externalId": "ACC-1", "Name": "Acme", "Rating": null,
                           "profile": { "Tier__c": "Gold", "Score__c": 7 } } }
            """));

        var (account, profile) = (unit.Records[0], unit.Records[1]);
        Assert.Equal(("Account", "ACC-1"), (account.SObject, account.ExternalId));
        Assert.Equal(new Dictionary<string, object?> { ["Name"] = "Acme", ["Rating"] = null }, account.Fields);
        Assert.Equal(("Profile__c", "ACC-1"), (profile.SObject, profile.ExternalId));
        Assert.Equal(new Dictionary<string, object?> { ["Tier__c"] = "Gold", ["Score__c"] = 7m }, profile.Fields);
        Assert.Equal(new SObjectLookup("Account__c", "Account", "External_Id__c", "ACC-1"), Assert.Single(profile.Lookups!));
    }

    [Fact]
    public void Profile_externalId_is_used_when_given()
    {
        var unit = AccountSyncService.Map("ACC-1", Read<AccountEvent>("""
            { "account": { "externalId": "ACC-1", "profile": { "externalId": "PRF-1", "Tier__c": "Gold" } } }
            """));

        Assert.Equal("PRF-1", unit.Records[1].ExternalId);
        Assert.Equal(["Tier__c"], unit.Records[1].Fields.Keys);
    }

    [Fact]
    public void Account_without_a_profile_writes_only_the_Account()
    {
        var unit = AccountSyncService.Map("ACC-1", Read<AccountEvent>("""
            { "meta": { "time": "2026-10-14T01:02:03.456Z" }, "account": { "externalId": "ACC-1", "Name": "Acme" } }
            """));

        Assert.Equal("Account", Assert.Single(unit.Records).SObject);
    }

    [Fact]
    public void Relationship_fields_are_everything_but_the_ids()
    {
        var unit = AccountRelationshipSyncService.Map("REL-1", Read<AccountRelationshipEvent>("""
            { "meta": { "time": "2026-10-14T01:02:03.456Z" },
              "accountRelationship": { "externalId": "REL-1", "accountExternalId": "ACC-1",
                                       "relatedAccountExternalId": "ACC-2", "Type__c": "Parent" } }
            """));

        var rel = Assert.Single(unit.Records);
        Assert.Equal(("Account_Relationship__c", "REL-1"), (rel.SObject, rel.ExternalId));
        Assert.Equal(["Type__c"], rel.Fields.Keys);
        Assert.Equal(["ACC-1", "ACC-2"], rel.Lookups!.Select(l => l.ExternalId));
    }

    [Theory]
    [InlineData("""{ "accountRelationship": { "externalId": "REL-1", "accountExternalId": "ACC-1" } }""")]
    [InlineData("""{ "accountRelationship": { "externalId": "REL-1", "accountExternalId": "ACC-1", "relatedAccountExternalId": "" } }""")]
    [InlineData("""{ "meta": { "time": "2026-10-14T01:02:03.456Z" } }""")]
    public void Relationship_without_both_accounts_cannot_be_mapped(string json)
        => Assert.Throws<ArgumentException>(() => AccountRelationshipSyncService.Map("REL-1", Read<AccountRelationshipEvent>(json)));

    // ---------- Coalescing ----------

    [Fact]
    public void Envelopes_are_merged_in_order_with_the_newest_time_and_nested_profile_fields()
    {
        var merged = SalesforceBatchUpsertService.Coalesce(
        [
            Row(1, """{ "meta": { "time": "2026-10-14T01:00:00Z" }, "account": { "externalId": "ACC-1", "Name": "Acme", "Rating": "Hot", "profile": { "Tier__c": "Gold" } } }"""),
            Row(2, """{ "meta": { "time": "2026-10-14T02:00:00Z" }, "account": { "externalId": "ACC-1", "Rating": null, "profile": { "Score__c": 9 } } }""")
        ]);

        Assert.Equal("2026-10-14T02:00:00Z", (string?)merged["meta"]!["time"]);
        var unit = AccountSyncService.Map("ACC-1", merged.Deserialize<AccountEvent>(SalesforceBatchUpsertService.EventJson)!);
        Assert.Equal(new Dictionary<string, object?> { ["Name"] = "Acme", ["Rating"] = null }, unit.Records[0].Fields);
        Assert.Equal(new Dictionary<string, object?> { ["Tier__c"] = "Gold", ["Score__c"] = 9m }, unit.Records[1].Fields);
    }

    // ---------- What the consumer admits ----------

    [Theory]
    [InlineData("""{ "meta": { "time": "2026-10-14T01:02:03.456Z" }, "account": { "externalId": "ACC-1" } }""", "account", "ACC-1", 456)]
    [InlineData("""{ "accountRelationship": { "externalId": "REL-1" }, "meta": { "time": "2026-10-14T03:02:03+02:00" } }""", "accountRelationship", "REL-1", 0)]
    public void Valid_event_yields_its_type_key_and_time(string json, string type, string key, int millisecond)
    {
        Assert.True(InboxEventEnvelope.TryRead(json, Types, out var header, out var problem), problem);
        Assert.Equal((type, key), (header!.EntityType, header.Key));
        Assert.Equal(new DateTimeOffset(2026, 10, 14, 1, 2, 3, millisecond, TimeSpan.Zero), header.Time); // the same instant
    }

    [Theory]
    [InlineData("not json", "")]
    [InlineData("[1, 2]", "not a JSON object")]
    [InlineData("""{ "meta": { "time": "2026-10-14T01:02:03Z" } }""", "exactly one entity object")]
    [InlineData("""{ "meta": { "time": "2026-10-14T01:02:03Z" }, "account": { "externalId": "A" }, "accountRelationship": { "externalId": "R" } }""", "exactly one entity object")]
    [InlineData("""{ "meta": { "time": "2026-10-14T01:02:03Z" }, "contact": { "externalId": "C-1" } }""", "No sync handles entity type \"contact\"")]
    [InlineData("""{ "meta": { "time": "2026-10-14T01:02:03Z" }, "account": "ACC-1" }""", "is not an object")]
    [InlineData("""{ "meta": { "time": "2026-10-14T01:02:03Z" }, "account": { "Name": "Acme" } }""", "account.externalId")]
    [InlineData("""{ "meta": { "time": "2026-10-14T01:02:03Z" }, "account": { "externalId": " " } }""", "account.externalId")]
    [InlineData("""{ "meta": { "time": "2026-10-14T01:02:03Z" }, "account": { "externalId": 17 } }""", "account.externalId")]
    [InlineData("""{ "account": { "externalId": "ACC-1" } }""", "meta.time")]
    [InlineData("""{ "meta": { "time": "2026-10-14T01:02:03" }, "account": { "externalId": "ACC-1" } }""", "meta.time")]
    [InlineData("""{ "meta": { "time": "2026-10-14 01:02:03Z" }, "account": { "externalId": "ACC-1" } }""", "meta.time")]
    [InlineData("""{ "meta": { "time": "now" }, "account": { "externalId": "ACC-1" } }""", "meta.time")]
    [InlineData("""{ "meta": { "time": "2026-13-14T01:02:03Z" }, "account": { "externalId": "ACC-1" } }""", "meta.time")]
    [InlineData("""{ "meta": { "time": 1791939723456 }, "account": { "externalId": "ACC-1" } }""", "meta.time")]
    [InlineData("""{ "meta": { "time": "2026-10-14T01:02:03Z" }, "account": { "externalId": "A" }, "account": { "externalId": "B" } }""", "")]
    public void Invalid_event_is_rejected_with_the_reason(string json, string reason)
    {
        Assert.False(InboxEventEnvelope.TryRead(json, Types, out var header, out var problem));
        Assert.Null(header);
        Assert.Contains(reason, problem);
    }

    private static TEvent Read<TEvent>(string json) => JsonSerializer.Deserialize<TEvent>(json, SalesforceBatchUpsertService.EventJson)!;

    private static InboxEvent Row(long id, string payload) => new(id, "ACC-1", id, payload, 1);
}
