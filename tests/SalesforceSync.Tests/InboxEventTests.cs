using System.Text.Json;
using System.Text.Json.Nodes;
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

    [Theory, AutoData]
    public void Account_fields_are_everything_but_externalId_and_profile_and_the_profile_defaults_to_the_account_id(
        string key, string name, int score)
    {
        var unit = AccountSyncService.Map(key, Read<AccountEvent>(new JsonObject
        {
            ["meta"] = new JsonObject { ["time"] = "2026-10-14T01:02:03.456Z" },
            ["account"] = new JsonObject
            {
                ["externalId"] = key, ["Name"] = name, ["Rating"] = null,
                ["profile"] = new JsonObject { ["Tier__c"] = "Gold", ["Score__c"] = score }
            }
        }));

        var (account, profile) = (unit.Records[0], unit.Records[1]);
        (account.SObject, account.ExternalId).Should().Be(("Account", key));
        account.Fields.Should().BeEquivalentTo(new Dictionary<string, object?> { ["Name"] = name, ["Rating"] = null });
        (profile.SObject, profile.ExternalId).Should().Be(("Profile__c", key));
        profile.Fields.Should().BeEquivalentTo(new Dictionary<string, object?> { ["Tier__c"] = "Gold", ["Score__c"] = (decimal)score });
        profile.Lookups.Should().ContainSingle()
            .Which.Should().Be(new SObjectLookup("Account__c", "Account", "External_Id__c", key));
    }

    [Theory, AutoData]
    public void Profile_externalId_is_used_when_given(string key, string profileKey)
    {
        var unit = AccountSyncService.Map(key, Read<AccountEvent>(new JsonObject
        {
            ["account"] = new JsonObject
            {
                ["externalId"] = key,
                ["profile"] = new JsonObject { ["externalId"] = profileKey, ["Tier__c"] = "Gold" }
            }
        }));

        unit.Records[1].ExternalId.Should().Be(profileKey);
        unit.Records[1].Fields.Keys.Should().Equal("Tier__c");
    }

    [Theory, AutoData]
    public void Account_without_a_profile_writes_only_the_Account(string key, string name)
    {
        var unit = AccountSyncService.Map(key, Read<AccountEvent>(new JsonObject
        {
            ["meta"] = new JsonObject { ["time"] = "2026-10-14T01:02:03.456Z" },
            ["account"] = new JsonObject { ["externalId"] = key, ["Name"] = name }
        }));

        unit.Records.Should().ContainSingle().Which.SObject.Should().Be("Account");
    }

    [Theory, AutoData]
    public void Relationship_fields_are_everything_but_the_ids(string key, string account, string related)
    {
        var unit = AccountRelationshipSyncService.Map(key, Read<AccountRelationshipEvent>(new JsonObject
        {
            ["meta"] = new JsonObject { ["time"] = "2026-10-14T01:02:03.456Z" },
            ["accountRelationship"] = new JsonObject
            {
                ["externalId"] = key, ["accountExternalId"] = account, ["relatedAccountExternalId"] = related,
                ["Type__c"] = "Parent"
            }
        }));

        var rel = unit.Records.Should().ContainSingle().Subject;
        (rel.SObject, rel.ExternalId).Should().Be(("Account_Relationship__c", key));
        rel.Fields.Keys.Should().Equal("Type__c");
        rel.Lookups!.Select(l => l.ExternalId).Should().Equal(account, related);
    }

    [Theory]
    [InlineData("""{ "accountRelationship": { "externalId": "REL-1", "accountExternalId": "ACC-1" } }""")]
    [InlineData("""{ "accountRelationship": { "externalId": "REL-1", "accountExternalId": "ACC-1", "relatedAccountExternalId": "" } }""")]
    [InlineData("""{ "meta": { "time": "2026-10-14T01:02:03.456Z" } }""")]
    public void Relationship_without_both_accounts_cannot_be_mapped(string json)
    {
        var map = () => AccountRelationshipSyncService.Map("REL-1", Read<AccountRelationshipEvent>(json));

        map.Should().ThrowExactly<ArgumentException>();
    }

    // ---------- Coalescing ----------

    [Fact]
    public void Envelopes_are_merged_in_order_with_the_newest_time_and_nested_profile_fields()
    {
        var merged = SalesforceBatchUpsertService.Coalesce(
        [
            Row(1, """{ "meta": { "time": "2026-10-14T01:00:00Z" }, "account": { "externalId": "ACC-1", "Name": "Acme", "Rating": "Hot", "profile": { "Tier__c": "Gold" } } }"""),
            Row(2, """{ "meta": { "time": "2026-10-14T02:00:00Z" }, "account": { "externalId": "ACC-1", "Rating": null, "profile": { "Score__c": 9 } } }""")
        ]);

        ((string?)merged["meta"]!["time"]).Should().Be("2026-10-14T02:00:00Z");
        var unit = AccountSyncService.Map("ACC-1", merged.Deserialize<AccountEvent>(SalesforceBatchUpsertService.EventJson)!);
        unit.Records[0].Fields.Should().BeEquivalentTo(new Dictionary<string, object?> { ["Name"] = "Acme", ["Rating"] = null },
            "a later null clears the field, an absent field keeps the earlier value");
        unit.Records[1].Fields.Should().BeEquivalentTo(new Dictionary<string, object?> { ["Tier__c"] = "Gold", ["Score__c"] = 9m },
            "nested profile fields are merged too");
    }

    // ---------- What the consumer admits ----------

    [Theory]
    [InlineData("""{ "meta": { "time": "2026-10-14T01:02:03.456Z" }, "account": { "externalId": "ACC-1" } }""", "account", "ACC-1", 456)]
    [InlineData("""{ "accountRelationship": { "externalId": "REL-1" }, "meta": { "time": "2026-10-14T03:02:03+02:00" } }""", "accountRelationship", "REL-1", 0)]
    public void Valid_event_yields_its_type_key_and_time(string json, string type, string key, int millisecond)
    {
        InboxEventEnvelope.TryRead(json, Types, out var header, out var problem).Should().BeTrue("the event is valid, but: {0}", problem);
        (header!.EntityType, header.Key).Should().Be((type, key));
        header.Time.Should().Be(new DateTimeOffset(2026, 10, 14, 1, 2, 3, millisecond, TimeSpan.Zero), "it is the same instant");
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
        InboxEventEnvelope.TryRead(json, Types, out var header, out var problem).Should().BeFalse();
        header.Should().BeNull();
        problem.Should().NotBeNullOrEmpty();
        if (reason.Length > 0) problem.Should().Contain(reason);
    }

    private static TEvent Read<TEvent>(string json) => JsonSerializer.Deserialize<TEvent>(json, SalesforceBatchUpsertService.EventJson)!;

    private static TEvent Read<TEvent>(JsonObject json) => json.Deserialize<TEvent>(SalesforceBatchUpsertService.EventJson)!;

    private static InboxEvent Row(long id, string payload) => new(id, "ACC-1", id, payload, 1);
}
