using System.Text.Json;
using System.Text.Json.Nodes;
using SalesforceSync.Inbox;
using SalesforceSync.Inbox.Payloads;
using SalesforceSync.Sync;
using SalesforceSync.Sync.Mapping;

namespace SalesforceSync.Tests;

/// <summary>
/// The raw event format: how an envelope (camelCase JSON, full record) maps to Salesforce records, how an entity's
/// pending events are coalesced, and what the Kafka consumer admits into the inbox.
/// </summary>
public sealed class InboxEventTests
{
    private static readonly HashSet<string> Types = [AccountSyncService.Type, AccountRelationshipSyncService.Type];

    private static readonly UpsertStrategy<AccountEvent> Accounts =
        AccountSyncService.CreateStrategy(new AccountMapper(), new ProfileMapper());

    private static readonly UpsertStrategy<AccountRelationshipEvent> Relationships =
        AccountRelationshipSyncService.CreateStrategy(new AccountRelationshipMapper());

    // ---------- Mapping ----------

    [Theory, AutoData]
    public void Account_event_maps_to_an_Account_and_a_Profile_that_defaults_to_the_account_id(string key, string name, int score)
    {
        var unit = Accounts.Map(key, Read<AccountEvent>(new JsonObject
        {
            ["meta"] = new JsonObject { ["time"] = "2026-10-14T01:02:03.456Z" },
            ["account"] = new JsonObject
            {
                ["externalId"] = key, ["name"] = name, ["rating"] = null, ["tags"] = new JsonArray("B2B", "Retail"),
                ["profile"] = new JsonObject { ["tier"] = "Gold", ["score"] = score }
            }
        }));

        var (account, profile) = (unit.Records[0], unit.Records[1]);
        (account.SObject, account.ExternalIdField, account.ExternalId).Should().Be(("Account", "External_Id__c", key));
        account.Fields.Should().Contain("Name", name).And.Contain("Tags__c", "B2B;Retail");
        account.Fields.Should().Contain("Rating", null).And.Contain("Description", null,
            "the event is the full record: null and absent both mean empty, and are sent on to the diff");
        (profile.SObject, profile.ExternalId).Should().Be(("Profile__c", key));
        profile.Fields.Should().Contain("Tier__c", "Gold").And.Contain("Score__c", (decimal)score).And.Contain("Notes__c", null);
        profile.Lookups.Should().ContainSingle()
            .Which.Should().Be(new SObjectLookup("Account__c", "Account", "External_Id__c", key));
    }

    [Theory, AutoData]
    public void Profile_externalId_is_used_when_given(string key, string profileKey)
    {
        var unit = Accounts.Map(key, Read<AccountEvent>(new JsonObject
        {
            ["account"] = new JsonObject
            {
                ["externalId"] = key,
                ["profile"] = new JsonObject { ["externalId"] = profileKey, ["tier"] = "Gold" }
            }
        }));

        unit.Records[1].ExternalId.Should().Be(profileKey);
        unit.Records[1].Lookups.Should().ContainSingle().Which.ExternalId.Should().Be(key, "it still belongs to the event's account");
    }

    [Theory, AutoData]
    public void Account_without_a_profile_writes_only_the_Account(string key, string name)
    {
        var unit = Accounts.Map(key, Read<AccountEvent>(new JsonObject
        {
            ["meta"] = new JsonObject { ["time"] = "2026-10-14T01:02:03.456Z" },
            ["account"] = new JsonObject { ["externalId"] = key, ["name"] = name }
        }));

        unit.Records.Should().ContainSingle().Which.SObject.Should().Be("Account");
    }

    [Theory, AutoData]
    public void Relationship_event_maps_to_a_relationship_looking_up_both_Accounts(string key, string account, string related)
    {
        var unit = Relationships.Map(key, Read<AccountRelationshipEvent>(new JsonObject
        {
            ["meta"] = new JsonObject { ["time"] = "2026-10-14T01:02:03.456Z" },
            ["accountRelationship"] = new JsonObject
            {
                ["externalId"] = key, ["accountExternalId"] = account, ["relatedAccountExternalId"] = related,
                ["type"] = "Parent", ["startDate"] = "2026-01-31"
            }
        }));

        var rel = unit.Records.Should().ContainSingle().Subject;
        (rel.SObject, rel.ExternalId).Should().Be(("Account_Relationship__c", key));
        rel.Fields.Should().BeEquivalentTo(new Dictionary<string, object?>
        {
            ["Name"] = null, ["Type__c"] = "Parent", ["Start_Date__c"] = new DateOnly(2026, 1, 31), ["Active__c"] = null
        });
        rel.Lookups.Should().Equal(
            new SObjectLookup("Account__c", "Account", "External_Id__c", account),
            new SObjectLookup("Related_Account__c", "Account", "External_Id__c", related));
    }

    [Theory]
    [InlineData("""{ "accountRelationship": { "externalId": "REL-1", "accountExternalId": "ACC-1" } }""")]
    [InlineData("""{ "accountRelationship": { "externalId": "REL-1", "accountExternalId": "ACC-1", "relatedAccountExternalId": "" } }""")]
    [InlineData("""{ "meta": { "time": "2026-10-14T01:02:03.456Z" } }""")]
    public void Relationship_without_both_accounts_cannot_be_mapped(string json)
    {
        var map = () => Relationships.Map("REL-1", Read<AccountRelationshipEvent>(json));

        map.Should().ThrowExactly<ArgumentException>();
    }

    [Fact]
    public void CamelCase_is_the_contract_but_a_PascalCase_body_binds_to_the_same_sObject()
    {
        const string camel = """
            { "meta": { "time": "2026-10-14T01:02:03Z" }, "account": { "externalId": "ACC-1", "name": "Acme", "isActive": true,
              "annualRevenue": 12.5, "customerSince": "2020-01-15", "profile": { "tier": "Gold", "discountPercent": 5 } } }
            """;
        const string pascal = """
            { "meta": { "time": "2026-10-14T01:02:03Z" }, "account": { "ExternalId": "ACC-1", "Name": "Acme", "IsActive": true,
              "AnnualRevenue": 12.5, "CustomerSince": "2020-01-15", "Profile": { "Tier": "Gold", "DiscountPercent": 5 } } }
            """;

        var fromCamel = Accounts.Map("ACC-1", Read<AccountEvent>(camel));
        var fromPascal = Accounts.Map("ACC-1", Read<AccountEvent>(pascal));

        fromPascal.Should().BeEquivalentTo(fromCamel);
        fromCamel.Records[0].Fields.Should().Contain("Name", "Acme").And.Contain("Active__c", true)
            .And.Contain("AnnualRevenue", 12.5m).And.Contain("Customer_Since__c", new DateOnly(2020, 1, 15));
        fromCamel.Records[1].Fields.Should().Contain("Tier__c", "Gold").And.Contain("Discount__c", 5m);
    }

    [Fact]
    public void Value_of_the_wrong_type_makes_the_event_unreadable()
    {
        var read = () => Read<AccountEvent>("""{ "account": { "externalId": "ACC-1", "name": "Acme", "numberOfEmployees": "lots" } }""");

        read.Should().Throw<JsonException>("the engine dead-letters an event it can't deserialize (INVALID_EVENT)");
    }

    // ---------- Coalescing ----------

    [Fact]
    public void Pending_events_coalesce_to_the_newest_by_time_then_by_id()
    {
        var events = new[]
        {
            new InboxEvent(Id: 7, "ACC-1", Version: 20, """{ "account": { "externalId": "ACC-1", "name": "newest time, ingested first" } }""", 1),
            new InboxEvent(Id: 5, "ACC-1", Version: 10, """{ "account": { "externalId": "ACC-1", "name": "older" } }""", 1),
            new InboxEvent(Id: 9, "ACC-1", Version: 20, """{ "account": { "externalId": "ACC-1", "name": "newest time, ingested last" } }""", 1),
            new InboxEvent(Id: 12, "ACC-1", Version: 15, """{ "account": { "externalId": "ACC-1", "name": "ingested last, but older" } }""", 1)
        };

        SalesforceBatchUpsertService.Coalesce(events).Id.Should().Be(9, "the newest time wins; of two with the same time, the one ingested later");
    }

    [Fact]
    public void The_newest_full_record_replaces_older_events_rather_than_merging_with_them()
    {
        var newest = SalesforceBatchUpsertService.Coalesce(
        [
            Row(1, """{ "meta": { "time": "2026-10-14T01:00:00Z" }, "account": { "externalId": "ACC-1", "name": "Acme", "rating": "Hot", "profile": { "tier": "Gold" } } }"""),
            Row(2, """{ "meta": { "time": "2026-10-14T02:00:00Z" }, "account": { "externalId": "ACC-1", "name": "Acme Ltd", "profile": { "score": 9 } } }""")
        ]);

        var unit = Accounts.Map("ACC-1", Read<AccountEvent>(newest.Payload));
        unit.Records[0].Fields.Should().Contain("Name", "Acme Ltd").And.Contain("Rating", null, "the newest record has no rating any more");
        unit.Records[1].Fields.Should().Contain("Score__c", 9m).And.Contain("Tier__c", null, "nor a tier");
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
