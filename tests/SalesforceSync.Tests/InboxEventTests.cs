using System.Text.Json;
using System.Text.Json.Nodes;
using SalesforceSync.Inbox;
using SalesforceSync.Sync;

namespace SalesforceSync.Tests;

/// <summary>
/// The raw event format: how an envelope (camelCase JSON, full record) maps to Salesforce records, how an entity's
/// pending events are coalesced, and what the Kafka consumer admits into the inbox.
/// </summary>
public sealed class InboxEventTests
{
    private static readonly UpsertStrategy<AccountEvent> Accounts =
        AccountSync.Strategy;

    private static readonly UpsertStrategy<AccountRelationshipEvent> Relationships =
        AccountRelationshipSync.Strategy;

    // ---------- Mapping ----------

    [Theory, AutoData]
    public void Account_event_maps_to_an_Account_and_a_Profile_that_defaults_to_the_account_id(string key, string name, int score)
    {
        var unit = Accounts.Map(key, Read<AccountEvent>(new JsonObject
        {
            ["meta"] = new JsonObject { ["time"] = "2026-10-14T01:02:03.456Z", ["type"] = "updated" },
            ["account"] = new JsonObject
            {
                ["externalId"] = key,
                ["name"] = name,
                ["rating"] = null,
                ["tags"] = new JsonArray("B2B", "Retail"),
                ["profile"] = new JsonObject { ["tier"] = "Gold", ["score"] = score }
            }
        }));

        var (account, profile) = (unit.Records[0], unit.Records[1]);
        (account.SObject, account.ExternalIdField, account.ExternalId).Should().Be(("Account", "ExternalId__c", key));
        account.Fields.Should().Contain("Name", name).And.Contain("Tags__c", "B2B;Retail");
        account.Fields.Should().Contain("Rating", null).And.Contain("Description", null,
            "the event is the full record: null and absent both mean empty, and are sent on to the diff");
        (profile.SObject, profile.ExternalId).Should().Be(("Profile__c", key));
        profile.Fields.Should().Contain("Tier__c", "Gold").And.Contain("Score__c", (decimal)score).And.Contain("Notes__c", null);
        profile.Fields.Should().Contain("AccountId__c", key, "the Account's key, as text");
        profile.ForeignKeys.Should().ContainSingle()
            .Which.Should().Be(new SObjectLookup("AccountId__c", "Account", "ExternalId__c", key));
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
        unit.Records[1].Fields["AccountId__c"].Should().Be(key, "it still belongs to the event's account");
    }

    [Theory, AutoData]
    public void Account_without_a_profile_writes_only_the_Account(string key, string name)
    {
        var unit = Accounts.Map(key, Read<AccountEvent>(new JsonObject
        {
            ["meta"] = new JsonObject { ["time"] = "2026-10-14T01:02:03.456Z", ["type"] = "updated" },
            ["account"] = new JsonObject { ["externalId"] = key, ["name"] = name }
        }));

        unit.Records.Should().ContainSingle().Which.SObject.Should().Be("Account");
    }

    [Theory, AutoData]
    public void Relationship_event_maps_to_a_relationship_looking_up_both_Accounts(string key, string account, string related)
    {
        var unit = Relationships.Map(key, Read<AccountRelationshipEvent>(new JsonObject
        {
            ["meta"] = new JsonObject { ["time"] = "2026-10-14T01:02:03.456Z", ["type"] = "updated" },
            ["accountRelationship"] = new JsonObject
            {
                ["externalId"] = key,
                ["accountExternalId"] = account,
                ["relatedAccountExternalId"] = related,
                ["type"] = "Parent",
                ["startDate"] = "2026-01-31"
            }
        }));

        var rel = unit.Records.Should().ContainSingle().Subject;
        (rel.SObject, rel.ExternalId).Should().Be(("Account_Relationship__c", key));
        rel.Fields.Should().BeEquivalentTo(new Dictionary<string, object?>
        {
            ["Name"] = null,
            ["AccountId__c"] = account,
            ["RelatedAccountId__c"] = related,
            ["Type__c"] = "Parent",
            ["Start_Date__c"] = new DateOnly(2026, 1, 31),
            ["Active__c"] = null
        });
        rel.ForeignKeys.Should().Equal(
            new SObjectLookup("AccountId__c", "Account", "ExternalId__c", account),
            new SObjectLookup("RelatedAccountId__c", "Account", "ExternalId__c", related));
    }

    [Theory]
    [InlineData(/*lang=json,strict*/ """{ "accountRelationship": { "externalId": "REL-1", "accountExternalId": "ACC-1" } }""")]
    [InlineData(/*lang=json,strict*/ """{ "accountRelationship": { "externalId": "REL-1", "accountExternalId": "ACC-1", "relatedAccountExternalId": "" } }""")]
    [InlineData(/*lang=json,strict*/ """{ "meta": { "time": "2026-10-14T01:02:03.456Z", "type": "updated" } }""")]
    public void Relationship_without_both_accounts_cannot_be_mapped(string json)
    {
        var map = () => Relationships.Map("REL-1", Read<AccountRelationshipEvent>(json));

        map.Should().ThrowExactly<ArgumentException>();
    }

    [Theory, AutoData]
    public void Contact_event_maps_to_a_Contact_looking_up_its_Account_and_reads_both(string key, string account, string lastName)
    {
        var unit = ContactSync.Strategy.Map(key, Read<ContactEvent>(new JsonObject
        {
            ["meta"] = new JsonObject { ["time"] = "2026-10-14T01:02:03.456Z", ["type"] = "inserted" },
            ["contact"] = new JsonObject { ["externalId"] = key, ["accountExternalId"] = account, ["lastName"] = lastName }
        }));

        var contact = unit.Records.Should().ContainSingle().Subject;
        (contact.SObject, contact.ExternalIdField, contact.ExternalId, contact.RecordType).Should()
            .Be(("Contact", "ExternalId__c", key, "Standard"));
        contact.Fields.Should().Contain("LastName", lastName).And.Contain("Email", null).And.Contain("AccountId__c", account);
        contact.ForeignKeys.Should().Equal(new SObjectLookup("AccountId__c", "Account", "ExternalId__c", account));
        ContactSync.Strategy.Queries([unit]).Select(q => (q.SObject, q.WhereField, q.Values.Single())).Should()
            .Equal(("Contact", "ExternalId__c", key), ("Account", "ExternalId__c", account));
    }

    [Theory]
    [InlineData(/*lang=json,strict*/ """{ "contact": { "externalId": "CON-1", "lastName": "Doe" } }""")]
    [InlineData(/*lang=json,strict*/ """{ "contact": { "externalId": "CON-1", "accountExternalId": " ", "lastName": "Doe" } }""")]
    public void Contact_without_its_account_cannot_be_mapped(string json)
    {
        var map = () => ContactSync.Strategy.Map("CON-1", Read<ContactEvent>(json));

        map.Should().ThrowExactly<ArgumentException>();
    }

    [Fact]
    public void CamelCase_is_the_contract_but_a_PascalCase_body_binds_to_the_same_sObject()
    {
        const string camel = /*lang=json,strict*/ """
            { "meta": { "time": "2026-10-14T01:02:03Z", "type": "updated" }, "account": { "externalId": "ACC-1", "name": "Acme", "isActive": true,
              "annualRevenue": 12.5, "customerSince": "2020-01-15", "profile": { "tier": "Gold", "discountPercent": 5 } } }
            """;
        const string pascal = /*lang=json,strict*/ """
            { "meta": { "time": "2026-10-14T01:02:03Z", "type": "updated" }, "account": { "ExternalId": "ACC-1", "Name": "Acme", "IsActive": true,
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
        var read = () => Read<AccountEvent>(/*lang=json,strict*/ """{ "account": { "externalId": "ACC-1", "name": "Acme", "numberOfEmployees": "lots" } }""");

        read.Should().Throw<JsonException>("the engine dead-letters an event it can't deserialize (INVALID_EVENT)");
    }

    // ---------- Coalescing: an entity's claimed rows -> one write ----------

    [Fact]
    public void Claimed_events_coalesce_to_the_newest_event_time_whatever_order_they_arrived_in()
    {
        var batch = Prepare(
            Row(7, Account("ACC-1", "newest, but stored first", time: "2026-10-14T03:00:00Z")),
            Row(8, Account("ACC-1", "older", time: "2026-10-14T01:00:00Z")),
            Row(9, Account("ACC-1", "oldest, stored last", time: "2026-10-14T00:00:00Z")));

        var work = batch.Work.Should().ContainSingle().Subject;
        work.EventId.Should().Be(7);
        work.Time.Should().Be(Micros("2026-10-14T03:00:00Z"));
        work.Unit.Records[0].Fields.Should().Contain("Name", "newest, but stored first");
        batch.SupersededIds.Should().BeEquivalentTo([8L, 9L]);
        batch.Invalid.Should().BeEmpty();
    }

    [Fact]
    public void Of_two_events_with_the_same_time_the_one_stored_later_wins()
    {
        var batch = Prepare(
            Row(4, Account("ACC-1", "second", time: "2026-10-14T01:00:00+00:00")),
            Row(3, Account("ACC-1", "first", time: "2026-10-14T03:00:00+02:00"))); // the same instant

        batch.Work.Should().ContainSingle().Which.EventId.Should().Be(4);
        batch.SupersededIds.Should().Equal(3L);
    }

    [Fact]
    public void The_newest_full_record_replaces_older_events_rather_than_merging_with_them()
    {
        var batch = Prepare(
            Row(1, /*lang=json,strict*/ """{ "meta": { "time": "2026-10-14T01:00:00Z", "type": "updated" }, "account": { "externalId": "ACC-1", "name": "Acme", "rating": "Hot", "profile": { "tier": "Gold" } } }"""),
            Row(2, /*lang=json,strict*/ """{ "meta": { "time": "2026-10-14T02:00:00Z", "type": "updated" }, "account": { "externalId": "ACC-1", "name": "Acme Ltd", "profile": { "score": 9 } } }"""));

        var unit = batch.Work.Should().ContainSingle().Subject.Unit;
        unit.Records[0].Fields.Should().Contain("Name", "Acme Ltd").And.Contain("Rating", null, "the newest record has no rating any more");
        unit.Records[1].Fields.Should().Contain("Score__c", 9m).And.Contain("Tier__c", null, "nor a tier");
    }

    [Theory]
    [InlineData("2026-10-14T01:00:00Z", "the same time: a redelivery")]
    [InlineData("2026-10-14T00:59:59.999999Z", "an older event that arrived late")]
    public void An_event_not_newer_than_what_is_synced_is_superseded_without_a_write(string time, string because)
    {
        var synced = new Dictionary<string, long> { ["ACC-1"] = Micros("2026-10-14T01:00:00Z") };

        var batch = Prepare(synced, Row(5, Account("ACC-1", "late", time)), Row(6, Account("ACC-2", "other entity", time)));

        batch.SupersededIds.Should().Equal([5L], because);
        batch.Work.Should().ContainSingle().Which.EntityId.Should().Be("ACC-2", "watermarks are per entity");
    }

    [Fact]
    public void An_event_newer_than_what_is_synced_is_written_even_by_a_microsecond()
    {
        var synced = new Dictionary<string, long> { ["ACC-1"] = Micros("2026-10-14T01:00:00Z") };

        Prepare(synced, Row(5, Account("ACC-1", "newer", "2026-10-14T01:00:00.000001Z"))).Work.Should().ContainSingle();
    }

    [Fact]
    public void A_then_B_then_A_ends_at_A()
    {
        // The last event equals the first; only its time tells them apart, and it is the newest
        var batch = Prepare(
            Row(1, Account("ACC-1", "A", "2026-10-14T01:00:00Z")),
            Row(2, Account("ACC-1", "B", "2026-10-14T02:00:00Z")),
            Row(3, Account("ACC-1", "A", "2026-10-14T03:00:00Z")));

        var work = batch.Work.Should().ContainSingle().Subject;
        work.EventId.Should().Be(3);
        work.Unit.Records[0].Fields.Should().Contain("Name", "A");
    }

    [Fact]
    public void An_unreadable_event_is_invalid_on_its_own_and_the_entity_still_syncs()
    {
        var batch = Prepare(
            Row(1, Account("ACC-1", "good", "2026-10-14T01:00:00Z")),
            Row(2, "not json"),
            Row(3, /*lang=json,strict*/ """{ "meta": { "time": "2026-10-14T05:00:00", "type": "updated" }, "account": { "externalId": "ACC-1", "name": "no offset" } }"""),
            Row(4, /*lang=json,strict*/ """{ "meta": { "time": "2026-10-14T05:00:00Z", "type": "updated" }, "account": { "externalId": "ACC-1", "name": "x", "numberOfEmployees": "lots" } }"""));

        batch.Invalid.Select(i => i.Id).Should().Equal(2L, 3L, 4L);
        batch.Work.Should().ContainSingle().Which.EventId.Should().Be(1);
        batch.SupersededIds.Should().BeEmpty();
    }

    [Fact]
    public void An_event_stored_under_another_entity_than_it_is_about_is_never_written()
    {
        // The lease is on the row's entity_id; writing ACC-2 under ACC-1's lease would bypass ACC-2's own
        var batch = Prepare(new InboxEvent(1, "ACC-1", Account("ACC-2", "mismatch", "2026-10-14T01:00:00Z"), 1));

        batch.Work.Should().BeEmpty();
        batch.Invalid.Should().ContainSingle().Which.Error.Should().Contain("ACC-2").And.Contain("ACC-1");
    }

    [Fact]
    public void A_newest_event_that_cannot_be_mapped_is_invalid_and_the_older_ones_stay_superseded()
    {
        var batch = PreparedBatch.Prepare(Relationships,
        [
            new InboxEvent(1, "REL-1", /*lang=json,strict*/ """{ "meta": { "time": "2026-10-14T01:00:00Z", "type": "updated" }, "accountRelationship": { "externalId": "REL-1", "accountExternalId": "A", "relatedAccountExternalId": "B" } }""", 1),
            new InboxEvent(2, "REL-1", /*lang=json,strict*/ """{ "meta": { "time": "2026-10-14T02:00:00Z", "type": "updated" }, "accountRelationship": { "externalId": "REL-1", "accountExternalId": "A" } }""", 1)
        ], new Dictionary<string, long>());

        batch.Invalid.Should().ContainSingle().Which.Id.Should().Be(2);
        batch.SupersededIds.Should().Equal([1L], "the newest event is the entity's state; an older one is not written in its place");
        batch.Work.Should().BeEmpty();
    }

    // ---------- What the consumer admits (the same typed read the engine uses) ----------

    [Theory]
    [InlineData(/*lang=json,strict*/ """{ "meta": { "time": "2026-10-14T01:02:03.456Z", "type": "updated" }, "account": { "externalId": "ACC-1" } }""", "ACC-1", 456)]
    [InlineData(/*lang=json,strict*/ """{ "account": { "externalId": "ACC-2" }, "meta": { "time": "2026-10-14T03:02:03+02:00", "type": "updated" } }""", "ACC-2", 0)]
    [InlineData(/*lang=json,strict*/ """{ "Meta": { "Time": "2026-10-14T01:02:03Z", "Type": "INSERTED" }, "Account": { "ExternalId": "ACC-3" }, "somethingElse": 1 }""", "ACC-3", 0)]
    public void Valid_event_yields_its_entity_id_and_time(string json, string entityId, int millisecond)
    {
        Accounts.TryRead(json, out var read, out var problem).Should().BeTrue("the event is valid, but: {0}", problem);
        read!.EntityId.Should().Be(entityId);
        read.Time.Should().Be(new DateTimeOffset(2026, 10, 14, 1, 2, 3, millisecond, TimeSpan.Zero), "it is the same instant");
    }

    [Theory]
    [InlineData("inserted")]
    [InlineData("updated")]
    [InlineData("Inserted")]
    [InlineData("UPDATED")]
    public void Event_type_is_inserted_or_updated_in_any_case(string type)
    {
        var json = $$"""{ "meta": { "time": "2026-10-14T01:00:00Z", "type": "{{type}}" }, "account": { "externalId": "ACC-1" } }""";

        Accounts.TryRead(json, out _, out var problem).Should().BeTrue("the type is valid, but: {0}", problem);
    }

    [Theory]
    [InlineData("not json", "Not a valid account event")]
    [InlineData("[1, 2]", "Not a valid account event")]
    [InlineData("null", "null")]
    [InlineData(/*lang=json,strict*/ """{ "meta": { "time": "2026-10-14T01:02:03Z", "type": "updated" } }""", "entity object")]
    [InlineData(/*lang=json,strict*/ """{ "meta": { "time": "2026-10-14T01:02:03Z", "type": "updated" }, "accountRelationship": { "externalId": "R" } }""", "entity object")]
    [InlineData(/*lang=json,strict*/ """{ "meta": { "time": "2026-10-14T01:02:03Z", "type": "updated" }, "account": "ACC-1" }""", "Not a valid account event")]
    [InlineData(/*lang=json,strict*/ """{ "meta": { "time": "2026-10-14T01:02:03Z", "type": "updated" }, "account": { "name": "Acme" } }""", "non-empty id")]
    [InlineData(/*lang=json,strict*/ """{ "meta": { "time": "2026-10-14T01:02:03Z", "type": "updated" }, "account": { "externalId": " " } }""", "non-empty id")]
    [InlineData(/*lang=json,strict*/ """{ "meta": { "time": "2026-10-14T01:02:03Z", "type": "updated" }, "account": { "externalId": 17 } }""", "Not a valid account event")]
    [InlineData(/*lang=json,strict*/ """{ "account": { "externalId": "ACC-1" } }""", "event time")]
    [InlineData(/*lang=json,strict*/ """{ "meta": { "time": "2026-10-14T01:02:03", "type": "updated" }, "account": { "externalId": "ACC-1" } }""", "event time")]
    [InlineData(/*lang=json,strict*/ """{ "meta": { "time": "2026-10-14 01:02:03Z", "type": "updated" }, "account": { "externalId": "ACC-1" } }""", "event time")]
    [InlineData(/*lang=json,strict*/ """{ "meta": { "time": "now", "type": "updated" }, "account": { "externalId": "ACC-1" } }""", "event time")]
    [InlineData(/*lang=json,strict*/ """{ "meta": { "time": "2026-13-14T01:02:03Z", "type": "updated" }, "account": { "externalId": "ACC-1" } }""", "event time")]
    [InlineData(/*lang=json,strict*/ """{ "meta": { "time": 1791939723456, "type": "updated" }, "account": { "externalId": "ACC-1" } }""", "Not a valid account event")]
    [InlineData(/*lang=json,strict*/ """{ "meta": { "time": "2026-10-14T01:02:03Z" }, "account": { "externalId": "ACC-1" } }""", "event type")]
    [InlineData(/*lang=json,strict*/ """{ "meta": { "time": "2026-10-14T01:02:03Z", "type": null }, "account": { "externalId": "ACC-1" } }""", "event type")]
    [InlineData(/*lang=json,strict*/ """{ "meta": { "time": "2026-10-14T01:02:03Z", "type": "deleted" }, "account": { "externalId": "ACC-1" } }""", "event type")]
    [InlineData(/*lang=json,strict*/ """{ "meta": { "time": "2026-10-14T01:02:03Z", "type": "upsert" }, "account": { "externalId": "ACC-1" } }""", "event type")]
    [InlineData(/*lang=json,strict*/ """{ "meta": { "time": "2026-10-14T01:02:03Z", "type": 1 }, "account": { "externalId": "ACC-1" } }""", "Not a valid account event")]
    public void Invalid_event_is_rejected_with_the_reason(string json, string reason)
    {
        Accounts.TryRead(json, out var read, out var problem).Should().BeFalse();
        read.Should().BeNull();
        problem.Should().Contain(reason);
    }

    [Fact]
    public void Each_entity_type_reads_only_its_own_events()
    {
        const string account = /*lang=json,strict*/ """{ "meta": { "time": "2026-10-14T01:02:03Z", "type": "updated" }, "account": { "externalId": "ACC-1" } }""";

        Relationships.TryRead(account, out _, out var problem).Should().BeFalse("an account event on the relationship topic is skipped at ingest");
        problem.Should().Contain("accountRelationship");
    }

    [Theory]
    [InlineData("sf_account_inbox", true)]
    [InlineData("sf_x1_inbox", true)]
    [InlineData("sf_inbox; drop table x", false)]
    [InlineData("Sf_Inbox", false)]
    [InlineData("\"sf_inbox\"", false)]
    [InlineData("public.sf_inbox", false)]
    [InlineData("", false)]
    public void Inbox_table_names_must_be_plain_identifiers(string table, bool valid)
    {
        var use = () => InboxSql.For(table);

        if (valid)
        {
            use.Should().NotThrow().Which.LockHeads.Should().Contain($"from {table} ");
        }
        else
        {
            use.Should().Throw<ArgumentException>();
        }
    }

    [Fact]
    public void Schema_script_has_one_inbox_per_table_and_no_placeholder_left()
    {
        var script = InboxSchema.Script(["sf_account_inbox", "sf_account_relationship_inbox"]);

        script.Should().Contain("create table if not exists sf_account_inbox (")
            .And.Contain("create table if not exists sf_account_relationship_inbox (")
            .And.Contain("create index if not exists sf_account_inbox_ready on sf_account_inbox")
            .And.NotContain("{inbox}");
        script.Should().NotContain("->", "no statement reads inside the payload");
        var bad = () => InboxSchema.Script(["sf_inbox; drop table sf_rate_limit"]);
        bad.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void No_inbox_statement_reads_inside_the_payload()
    {
        var sql = InboxSql.For("sf_account_inbox");
        string[] claimAndOutcomes = [sql.LockHeads, sql.ClaimEntities, sql.ReleaseLostClaims, sql.SyncedTimes];

        claimAndOutcomes.Should().AllSatisfy(statement => statement.Should().NotContain("->").And.NotContain("sf_event_").And.NotContain("externalId"));
    }

    private static string Account(string id, string name, string time)
    {
        return new JsonObject
        {
            ["meta"] = new JsonObject { ["time"] = time, ["type"] = "updated" },
            ["account"] = new JsonObject { ["externalId"] = id, ["name"] = name }
        }.ToJsonString();
    }

    private static PreparedBatch Prepare(params InboxEvent[] rows)
    {
        return PreparedBatch.Prepare(Accounts, rows, new Dictionary<string, long>());
    }

    private static PreparedBatch Prepare(Dictionary<string, long> synced, params InboxEvent[] rows)
    {
        return PreparedBatch.Prepare(Accounts, rows, synced);
    }

    private static long Micros(string time)
    {
        return PreparedBatch.ToMicroseconds(DateTimeOffset.Parse(time, System.Globalization.CultureInfo.InvariantCulture));
    }

    /// <summary>A claimed row of the entity its payload names.</summary>
    private static InboxEvent Row(long id, string payload)
    {
        return new(id, Accounts.TryRead(payload, out var read, out _) ? read.EntityId : "ACC-1", payload, 1);
    }

    private static TEvent Read<TEvent>(string json)
    {
        return JsonSerializer.Deserialize<TEvent>(json, SalesforceBatchUpsertService.EventJson)!;
    }

    private static TEvent Read<TEvent>(JsonObject json)
    {
        return json.Deserialize<TEvent>(SalesforceBatchUpsertService.EventJson)!;
    }
}