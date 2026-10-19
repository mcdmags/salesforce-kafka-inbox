using System.Text.Json.Nodes;
using SalesforceSync.Sync;

namespace SalesforceSync.Tests;

/// <summary>
/// The Kafka aggregate split into per-entity events (<see cref="AccountAggregate.TrySplit"/>): only what changed
/// between before and after is stored, a removed child is a delete, and every event is one its type's strategy reads
/// and maps.
/// No database, no Kafka.
/// </summary>
public sealed class AccountAggregateTests
{
    private const string Time = "2026-10-14T01:02:03.456Z";

    [Fact]
    public void An_insert_stores_the_Account_and_every_child_each_naming_the_Account()
    {
        var events = Split(Aggregate(null, Snapshot(contacts: [Contact("C1")], relationships: [Relationship("R1", "ACC-2")])));

        events.Select(e => (e.EntityType, Type(e), Id(e))).Should().Equal(
            ("account", "inserted", "ACC-1"), ("accountRelationship", "inserted", "R1"), ("contact", "inserted", "C1"));
        Payload(events[1])["accountExternalId"]!.GetValue<string>().Should().Be("ACC-1", "the aggregate's Account is the relationship's");
        Payload(events[2])["accountExternalId"]!.GetValue<string>().Should().Be("ACC-1");
        events.Select(e => JsonNode.Parse(e.Json)!["meta"]!["time"]!.GetValue<string>()).Should().AllBe(Time, "every event has the aggregate's time");
        Payload(events[0]).ContainsKey("contacts").Should().BeFalse("the account's event is the Account and its Profile only");
    }

    [Fact]
    public void An_aggregate_where_nothing_changed_stores_nothing()
    {
        var snapshot = Snapshot(contacts: [Contact("C1")], relationships: [Relationship("R1", "ACC-2")]);

        Split(Aggregate(snapshot, snapshot.DeepClone().AsObject())).Should().BeEmpty("no inbox row, no claim, no Salesforce read");
    }

    [Theory]
    [InlineData("added")]
    [InlineData("changed")]
    public void Only_the_child_that_was_added_or_changed_is_stored(string what)
    {
        var before = Snapshot(contacts: what == "added" ? [Contact("C1")] : [Contact("C1"), Contact("C2")]);
        var after = Snapshot(contacts: [Contact("C1"), Contact("C2", lastName: "Changed")]);

        var stored = Split(Aggregate(before, after)).Should().ContainSingle().Subject;

        (stored.EntityType, Type(stored), Id(stored)).Should().Be(("contact", what == "added" ? "inserted" : "updated", "C2"));
        Payload(stored)["lastName"]!.GetValue<string>().Should().Be("Changed", "the child as it is in after");
    }

    [Theory]
    [InlineData("name")]
    [InlineData("profile")]
    public void The_Account_is_stored_when_it_or_its_Profile_changed_and_its_unchanged_children_are_not(string what)
    {
        var before = Snapshot(contacts: [Contact("C1")]);
        var after = Snapshot(contacts: [Contact("C1")], name: what == "name" ? "Renamed" : "Acme", tier: what == "profile" ? "Gold" : "Silver");

        var stored = Split(Aggregate(before, after)).Should().ContainSingle().Subject;

        (stored.EntityType, Type(stored)).Should().Be(("account", "updated"));
    }

    [Fact]
    public void A_child_in_before_but_not_in_after_is_a_delete_by_its_external_id_alone_and_its_strategy_deletes_it()
    {
        var events = Split(Aggregate(Snapshot(contacts: [Contact("C1"), Contact("C2")], relationships: [Relationship("R1", "ACC-2")]),
            Snapshot(contacts: [Contact("C2")], relationships: [Relationship("R9", "ACC-3")])));

        events.Select(e => (e.EntityType, Type(e), Id(e))).Should().BeEquivalentTo(
            [("accountRelationship", "inserted", "R9"), ("accountRelationship", "deleted", "R1"), ("contact", "deleted", "C1")]);
        var contactDelete = events.Single(e => Type(e) == "deleted" && e.EntityType == "contact");
        JsonNode.DeepEquals(Payload(contactDelete), new JsonObject { ["externalId"] = "C1" }).Should().BeTrue("a delete needs the key only");

        var unit = Map(ContactSync.Strategy, contactDelete);
        unit.Records.Should().ContainSingle().Which.Should().Match<SObjectRecord>(r =>
            r.Delete && r.SObject == "Contact" && r.ExternalIdField == "ExternalId__c" && r.ExternalId == "C1" && r.Fields.Count == 0);
        Map(AccountRelationshipSync.Strategy, events.Single(e => Id(e) == "R1")).Records.Should().ContainSingle()
            .Which.Should().Match<SObjectRecord>(r => r.Delete && r.SObject == "Account_Relationship__c" && r.ExternalId == "R1");
    }

    [Theory]
    [InlineData("after null")]
    [InlineData("after absent")]
    [InlineData("type deleted")]
    public void A_deleted_Account_is_deleted_with_its_Profile_and_every_child_of_before_each_by_an_event_of_its_own(string how)
    {
        var before = Snapshot(contacts: [Contact("C1")], relationships: [Relationship("R1", "ACC-2")]);
        var aggregate = Aggregate(before, how == "type deleted" ? before.DeepClone().AsObject() : null, how == "type deleted" ? "deleted" : "updated");
        if (how == "after absent")
        {
            aggregate.Remove("after");
        }

        var events = Split(aggregate);

        events.Select(e => (e.EntityType, Type(e), Id(e))).Should().Equal(
            ("account", "deleted", "ACC-1"), ("accountRelationship", "deleted", "R1"), ("contact", "deleted", "C1"));
        Map(AccountSync.Strategy, events[0]).Records.Select(r => (r.SObject, r.ExternalId, r.Delete)).Should().Equal(
            [("Profile__c", "PRF-1", true), ("Account", "ACC-1", true)], "the Profile, by its own key, before its Account");
    }

    [Fact]
    public void A_deleted_Account_without_a_Profile_key_deletes_the_Profile_by_the_Accounts_key()
    {
        var before = Snapshot();
        before["profile"]!.AsObject().Remove("externalId");

        var account = Split(Aggregate(before, null)).Single();

        Map(AccountSync.Strategy, account).Records.Select(r => (r.SObject, r.ExternalId)).Should().Equal(
            [("Profile__c", "ACC-1"), ("Account", "ACC-1")], "a profile's key defaults to its account's");
    }

    [Fact]
    public void Property_order_does_not_make_a_change()
    {
        var before = Snapshot(contacts: [Contact("C1")]);
        var after = Snapshot(contacts: [new JsonObject { ["lastName"] = "Doe", ["externalId"] = "C1" }]);

        Split(Aggregate(before, after)).Should().BeEmpty();
    }

    [Theory]
    [InlineData("not json", "Not a valid account aggregate")]
    [InlineData("[1]", "Not a valid account aggregate")]
    [InlineData("null", "not a JSON object")]
    [InlineData(/*lang=json,strict*/ """{ "after": { "externalId": "ACC-1" } }""", "event time")]
    [InlineData(/*lang=json,strict*/ """{ "meta": { "time": "2026-10-14T01:02:03", "type": "updated" }, "after": { "externalId": "ACC-1" } }""", "event time")]
    [InlineData(/*lang=json,strict*/ """{ "meta": { "time": "2026-10-14T01:02:03Z", "type": "upsert" }, "after": { "externalId": "ACC-1" } }""", "event type")]
    [InlineData(/*lang=json,strict*/ """{ "meta": { "time": "2026-10-14T01:02:03Z", "type": "updated" } }""", "before or after")]
    [InlineData(/*lang=json,strict*/ """{ "meta": { "time": "2026-10-14T01:02:03Z", "type": "updated" }, "after": [1] }""", "Not a valid account aggregate")]
    [InlineData(/*lang=json,strict*/ """{ "meta": { "time": "2026-10-14T01:02:03Z", "type": "updated" }, "after": { "name": "x" } }""", "non-empty externalId")]
    [InlineData(/*lang=json,strict*/ """{ "meta": { "time": "2026-10-14T01:02:03Z", "type": "updated" }, "before": { "externalId": "ACC-1" }, "after": { "externalId": "ACC-2" } }""", "different")]
    public void A_message_that_is_no_aggregate_is_rejected_with_the_reason(string json, string reason)
    {
        AccountAggregate.TrySplit(json, out var events, out var problem).Should().BeFalse();
        events.Should().BeNull();
        problem.Should().ContainEquivalentOf(reason);
    }

    [Fact]
    public void A_child_without_an_external_id_is_passed_on_as_a_change_for_its_types_read_to_reject_and_never_taken_for_a_delete()
    {
        var events = Split(Aggregate(Snapshot(contacts: [new JsonObject { ["lastName"] = "Nameless" }]),
            Snapshot(contacts: [new JsonObject { ["lastName"] = "Nameless" }])));

        var stored = events.Should().ContainSingle().Subject;
        ContactSync.Strategy.TryRead(stored.Json, out _, out var problem).Should().BeFalse();
        problem.Should().Contain("non-empty id");
    }

    // ---------- Helpers ----------

    private static List<EntityEvent> Split(JsonObject aggregate)
    {
        AccountAggregate.TrySplit(aggregate.ToJsonString(), out var events, out var problem).Should().BeTrue("the aggregate is valid, but: {0}", problem);
        return events!;
    }

    /// <summary>The unit the type's strategy maps the stored event to, as the engine does after a claim.</summary>
    private static SyncUnit Map<TEvent>(SyncStrategy<TEvent> strategy, EntityEvent e)
    {
        strategy.TryRead(e.Json, out var read, out var problem).Should().BeTrue("the stored event is valid, but: {0}", problem);
        return strategy.Map(read!.EntityId, read.Event);
    }

    private static JsonObject Aggregate(JsonObject? before, JsonObject? after, string type = "updated")
    {
        return new JsonObject
        {
            ["meta"] = new JsonObject { ["time"] = Time, ["type"] = type },
            ["before"] = before,
            ["after"] = after
        };
    }

    private static JsonObject Snapshot(string name = "Acme", string tier = "Silver", JsonObject[]? contacts = null, JsonObject[]? relationships = null)
    {
        return new JsonObject
        {
            ["externalId"] = "ACC-1",
            ["name"] = name,
            ["profile"] = new JsonObject { ["externalId"] = "PRF-1", ["tier"] = tier },
            ["accountRelationships"] = new JsonArray([.. relationships ?? []]),
            ["contacts"] = new JsonArray([.. contacts ?? []])
        };
    }

    private static JsonObject Contact(string id, string lastName = "Doe")
    {
        return new JsonObject { ["externalId"] = id, ["lastName"] = lastName };
    }

    private static JsonObject Relationship(string id, string related)
    {
        return new JsonObject { ["externalId"] = id, ["relatedAccountExternalId"] = related, ["type"] = "Partner" };
    }

    private static string Type(EntityEvent e)
    {
        return JsonNode.Parse(e.Json)!["meta"]!["type"]!.GetValue<string>();
    }

    private static JsonObject Payload(EntityEvent e)
    {
        return JsonNode.Parse(e.Json)![e.EntityType]!.AsObject();
    }

    private static string Id(EntityEvent e)
    {
        return Payload(e)["externalId"]!.GetValue<string>();
    }
}