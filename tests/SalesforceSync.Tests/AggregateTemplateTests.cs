using System.Text.Json.Nodes;
using SalesforceSync.Benchmarks;
using SalesforceSync.LoadGenerator;

namespace SalesforceSync.Tests;

/// <summary>
/// Example JSON the benchmark's aggregates copy (<see cref="AggregateTemplate"/>, compiled in from benchmarks/):
/// your fields stay, ids and version markers are set, a file not fitting the event is refused.
/// </summary>
public sealed class AggregateTemplateTests
{
    private static AggregateTemplate Example => AggregateTemplate.Load(Path.Combine(AppContext.BaseDirectory, "example-aggregate.json"));

    private static JsonObject Snapshot(AggregateTemplate template, long version = 3)
    {
        return template.Account("ACC-000007", version, [template.Relationship("REL-000007-000002", "ACC-000002", version)], [template.Contact("CON-000007-1", version)]);
    }

    [Fact]
    public void The_built_in_example_has_the_fields_the_benchmark_always_published()
    {
        var account = Snapshot(AggregateTemplate.Default);

        account.Select(p => p.Key).Should().Equal("externalId", "name", "description", "profile", "accountRelationships", "contacts");
        (account["name"]!.GetValue<string>(), account["description"]!.GetValue<string>(), account["profile"]!["score"]!.GetValue<long>())
            .Should().Be(("Account ACC-000007", "v3", 3));
        account["accountRelationships"]![0]!.AsObject().Select(p => p.Key).Should().Equal("externalId", "relatedAccountExternalId", "name", "type", "isActive");
        account["contacts"]![0]!.AsObject().Select(p => p.Key).Should().Equal("externalId", "firstName", "lastName");
    }

    [Fact]
    public void Your_fields_stay_and_the_ids_and_version_markers_are_set()
    {
        var account = Snapshot(Example);

        account["externalId"]!.GetValue<string>().Should().Be("ACC-000007");
        account["description"]!.GetValue<string>().Should().Be("v3", "the marker the check reads");
        account["profile"]!["score"]!.GetValue<long>().Should().Be(3, "the marker the check reads");
        (account["industry"]!.GetValue<string>(), account["annualRevenue"]!.GetValue<decimal>(), account["tags"]!.AsArray().Count)
            .Should().Be(("Technology", 1250000.5m, 2), "everything else is as in the file");
        account["lastContactedAt"].Should().BeNull();
        account["profile"]!["name"]!.GetValue<string>().Should().Be("Profile ACC-000007", "{key} is the entity's key");

        var relationship = account["accountRelationships"]![0]!;
        (relationship["externalId"]!.GetValue<string>(), relationship["relatedAccountExternalId"]!.GetValue<string>(), relationship["name"]!.GetValue<string>(),
                relationship["startDate"]!.GetValue<string>()).Should().Be(("REL-000007-000002", "ACC-000002", "v3", "2020-03-01"));
        var contact = account["contacts"]![0]!;
        (contact["externalId"]!.GetValue<string>(), contact["lastName"]!.GetValue<string>(), contact["firstName"]!.GetValue<string>(),
                contact["email"]!.GetValue<string>()).Should().Be(("CON-000007-1", "v3", "Ada", "CON-000007-1@example.com"));
    }

    [Fact]
    public void A_template_does_not_change_between_events()
    {
        var template = Example;

        var first = Snapshot(template, 1).ToJsonString();
        Snapshot(template, 2);

        Snapshot(template, 1).ToJsonString().Should().Be(first);
    }

    [Fact]
    public void A_bare_snapshot_is_a_template_too_and_a_missing_child_example_is_the_built_in_one()
    {
        var template = AggregateTemplate.Parse(/*lang=json,strict*/ """{ "externalId": "X", "name": "Acme {key}", "industry": "Retail", "profile": { "tier": "Gold" } }""", "test");

        var account = Snapshot(template);

        account["industry"]!.GetValue<string>().Should().Be("Retail");
        account["profile"]!["tier"]!.GetValue<string>().Should().Be("Gold");
        account["contacts"]![0]!["firstName"]!.GetValue<string>().Should().Be("Contact");
    }

    [Theory]
    [InlineData(/*lang=json,strict*/ """{ "after": { "externalId": "A", "profile": {}, "nmae": "typo" } }""", "after.nmae: AccountPayload has no such property")]
    [InlineData(/*lang=json,strict*/ """{ "after": { "externalId": "A", "profile": { "tire": "Gold" } } }""", "after.profile.tire: ProfilePayload has no such property")]
    [InlineData(/*lang=json,strict*/ """{ "after": { "externalId": "A", "profile": {}, "contacts": [ { "mobile": "1" } ] } }""", "after.contacts[0].mobile: ContactPayload has no such property")]
    [InlineData(/*lang=json,strict*/ """{ "after": { "externalId": "A", "profile": {}, "accountRelationships": [ { "kind": "x" } ] } }""", "after.accountRelationships[0].kind: AccountRelationshipPayload has no such property")]
    [InlineData(/*lang=json,strict*/ """{ "after": { "externalId": "A", "profile": {}, "annualRevenue": "lots" } }""", "not an aggregate the sync can read")]
    [InlineData(/*lang=json,strict*/ """{ "after": { "externalId": "A", "profile": {}, "contacts": [ { "isActive": 1 } ] } }""", "no such property")]
    [InlineData(/*lang=json,strict*/ """{ "after": { "externalId": "A" } }""", "needs a \"profile\" object")]
    [InlineData(/*lang=json,strict*/ """{ "meta": { "time": "2026-10-14T01:02:03Z", "type": "updated" } }""", "there is no account")]
    [InlineData("[1, 2]", "the JSON is not an object")]
    [InlineData("{ not json", "not valid JSON")]
    public void A_template_that_does_not_fit_the_event_is_refused_with_what_is_wrong(string json, string problem)
    {
        var parse = () => AggregateTemplate.Parse(json, "test");

        parse.Should().Throw<InvalidDataException>().Which.Message.Should().Contain("test doesn't conform").And.Contain(problem);
    }

    [Fact]
    public void Every_problem_is_listed_at_once()
    {
        var parse = () => AggregateTemplate.Parse(/*lang=json,strict*/ """{ "after": { "externalId": "A", "nmae": 1, "profile": { "tire": 1 } } }""", "test");

        parse.Should().Throw<InvalidDataException>().Which.Message.Should().Contain("after.nmae").And.Contain("after.profile.tire");
    }

    [Fact]
    public void A_workload_of_the_template_has_its_fields_in_every_aggregate()
    {
        var workload = AggregateWorkload.Generate(300, 20, template: Example);

        var snapshots = workload.Aggregates.Select(a => JsonNode.Parse(a)!["after"]).OfType<JsonObject>().ToList();

        snapshots.Should().NotBeEmpty().And.OnlyContain(s => s["industry"]!.GetValue<string>() == "Technology" && s["profile"]!["tier"]!.GetValue<string>() == "Gold");
        snapshots.SelectMany(s => s["contacts"]!.AsArray()).Should().NotBeEmpty().And.OnlyContain(c => c!["firstName"]!.GetValue<string>() == "Ada");
    }
}
