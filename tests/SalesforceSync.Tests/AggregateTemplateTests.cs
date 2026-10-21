using SalesforceSync.LoadGenerator;
using SalesforceSync.Sync;

namespace SalesforceSync.Tests;

/// <summary>
/// The event template every published aggregate is built on (<see cref="AggregateTemplate"/>, compiled in from the
/// load generator): templates/account-event.json is one the sync reads, and a file not fitting the event classes is
/// refused with every problem listed.
/// </summary>
public sealed class AggregateTemplateTests
{
    [Fact]
    public void The_template_in_the_repository_is_an_event_the_sync_reads()
    {
        var template = AggregateTemplate.Load(AggregateTemplate.DefaultPath);

        var account = template.Account();

        AggregateShape.Profile.Get(account).Should().NotBeNull();
        template.Relationship().Should().NotBeNull();
        template.Contact().Should().NotBeNull();
        AggregateShape.Relationships.Get(account).Should().BeEmpty("children are built one by one, on the template's first");
    }

    [Fact]
    public void Every_copy_is_new()
    {
        var template = AggregateTemplate.Load(AggregateTemplate.DefaultPath);

        var first = template.Account();
        AggregateShape.AccountId.Set(first, "changed");

        AggregateShape.AccountId.Get(template.Account()).Should().NotBe("changed");
    }

    [Fact]
    public void A_template_without_children_is_one_too()
    {
        var template = AggregateTemplate.Parse(/*lang=json,strict*/ """{ "after": { "externalId": "X", "name": "Acme", "profile": { "tier": "Gold" } } }""", "test");

        (template.Relationship(), template.Contact()).Should().Be((null, null));
    }

    [Fact]
    public void A_missing_file_is_refused()
    {
        var load = () => AggregateTemplate.Load("no-such-template.json");

        load.Should().Throw<InvalidDataException>().Which.Message.Should().Contain("doesn't exist");
    }

    [Theory]
    [InlineData(/*lang=json,strict*/ """{ "after": { "externalId": "A", "profile": {}, "nmae": "typo" } }""", "after.nmae: AccountSnapshot has no such property")]
    [InlineData(/*lang=json,strict*/ """{ "after": { "externalId": "A", "profile": { "tire": "Gold" } } }""", "after.profile.tire: ProfilePayload has no such property")]
    [InlineData(/*lang=json,strict*/ """{ "after": { "externalId": "A", "profile": {}, "contacts": [ { "mobile": "1" } ] } }""", "after.contacts[0].mobile: ContactPayload has no such property")]
    [InlineData(/*lang=json,strict*/ """{ "after": { "externalId": "A", "profile": {}, "accountRelationships": [ { "kind": "x" } ] } }""", "after.accountRelationships[0].kind: AccountRelationshipPayload has no such property")]
    [InlineData(/*lang=json,strict*/ """{ "meta": { "tme": "x" }, "after": { "externalId": "A", "profile": {} } }""", "meta.tme: EventMeta has no such property")]
    [InlineData(/*lang=json,strict*/ """{ "after": { "externalId": "A", "profile": {}, "annualRevenue": "lots" } }""", "not an aggregate the sync can read")]
    [InlineData(/*lang=json,strict*/ """{ "after": { "externalId": "A" } }""", "profile: needs an object")]
    [InlineData(/*lang=json,strict*/ """{ "meta": { "time": "2026-10-14T01:02:03Z", "type": "updated" } }""", "there is no account")]
    [InlineData("[1, 2]", "the JSON is not an object")]
    [InlineData("{ not json", "not valid JSON")]
    public void A_template_that_does_not_fit_the_event_classes_is_refused_with_what_is_wrong(string json, string problem)
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
    public void Json_names_come_from_the_event_classes()
    {
        AggregateTemplate.JsonName(AggregateShape.RelatedAccount.Property).Should().Be("relatedAccountExternalId");
        AggregateTemplate.JsonName(typeof(AccountAggregateEvent).GetProperty(nameof(AccountAggregateEvent.After))!).Should().Be("after");
    }
}
