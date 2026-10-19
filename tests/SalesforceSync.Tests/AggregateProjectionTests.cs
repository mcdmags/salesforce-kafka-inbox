using SalesforceSync.Sync;

namespace SalesforceSync.Tests;

/// <summary>
/// Each entity type takes only what it needs from an aggregate (<see cref="IAggregateProjection"/>): the Account its own
/// part, a child type its own list. <see cref="AccountAggregateTests"/> covers the split of a whole message.
/// </summary>
public sealed class AggregateProjectionTests
{
    private const string Meta = """{ "time": "2026-10-14T01:02:03.456Z", "type": "updated" }""";

    [Fact]
    public void Each_type_takes_only_its_own_part_of_a_new_aggregate()
    {
        var change = Read($$"""
            { "meta": {{Meta}}, "after": { "externalId": "ACC-1", "name": "Acme",
              "accountRelationships": [ { "externalId": "R1", "relatedAccountExternalId": "ACC-2" } ],
              "contacts": [ { "externalId": "C1", "lastName": "Doe" }, { "externalId": "C2", "lastName": "Roe" } ] } }
            """);

        Syncs.Account.Project(change).Select(e => e.Account!.ExternalId).Should().Equal("ACC-1");
        Syncs.Relationship.Project(change).Select(e => e.AccountRelationship!.ExternalId).Should().Equal("R1");
        Syncs.Contact.Project(change).Select(e => (e.Contact!.ExternalId, e.Contact.AccountExternalId)).Should()
            .Equal(("C1", "ACC-1"), ("C2", "ACC-1"));
    }

    [Fact]
    public void A_type_with_nothing_changed_projects_nothing_while_another_does()
    {
        var change = Read($$"""
            { "meta": {{Meta}},
              "before": { "externalId": "ACC-1", "name": "Acme", "contacts": [ { "externalId": "C1", "lastName": "Doe" } ] },
              "after": { "externalId": "ACC-1", "name": "Acme", "contacts": [ { "externalId": "C1", "lastName": "Changed" } ] } }
            """);

        Syncs.Account.Project(change).Should().BeEmpty("the account is the same");
        Syncs.Relationship.Project(change).Should().BeEmpty();
        Syncs.Contact.Project(change).Should().ContainSingle().Which.Meta!.Type.Should().Be("updated");
    }

    [Fact]
    public void A_deleted_account_is_deleted_by_each_type_for_what_it_had()
    {
        var change = Read($$"""
            { "meta": {{Meta}}, "before": { "externalId": "ACC-1", "profile": { "externalId": "PRF-1" },
              "accountRelationships": [ { "externalId": "R1" } ], "contacts": [ { "externalId": "C1" } ] }, "after": null }
            """);

        var account = Syncs.Account.Project(change).Should().ContainSingle().Subject;
        (account.Meta!.Type, account.Account!.ExternalId, account.Account.Profile!.ExternalId).Should().Be(("deleted", "ACC-1", "PRF-1"));
        Syncs.Relationship.Project(change).Select(e => (e.Meta!.Type, e.AccountRelationship!.ExternalId)).Should().Equal(("deleted", "R1"));
        Syncs.Contact.Project(change).Select(e => (e.Meta!.Type, e.Contact!.ExternalId)).Should().Equal(("deleted", "C1"));
    }

    [Fact]
    public void The_json_a_type_stores_is_one_its_strategy_reads()
    {
        var change = Read($$"""{ "meta": {{Meta}}, "after": { "externalId": "ACC-1", "contacts": [ { "externalId": "C1", "lastName": "Doe" } ] } }""");

        var json = Syncs.Contact.Strategy.ProjectJson(change).Should().ContainSingle().Subject;

        Syncs.Contact.Strategy.TryRead(json, out var read, out var problem).Should().BeTrue(problem);
        (read!.EntityId, read.Time).Should().Be(("C1", DateTimeOffset.Parse("2026-10-14T01:02:03.456Z")));
    }

    private static AggregateChange Read(string json)
    {
        Syncs.Aggregate.TryRead(json, out var change, out var problem).Should().BeTrue(problem);
        return change!;
    }
}
