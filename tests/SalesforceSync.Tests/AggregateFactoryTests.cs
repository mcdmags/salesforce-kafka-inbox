using System.Text.Json;
using System.Text.Json.Nodes;
using SalesforceSync.LoadGenerator;
using SalesforceSync.Sync;

namespace SalesforceSync.Tests;

/// <summary>
/// The records of every published aggregate (<see cref="AggregateFactory"/>, compiled in from the load generator):
/// copies of the template with new AutoFixture values, the template's empty fields and kept fields as they are, ids,
/// links and version markers set, and an aggregate the sync reads.
/// </summary>
public sealed class AggregateFactoryTests
{
    private static readonly AggregateTemplate Template = AggregateTemplate.Load(AggregateTemplate.DefaultPath);

    private static AccountSnapshot Account(AggregateFactory factory, long version = 3)
    {
        return factory.Account("ACC-000007", "PRF-000007", version, [factory.Relationship("REL-000007-000002", "ACC-000002", version)],
            [factory.Contact("CON-000007-1", version)]);
    }

    private static string Aggregate(AccountSnapshot account)
    {
        return JsonSerializer.Serialize(new AccountAggregateEvent(new EventMeta { Time = "2026-10-20T01:02:03Z", Type = "updated" }, null, account), EventJson.Options);
    }

    [Fact]
    public void Ids_links_and_version_markers_are_set_and_the_sync_reads_the_aggregate()
    {
        var account = Account(new AggregateFactory(Template, seed: 1));

        new AccountAggregate().TryRead(Aggregate(account), out _, out var problem).Should().BeTrue(problem);
        AggregateShape.AccountId.Get(account).Should().Be("ACC-000007");
        AggregateShape.AccountVersion.Version(account).Should().Be(3);
        var profile = AggregateShape.Profile.Get(account)!;
        (AggregateShape.ProfileId.Get(profile), AggregateShape.ProfileVersion.Version(profile)).Should().Be(("PRF-000007", 3L));
        var relationship = AggregateShape.Relationships.Get(account)!.Should().ContainSingle().Subject!;
        (AggregateShape.RelationshipId.Get(relationship), AggregateShape.RelatedAccount.Get(relationship), AggregateShape.RelationshipAccount.Get(relationship),
            AggregateShape.RelationshipVersion.Version(relationship)).Should().Be(("REL-000007-000002", "ACC-000002", null, 3L));
        var contact = AggregateShape.Contacts.Get(account)!.Should().ContainSingle().Subject!;
        (AggregateShape.ContactId.Get(contact), AggregateShape.ContactAccount.Get(contact), AggregateShape.ContactVersion.Version(contact))
            .Should().Be(("CON-000007-1", null, 3L));
    }

    [Fact]
    public void Filled_fields_get_new_values_empty_and_kept_ones_stay_as_in_the_template()
    {
        var original = Template.Account();
        var account = Account(new AggregateFactory(Template, seed: 1));

        account.Name.Should().NotBe(original.Name);
        account.AnnualRevenue.Should().NotBe(original.AnnualRevenue);
        account.CustomerSince.Should().NotBe(original.CustomerSince);
        account.Profile!.StartDate.Should().NotBe(original.Profile!.StartDate);
        account.LastContactedAt.Should().BeNull("null in the template");
        account.Profile.Notes.Should().BeNull("null in the template");
        (account.Rating, account.Industry, account.Profile.Tier).Should().Be((original.Rating, original.Industry, original.Profile.Tier), "picklists are kept");
        account.Tags.Should().Equal(original.Tags!, "lists are kept");
        AggregateShape.Contacts.Get(account)!.Single()!.Email.Should().Be("CON-000007-1@example.com", "kept, {key} replaced");
    }

    [Fact]
    public void The_same_seed_gives_the_same_records_and_every_change_new_values()
    {
        var first = Aggregate(Account(new AggregateFactory(Template, seed: 1)));
        var factory = new AggregateFactory(Template, seed: 1);

        Aggregate(Account(factory)).Should().Be(first);
        Account(factory).Name.Should().NotBe(Account(new AggregateFactory(Template, seed: 1)).Name);
    }

    [Fact]
    public void A_template_without_children_gets_them_from_AutoFixture()
    {
        var factory = new AggregateFactory(AggregateTemplate.Parse(/*lang=json,strict*/ """{ "after": { "externalId": "X", "profile": {} } }""", "test"), seed: 1);

        var contact = factory.Contact("CON-1", 2);

        (contact.ExternalId, contact.FirstName, AggregateShape.ContactVersion.Version(contact)).Should().Match<(string?, string?, long?)>(c =>
            c.Item1 == "CON-1" && !string.IsNullOrEmpty(c.Item2) && c.Item3 == 2);
    }

    [Fact]
    public void Texts_fit_the_shortest_Salesforce_text_fields()
    {
        var aggregate = Aggregate(Account(new AggregateFactory(Template, seed: 1)));

        foreach (var value in JsonNode.Parse(aggregate).DescendantValues())
        {
            value.Length.Should().BeLessThanOrEqualTo(40, "Phone and FirstName allow 40 characters");
        }
    }
}

internal static class JsonNodeTexts
{
    /// <summary>Every text value under <paramref name="node"/>, at any depth.</summary>
    public static IEnumerable<string> DescendantValues(this JsonNode? node)
    {
        return node switch
        {
            JsonObject o => o.SelectMany(p => p.Value.DescendantValues()),
            JsonArray a => a.SelectMany(i => i.DescendantValues()),
            JsonValue v when v.TryGetValue<string>(out var text) => [text],
            _ => []
        };
    }
}
