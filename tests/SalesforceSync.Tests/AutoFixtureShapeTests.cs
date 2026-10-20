using System.Text.Json.Nodes;
using SalesforceSync.LoadGenerator;
using SalesforceSync.Sync;

namespace SalesforceSync.Tests;

/// <summary>
/// The load generator's AutoFixture mode (<see cref="AutoFixtureShape"/>, compiled in from the load generator): the
/// aggregates it builds must be ones the sync reads, with the ids set and values short enough for Salesforce.
/// </summary>
public sealed class AutoFixtureShapeTests
{
    private readonly AutoFixtureShape _shape = new();

    private JsonObject Aggregate()
    {
        return new JsonObject
        {
            ["meta"] = new JsonObject { ["time"] = "2026-10-20T01:02:03Z", ["type"] = "updated" },
            ["after"] = _shape.Account("ACC-000007", "PRF-000007", [_shape.Relationship("REL-000007-000002", "ACC-000002")], [_shape.Contact("CON-000007-1")])
        };
    }

    [Fact]
    public void Every_field_of_the_event_classes_is_filled_and_the_sync_reads_the_aggregate()
    {
        var aggregate = Aggregate();

        new AccountAggregate().TryRead(aggregate.ToJsonString(), out var change, out var problem).Should().BeTrue(problem);
        var account = change!.After!;
        (account.ExternalId, account.Profile!.ExternalId).Should().Be(("ACC-000007", "PRF-000007"));
        account.Name.Should().NotBeNullOrEmpty();
        account.Phone.Should().NotBeNullOrEmpty();
        account.AnnualRevenue.Should().NotBeNull();
        account.CustomerSince.Should().NotBeNull();
        account.LastContactedAt.Should().NotBeNull();
        account.Tags.Should().NotBeEmpty();
        account.Profile.StartDate.Should().NotBeNull();
        var relationship = account.AccountRelationships!.Should().ContainSingle().Subject!;
        (relationship.ExternalId, relationship.RelatedAccountExternalId).Should().Be(("REL-000007-000002", "ACC-000002"));
        var contact = account.Contacts!.Should().ContainSingle().Subject!;
        (contact.ExternalId, contact.Email).Should().Be(("CON-000007-1", "con-000007-1@example.com"));
    }

    [Fact]
    public void Texts_fit_the_shortest_Salesforce_text_fields()
    {
        var texts = Aggregate().ToJsonString();

        foreach (var value in JsonNode.Parse(texts)!.AsObject().DescendantValues())
        {
            value.Length.Should().BeLessThanOrEqualTo(40, "Phone and FirstName allow 40 characters");
        }
    }

    [Fact]
    public void Every_event_gets_new_values()
    {
        var first = Aggregate()["after"]!["name"]!.GetValue<string>();

        Aggregate()["after"]!["name"]!.GetValue<string>().Should().NotBe(first);
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
