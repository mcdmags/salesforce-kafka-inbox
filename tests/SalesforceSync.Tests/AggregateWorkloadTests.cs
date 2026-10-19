using System.Text.Json.Nodes;
using SalesforceSync.Benchmarks;
using SalesforceSync.FakeSalesforce;
using SalesforceSync.ToSalesforce;

namespace SalesforceSync.Tests;

/// <summary>
/// Drain benchmarks' workload (<see cref="AggregateWorkload"/>, compiled in from benchmarks/). Expected end state =
/// aggregates split as the consumer splits them, applied in order, newest event per entity wins; so a correct sync
/// passes the benchmark's check. Also holds what the benchmark is for: children removed and re-added, accounts
/// deleted, every relationship naming two Accounts that stay.
/// </summary>
public sealed class AggregateWorkloadTests
{
    /// <summary>Streaming benchmark's shape and the drain's (100 and 1,000 accounts).</summary>
    public static TheoryData<int, int> Shapes => new() { { 5_000, 50 }, { 5_000, 100 }, { 5_000, 1_000 } };

    [Theory]
    [MemberData(nameof(Shapes))]
    public void The_expected_end_state_is_what_the_split_aggregates_lead_to(int aggregates, int accounts)
    {
        var workload = AggregateWorkload.Generate(aggregates, accounts);

        var replayed = Newest(workload).ToDictionary(e => e.Key, e => Replay(e.Key.Type, e.Value));

        workload.Aggregates.Should().HaveCount(aggregates);
        workload.Final.ToDictionary(e => e.Key, e => e.Value.Present ? e.Value : new AggregateWorkload.Expected(false, 0))
            .Should().BeEquivalentTo(replayed);
    }

    [Theory]
    [MemberData(nameof(Shapes))]
    public void It_removes_and_re_adds_children_deletes_accounts_and_keeps_every_relationships_Accounts(int aggregates, int accounts)
    {
        var workload = AggregateWorkload.Generate(aggregates, accounts);
        var events = Split(workload).ToList();

        var deletedAccounts = workload.Final.Count(e => e.Key.Type == AccountSync.Type && !e.Value.Present);
        var removedChildren = workload.Final.Count(e => e.Key.Type != AccountSync.Type && !e.Value.Present
                                                        && workload.Final[(AccountSync.Type, OwnerOf(e.Key.Key))].Present);
        var readded = events.Where(e => e.Key.Type != AccountSync.Type).GroupBy(e => e.Key)
            .Count(g => g.SkipWhile(e => !IsDelete(e.Event)).Any(e => !IsDelete(e.Event)) && workload.Final[g.Key].Present);

        deletedAccounts.Should().BeInRange(1, accounts / 10, "a few whole accounts are deleted");
        removedChildren.Should().BePositive("children are removed from aggregates that go on");
        readded.Should().BePositive("a removed child comes back, as a new record");
        workload.Final.Where(e => e.Key.Type == AccountRelationshipSync.Type && e.Value.Present
                                  && !workload.Final[(AccountSync.Type, e.Value.Related!)].Present)
            .Should().BeEmpty("a relationship's Accounts both stay");
        AggregateWorkload.Generate(aggregates, accounts).Aggregates.Should().Equal(workload.Aggregates, "every iteration gets the same");
    }

    [Fact]
    public async Task The_check_passes_an_org_that_got_the_events_of_the_example_template_with_all_its_fields()
    {
        var workload = AggregateWorkload.Generate(300, 20, template: AggregateTemplate.Load(Path.Combine(AppContext.BaseDirectory, "example-aggregate.json")));
        var org = new FakeSalesforceStore(new FakeSalesforceOptions { EnforceDailyLimit = false });
        var syncs = new SyncHarness(org);
        foreach (var (key, e) in Split(workload))
        {
            await Apply(syncs, key, e);
        }

        var (wrong, regressions) = workload.Check(org);

        wrong.Should().BeEmpty();
        regressions.Should().Be(0);
        org.FindByExternalId("Account", "ACC-000001")!["Industry"]!.GetValue<string>().Should().Be("Technology");
        org.All("Contact").Should().OnlyContain(c => c["FirstName"]!.GetValue<string>() == "Ada" && c["Email"]!.GetValue<string>().EndsWith("@example.com"));
    }

    [Fact]
    public async Task The_check_passes_an_org_the_events_reached_in_order_and_finds_a_stale_record_and_one_not_deleted()
    {
        var workload = AggregateWorkload.Generate(1_000, 20);
        var org = new FakeSalesforceStore(new FakeSalesforceOptions { EnforceDailyLimit = false });
        var syncs = new SyncHarness(org);
        var events = Split(workload).ToList();
        foreach (var (key, e) in events)
        {
            await Apply(syncs, key, e);
        }

        var (passedWrong, passedRegressions) = workload.Check(org);
        var stale = workload.Final.First(e => e.Key.Type == AccountSync.Type && e.Value is { Present: true, Version: > 1 }).Key.Key;
        org.Edit("Account", stale, new JsonObject { ["Description"] = "v1" });
        // Removed contact of a staying account, written again (Account exists, so org takes it)
        var removed = workload.Final.First(e => e.Key.Type == ContactSync.Type && !e.Value.Present
                                                && workload.Final[(AccountSync.Type, OwnerOf(e.Key.Key))].Present).Key;
        await Apply(syncs, removed, events.Last(e => e.Key == removed && !IsDelete(e.Event)).Event);
        var (wrong, regressions) = workload.Check(org);

        passedWrong.Should().BeEmpty();
        passedRegressions.Should().Be(0);
        wrong.Should().HaveCount(2).And.Contain(w => w.StartsWith(stale)).And.Contain($"{removed.Key}: expected deleted, still in the org");
        regressions.Should().Be(1, "v1 after a newer version");
    }

    /// <summary>Entity event synced as a worker syncs a claimed row; must not fail.</summary>
    private static async Task Apply(SyncHarness syncs, (string Type, string Key) entity, JsonObject e)
    {
        await syncs.SyncAsync(entity.Type, entity.Key, e.ToJsonString());
        syncs.Accounts.Dead.Concat(syncs.Accounts.Failed).Concat(syncs.Relationships.Dead).Concat(syncs.Relationships.Failed)
            .Concat(syncs.Contacts.Dead).Concat(syncs.Contacts.Failed).Should().BeEmpty($"{entity.Key} was written");
    }

    /// <summary>Account a child's key names: REL-{account}-{other}, CON-{account}-{n}.</summary>
    private static string OwnerOf(string childKey)
    {
        return $"ACC-{childKey.Split('-')[1]}";
    }

    private static bool IsDelete(JsonObject e)
    {
        return (string?)e["meta"]!["type"] == "deleted";
    }

    private static IEnumerable<((string Type, string Key) Key, JsonObject Event)> Split(AggregateWorkload workload)
    {
        foreach (var aggregate in workload.Aggregates)
        {
            Syncs.TrySplit(aggregate, out var events, out var problem).Should().BeTrue(problem);
            foreach (var e in events!)
            {
                var json = JsonNode.Parse(e.Json)!.AsObject();
                yield return ((e.EntityType, (string)json[e.EntityType]!["externalId"]!), json);
            }
        }
    }

    private static Dictionary<(string Type, string Key), JsonObject> Newest(AggregateWorkload workload)
    {
        var newest = new Dictionary<(string Type, string Key), JsonObject>();
        foreach (var (key, e) in Split(workload))
        {
            newest[key] = e;
        }

        return newest;
    }

    /// <summary>What an entity's newest event leaves in org: nothing for delete, else its version and Accounts.</summary>
    private static AggregateWorkload.Expected Replay(string type, JsonObject e)
    {
        if (IsDelete(e))
        {
            return new AggregateWorkload.Expected(false, 0);
        }

        var payload = e[type]!;
        if (type == AccountSync.Type)
        {
            var version = long.Parse(((string)payload["description"]!)[1..]);
            ((long)payload["profile"]!["score"]!).Should().Be(version, "the Profile carries the account's version too");
            return new AggregateWorkload.Expected(true, version);
        }

        return type == AccountRelationshipSync.Type
            ? new AggregateWorkload.Expected(true, long.Parse(((string)payload["name"]!)[1..]),
                (string?)payload["accountExternalId"], (string?)payload["relatedAccountExternalId"])
            : new AggregateWorkload.Expected(true, long.Parse(((string)payload["lastName"]!)[1..]), (string?)payload["accountExternalId"]);
    }
}