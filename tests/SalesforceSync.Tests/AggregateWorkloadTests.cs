using System.Text.Json.Nodes;
using Microsoft.Extensions.Options;
using SalesforceSync.Data.Inbox;
using SalesforceSync.Benchmarks;
using SalesforceSync.FakeSalesforce;
using SalesforceSync.Infrastructure.Salesforce;
using SalesforceSync.Sync;

namespace SalesforceSync.Tests;

/// <summary>
/// The drain benchmarks' workload (<see cref="AggregateWorkload"/>, compiled in from benchmarks/): the end state it
/// expects is the one its aggregates lead to when split as the consumer splits them and applied in order, the newest
/// event of each entity winning; so a correct sync passes the benchmark's check. And it holds what the benchmark is
/// for: children removed and re-added, accounts deleted, every relationship naming two Accounts that stay.
/// </summary>
public sealed class AggregateWorkloadTests
{
    /// <summary>The streaming benchmark's shape and the drain's (100 and 1,000 accounts).</summary>
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
    public async Task The_check_passes_an_org_the_events_reached_in_order_and_finds_a_stale_record_and_one_not_deleted()
    {
        var workload = AggregateWorkload.Generate(1_000, 20);
        var org = new FakeSalesforceStore(new FakeSalesforceOptions { EnforceDailyLimit = false });
        var engine = SalesforceStep.Service(SalesforceStep.Http(new FakeSalesforceHandler(org)), Options.Create(new SalesforceOptions()));
        var events = Split(workload).ToList();
        foreach (var (key, e) in events)
        {
            await ApplyAsync(engine, key, e);
        }

        var (passedWrong, passedRegressions) = workload.Check(org);
        var stale = workload.Final.First(e => e.Key.Type == AccountSync.Type && e.Value is { Present: true, Version: > 1 }).Key.Key;
        org.Edit("Account", stale, new JsonObject { ["Description"] = "v1" });
        // A removed contact of an account that stays, written again (its Account exists, so the org takes it)
        var removed = workload.Final.First(e => e.Key.Type == ContactSync.Type && !e.Value.Present
                                                && workload.Final[(AccountSync.Type, OwnerOf(e.Key.Key))].Present).Key;
        await ApplyAsync(engine, removed, events.Last(e => e.Key == removed && !IsDelete(e.Event)).Event);
        var (wrong, regressions) = workload.Check(org);

        passedWrong.Should().BeEmpty();
        passedRegressions.Should().Be(0);
        wrong.Should().HaveCount(2).And.Contain(w => w.StartsWith(stale)).And.Contain($"{removed.Key}: expected deleted, still in the org");
        regressions.Should().Be(1, "v1 after a newer version");
    }

    /// <summary>An entity event through the engine minus the inbox, as a worker syncs a claimed row: read, map, write.</summary>
    private static Task ApplyAsync(SalesforceBatchSyncService engine, (string Type, string Key) entity, JsonObject e)
    {
        return entity.Type switch
        {
            AccountSync.Type => ApplyAsync(engine, AccountSync.Strategy, entity.Key, e),
            AccountRelationshipSync.Type => ApplyAsync(engine, AccountRelationshipSync.Strategy, entity.Key, e),
            _ => ApplyAsync(engine, ContactSync.Strategy, entity.Key, e)
        };
    }

    private static async Task ApplyAsync<TEvent>(SalesforceBatchSyncService engine, SyncStrategy<TEvent> strategy, string key, JsonObject e)
    {
        strategy.TryRead(e.ToJsonString(), out var read, out var problem).Should().BeTrue(problem);
        var unit = strategy.Map(key, read!.Event);
        var result = (await engine.WriteToSalesforceAsync([(key, unit)], [.. strategy.Queries([unit])],
            ContactInboxRow.MaxValuesPerQuery, ContactInboxRow.MaxRecordsPerWrite, CancellationToken.None)).Single();
        result.Outcome.Should().BeOneOf([SyncOutcome.Saved, SyncOutcome.Unchanged], $"{key}: {result.PrimaryError?.Message}");
    }

    /// <summary>The account a child's key names: REL-{account}-{other}, CON-{account}-{n}.</summary>
    private static string OwnerOf(string childKey)
    {
        return $"ACC-{childKey.Split('-')[1]}";
    }

    private static bool IsDelete(JsonObject e)
    {
        return (string?)e["meta"]!["type"] == "deleted";
    }

    /// <summary>Every entity event of the aggregates, in order, by entity type and key.</summary>
    private static IEnumerable<((string Type, string Key) Key, JsonObject Event)> Split(AggregateWorkload workload)
    {
        foreach (var aggregate in workload.Aggregates)
        {
            AccountAggregate.TrySplit(aggregate, out var events, out var problem).Should().BeTrue(problem);
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

    /// <summary>What an entity's newest event leaves in the org: nothing for a delete, else its version and Accounts.</summary>
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