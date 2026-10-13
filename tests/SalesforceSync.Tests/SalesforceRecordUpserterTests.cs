using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using SalesforceSync.FakeSalesforce;
using SalesforceSync.Salesforce;
using SalesforceSync.Sync;

namespace SalesforceSync.Tests;

/// <summary>
/// The generic Salesforce half of the engine (Describe, composite read, diff, lookups, graph write, classification)
/// driven by the real Account and AccountRelationship strategies, against the in-memory org.
/// </summary>
public sealed class SalesforceRecordUpserterTests
{
    private readonly FakeSalesforceStore _org = new(new FakeSalesforceOptions { EnforceDailyLimit = false });
    private readonly Fixture _any = new();
    private readonly SalesforceRecordUpserter _upserter;

    public SalesforceRecordUpserterTests()
    {
        var options = Options.Create(new SalesforceOptions());
        var http = new HttpClient(new GzipRequestHandler(options) { InnerHandler = new FakeSalesforceHandler(_org) })
        {
            BaseAddress = new Uri("https://fake.my.salesforce.com")
        };
        _upserter = new SalesforceRecordUpserter(new SalesforceRestClient(http), new SalesforceSchemaCache(TimeProvider.System),
            new SalesforceErrors(options), new WriteEchoTracker(), options, NullLogger<SalesforceRecordUpserter>.Instance);
    }

    // ---------- Accounts ----------

    [Fact]
    public async Task Creates_Account_and_linked_Profile()
    {
        var name = _any.Create<string>();

        var result = await Account("A1", new() { ["Name"] = name }, new() { ["Tier__c"] = "Gold" });

        result.Outcome.Should().Be(SyncOutcome.Saved);
        ((string?)_org.FindByExternalId("Account", "A1")!["Name"]).Should().Be(name);
        ((string?)_org.FindByExternalId("Profile__c", "P-A1")!["Account__c"]).Should().Be(IdOf("Account", "A1"),
            "the Profile is linked to the Account created in the same graph");
    }

    [Fact]
    public async Task A_batch_costs_one_composite_read_and_one_graph_write()
    {
        await Account("A1", new() { ["Name"] = _any.Create<string>() }, new());  // warms the describe cache
        var before = _org.GetStats();

        await Accounts(
            ("A2", Event(new() { ["Name"] = _any.Create<string>() }, new() { ["Tier__c"] = "Gold" })),
            ("A3", Event(new() { ["Name"] = _any.Create<string>() }, new() { ["Tier__c"] = "Silver" })));

        var after = _org.GetStats();
        (after.ApiCalls - before.ApiCalls).Should().Be(2);
        (after.CompositeRequests - before.CompositeRequests).Should().Be(1, "every read goes in one composite request");
        (after.GraphRequests - before.GraphRequests).Should().Be(1, "every write goes in one composite graph request");
        (after.Describes - before.Describes).Should().Be(0, "Describe results are cached");
    }

    [Fact]
    public async Task Values_Salesforce_stores_differently_are_written_once_not_on_every_event()
    {
        var account = new Dictionary<string, object?>
        {
            ["Name"] = "Acme",
            ["AnnualRevenue"] = "10.005",  // currency(18,2): stored as 10.01
            ["Active__c"] = null,          // checkbox: stored as false
            ["Tags__c"] = "Retail;B2B",    // multi-select: stored in picklist order
            ["Last_Contacted__c"] = "2026-10-13T01:02:03.4567+02:00"
        };
        var profile = new Dictionary<string, object?> { ["Score__c"] = "42", ["Discount__c"] = 12.345m };

        (await Account("A1", account, profile)).Outcome.Should().Be(SyncOutcome.Saved);
        (await Account("A1", account, profile)).Outcome.Should().Be(SyncOutcome.Unchanged,
            "the normalized values equal what Salesforce stored");

        _org.History("Account", "A1").Should().ContainSingle();
        _org.History("Profile__c", "P-A1").Should().ContainSingle();
    }

    [Fact]
    public async Task Only_differing_fields_are_sent()
    {
        var (name, description) = (_any.Create<string>(), _any.Create<string>());
        await Account("A1", new() { ["Name"] = name, ["Rating"] = "Hot", ["Description"] = description }, new());

        await Account("A1", new() { ["Name"] = name, ["Rating"] = "Cold", ["Description"] = description }, new());

        _org.History("Account", "A1")![^1].Select(kv => kv.Key).Should().Equal("Rating");
    }

    [Theory]
    [InlineData("No_Such_Field__c", "x", "INVALID_FIELD")]
    [InlineData("LastModifiedDate", "2026-01-01T00:00:00Z", "INVALID_FIELD_FOR_INSERT_UPDATE")]
    [InlineData("Phone", "0123456789012345678901234567890123456789+", "STRING_TOO_LONG")]
    [InlineData("NumberOfEmployees", "lots", "INVALID_TYPE_ON_FIELD_IN_RECORD")]
    public async Task A_value_Salesforce_would_reject_fails_only_its_own_unit_before_any_call(string field, string value, string code)
    {
        await Account("WARM", new() { ["Name"] = "Warm" }, new());
        var before = _org.GetStats();

        var results = await Accounts(
            ("BAD", Event(new() { ["Name"] = "Bad", [field] = value }, new())),
            ("GOOD", Event(new() { ["Name"] = "Good" }, new())));

        var bad = results.Should().ContainSingle(r => r.Key == "BAD").Subject;
        bad.Outcome.Should().Be(SyncOutcome.PermanentFailure);
        (bad.PrimaryError?.Code).Should().Be(code);
        results.Should().ContainSingle(r => r.Key == "GOOD").Which.Outcome.Should().Be(SyncOutcome.Saved);
        _org.FindByExternalId("Account", "BAD").Should().BeNull();
        (_org.GetStats().ApiCalls - before.ApiCalls).Should().Be(2, "the bad unit didn't break the query");
    }

    [Fact]
    public async Task Validation_rule_error_is_permanent_for_that_unit_only()
    {
        _org.InjectFault("Account", "A1", "FIELD_CUSTOM_VALIDATION_EXCEPTION", "Industry is required", int.MaxValue);

        var results = await Accounts(("A1", Event(new() { ["Name"] = "One" }, new())), ("A2", Event(new() { ["Name"] = "Two" }, new())));

        var failed = results.Should().ContainSingle(r => r.Key == "A1").Subject;
        failed.Outcome.Should().Be(SyncOutcome.PermanentFailure);
        (failed.PrimaryError?.Code).Should().Be("FIELD_CUSTOM_VALIDATION_EXCEPTION");
        results.Should().ContainSingle(r => r.Key == "A2").Which.Outcome.Should().Be(SyncOutcome.Saved);
        _org.FindByExternalId("Profile__c", "P-A1").Should().BeNull("the graph rolled back as a whole");
    }

    [Fact]
    public async Task Row_lock_is_transient()
    {
        _org.InjectFault("Account", "A1", "UNABLE_TO_LOCK_ROW", "unable to obtain exclusive access", 1);

        var name = _any.Create<string>();

        (await Account("A1", new() { ["Name"] = name }, new())).Outcome.Should().Be(SyncOutcome.TransientFailure);
        (await Account("A1", new() { ["Name"] = name }, new())).Outcome.Should().Be(SyncOutcome.Saved, "the lock was released");
    }

    [Fact]
    public async Task Graphs_halted_after_14_failures_are_not_attempted_not_failed()
    {
        var items = Enumerable.Range(0, 25).Select(i => ($"A{i:D2}", Event(new() { ["Name"] = $"N{i}" }, new()))).ToArray();
        foreach (var (key, _) in items.Take(15))
            _org.InjectFault("Account", key, "FIELD_CUSTOM_VALIDATION_EXCEPTION", "nope", int.MaxValue);

        var results = (await Accounts(items)).ToDictionary(r => r.Key);

        items.Take(15).Select(i => results[i.Item1].Outcome).Should().AllBeEquivalentTo(SyncOutcome.PermanentFailure);
        items.Skip(15).Select(i => results[i.Item1].Outcome).Should().AllBeEquivalentTo(SyncOutcome.NotAttempted,
            "Salesforce stops processing after 14 failed graphs");
        _org.GetStats().GraphsHalted.Should().Be(10);
    }

    [Fact]
    public async Task Profile_moved_to_another_Account_is_relinked()
    {
        await Account("A1", new() { ["Name"] = "One" }, new());
        await Account("A2", new() { ["Name"] = "Two" }, new());

        // The same profile, now owned by A2
        var result = (await Accounts(("A2", Event(new() { ["Name"] = "Two" }, new(), profileId: "P-A1")))).Single();

        result.Outcome.Should().Be(SyncOutcome.Saved);
        ((string?)_org.FindByExternalId("Profile__c", "P-A1")!["Account__c"]).Should().Be(IdOf("Account", "A2"));
    }

    [Fact]
    public async Task Large_requests_are_sent_gzip_compressed()
    {
        var items = Enumerable.Range(0, 20)
            .Select(i => ($"A{i:D2}", Event(new() { ["Name"] = $"Account number {i}", ["Description"] = new string('x', 200) }, new())))
            .ToArray();

        (await Accounts(items)).Select(r => r.Outcome).Should().AllBeEquivalentTo(SyncOutcome.Saved,
            "the org decompressed and applied them");
    }

    // ---------- Account relationships: the same engine, another strategy ----------

    [Fact]
    public async Task Relationship_links_two_existing_Accounts()
    {
        await Accounts(("A1", Event(new() { ["Name"] = "Parent" }, new())), ("A2", Event(new() { ["Name"] = "Child" }, new())));

        var result = await Relationship("R1", "A1", "A2", new() { ["Type__c"] = "Parent", ["Active__c"] = true });

        result.Outcome.Should().Be(SyncOutcome.Saved);
        var rel = _org.FindByExternalId("Account_Relationship__c", "R1")!;
        ((string?)rel["Account__c"]).Should().Be(IdOf("Account", "A1"));
        ((string?)rel["Related_Account__c"]).Should().Be(IdOf("Account", "A2"));
        ((string?)rel["Type__c"]).Should().Be("Parent");
    }

    [Fact]
    public async Task Relationship_whose_Account_is_not_synced_yet_is_retried_later()
    {
        await Account("A1", new() { ["Name"] = "Parent" }, new());
        var graphsBefore = _org.GetStats().GraphRequests;

        var early = await Relationship("R1", "A1", "A2", new() { ["Type__c"] = "Parent" });

        early.Outcome.Should().Be(SyncOutcome.TransientFailure);
        (early.PrimaryError?.Code).Should().Be(SalesforceErrors.DependencyNotSynced);
        (early.PrimaryError?.Message).Should().Contain("A2");
        _org.GetStats().GraphRequests.Should().Be(graphsBefore, "nothing was written");

        await Account("A2", new() { ["Name"] = _any.Create<string>() }, new());
        (await Relationship("R1", "A1", "A2", new() { ["Type__c"] = "Parent" })).Outcome.Should().Be(SyncOutcome.Saved);
    }

    [Fact]
    public async Task Relationship_update_sends_only_what_changed_and_relinks()
    {
        await Accounts(("A1", Event(new() { ["Name"] = "1" }, new())), ("A2", Event(new() { ["Name"] = "2" }, new())),
            ("A3", Event(new() { ["Name"] = "3" }, new())));
        await Relationship("R1", "A1", "A2", new() { ["Type__c"] = "Partner", ["Active__c"] = true });

        await Relationship("R1", "A1", "A2", new() { ["Type__c"] = "Reseller", ["Active__c"] = true });
        _org.History("Account_Relationship__c", "R1")![^1].Select(kv => kv.Key).Should().Equal("Type__c");

        await Relationship("R1", "A1", "A3", new() { ["Type__c"] = "Reseller", ["Active__c"] = true });
        _org.History("Account_Relationship__c", "R1")![^1].Select(kv => kv.Key).Should().Equal("Related_Account__c");
        ((string?)_org.FindByExternalId("Account_Relationship__c", "R1")!["Related_Account__c"]).Should().Be(IdOf("Account", "A3"));

        (await Relationship("R1", "A1", "A3", new() { ["Type__c"] = "Reseller", ["Active__c"] = true })).Outcome
            .Should().Be(SyncOutcome.Unchanged);
    }

    // ---------- Helpers ----------

    private async Task<UpsertResult> Account(string key, Dictionary<string, object?> account, Dictionary<string, object?> profile)
        => (await Accounts((key, Event(account, profile)))).Single();

    /// <summary>Profiles are keyed "P-{account}" unless the event says otherwise.</summary>
    private Task<List<UpsertResult>> Accounts(params (string Key, JsonObject Event)[] items)
        => Upsert(AccountSyncService.Strategy, items.Select(i =>
        {
            var account = i.Event["account"]!.AsObject();
            account["externalId"] = i.Key;
            account["profile"]!["externalId"] ??= $"P-{i.Key}";
            return (i.Key, Read<AccountEvent>(i.Event));
        }).ToArray());

    private async Task<UpsertResult> Relationship(string key, string account, string related, Dictionary<string, object?> fields)
    {
        var rel = Body(fields);
        rel["externalId"] = key;
        rel["accountExternalId"] = account;
        rel["relatedAccountExternalId"] = related;
        return (await Upsert(AccountRelationshipSyncService.Strategy,
            (key, Read<AccountRelationshipEvent>(new JsonObject { ["accountRelationship"] = rel })))).Single();
    }

    /// <summary>What the engine does with a strategy, minus the inbox: map, query, upsert.</summary>
    private Task<List<UpsertResult>> Upsert<TEvent>(UpsertStrategy<TEvent> strategy, params (string Key, TEvent Event)[] items)
    {
        var units = items.Select(i => (i.Key, strategy.Map(i.Key, i.Event))).ToList();
        return _upserter.UpsertAsync(units, strategy.Queries(units.Select(u => u.Item2).ToList()).ToList(), CancellationToken.None);
    }

    /// <summary>An account event envelope (externalId and the profile's default id are filled in by <see cref="Accounts"/>).</summary>
    private static JsonObject Event(Dictionary<string, object?> account, Dictionary<string, object?> profile, string? profileId = null)
    {
        var body = Body(account);
        body["profile"] = Body(profile);
        if (profileId is not null) body["profile"]!["externalId"] = profileId;
        return new JsonObject { ["meta"] = new JsonObject { ["time"] = "2026-10-14T01:02:03.456Z" }, ["account"] = body };
    }

    private static JsonObject Body(Dictionary<string, object?> fields)
        => JsonSerializer.SerializeToNode(fields)!.AsObject();

    /// <summary>The event as the engine reads it from the inbox.</summary>
    private static TEvent Read<TEvent>(JsonObject json) => json.Deserialize<TEvent>(SalesforceBatchUpsertService.EventJson)!;

    private string? IdOf(string sobject, string externalId) => (string?)_org.FindByExternalId(sobject, externalId)?["Id"];
}
