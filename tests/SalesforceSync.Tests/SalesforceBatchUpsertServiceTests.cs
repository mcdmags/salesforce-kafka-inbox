using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Extensions.Options;
using SalesforceSync.FakeSalesforce;
using SalesforceSync.Inbox;
using SalesforceSync.Inbox.Payloads;
using SalesforceSync.Salesforce;
using SalesforceSync.Sync;
using SalesforceSync.Sync.Mapping;

namespace SalesforceSync.Tests;

/// <summary>
/// The generic Salesforce half of the engine (Describe, composite read, diff, lookups, collections write, classification)
/// driven by the real Account and AccountRelationship strategies and mappers, against the in-memory org. Events are
/// written as the source sends them (camelCase payload names, full records); assertions read Salesforce names.
/// </summary>
public sealed class SalesforceBatchUpsertServiceTests
{
    private static readonly UpsertStrategy<AccountEvent> AccountStrategy =
        AccountSyncService.CreateStrategy(new AccountMapper(), new ProfileMapper());

    private static readonly UpsertStrategy<AccountRelationshipEvent> RelationshipStrategy =
        AccountRelationshipSyncService.CreateStrategy(new AccountRelationshipMapper());

    private readonly FakeSalesforceStore _org = new(new FakeSalesforceOptions { EnforceDailyLimit = false });
    private readonly Fixture _any = new();
    private readonly SalesforceBatchUpsertService _upserter;

    public SalesforceBatchUpsertServiceTests()
    {
        _upserter = Upserter();
    }

    /// <summary>An upserter on the same org and its own caches, with the given settings.</summary>
    private SalesforceBatchUpsertService Upserter(SalesforceOptions? salesforce = null, SyncOptions? sync = null)
    {
        var options = Options.Create(salesforce ?? new SalesforceOptions());
        var http = new HttpClient(new GzipRequestHandler(options) { InnerHandler = new FakeSalesforceHandler(_org) })
        {
            BaseAddress = new Uri("https://fake.my.salesforce.com")
        };
        return SalesforceStep.Service(http, options, sync);
    }

    // ---------- Accounts ----------

    [Fact]
    public async Task Creates_Account_and_linked_Profile()
    {
        var name = _any.Create<string>();

        var result = await Account("A1", new() { ["name"] = name }, new() { ["tier"] = "Gold" });

        result.Outcome.Should().Be(SyncOutcome.Saved);
        ((string?)_org.FindByExternalId("Account", "A1")!["Name"]).Should().Be(name);
        ((string?)_org.FindByExternalId("Profile__c", "P-A1")!["Account__c"]).Should().Be(IdOf("Account", "A1"),
            "the Profile is linked to the Account created in the same request");
    }

    [Fact]
    public async Task A_batch_costs_one_composite_read_and_one_composite_write()
    {
        await Account("A1", new() { ["name"] = _any.Create<string>() }, []);  // warms the describe cache
        var before = _org.GetStats();

        await Accounts(
            ("A2", Event(new() { ["name"] = _any.Create<string>() }, new() { ["tier"] = "Gold" })),
            ("A3", Event(new() { ["name"] = _any.Create<string>() }, new() { ["tier"] = "Silver" })));

        var after = _org.GetStats();
        (after.ApiCalls - before.ApiCalls).Should().Be(2);
        (after.CompositeRequests - before.CompositeRequests).Should().Be(2, "one composite request reads, one writes");
        (after.WriteRequests - before.WriteRequests).Should().Be(1, "every record goes in one composite request");
        (after.RecordsSucceeded - before.RecordsSucceeded).Should().Be(4, "two Accounts and two Profiles");
        (after.Describes - before.Describes).Should().Be(0, "Describe results are cached");
    }

    [Fact]
    public async Task Values_Salesforce_stores_differently_are_written_once_not_on_every_event()
    {
        var account = new Dictionary<string, object?>
        {
            ["name"] = "Acme",
            ["annualRevenue"] = 10.005m,           // currency(18,2): stored as 10.01
            ["isActive"] = null,                   // checkbox: stored as false
            ["tags"] = new[] { "Retail", "B2B" },  // multi-select: stored in picklist order
            ["lastContactedAt"] = "2026-10-13T01:02:03.4567+02:00"
        };
        var profile = new Dictionary<string, object?> { ["score"] = "42", ["discountPercent"] = 12.345m };

        (await Account("A1", account, profile)).Outcome.Should().Be(SyncOutcome.Saved);
        (await Account("A1", account, profile)).Outcome.Should().Be(SyncOutcome.Unchanged,
            "the normalized values equal what Salesforce stored");

        _org.History("Account", "A1").Should().ContainSingle();
        _org.History("Profile__c", "P-A1").Should().ContainSingle();
    }

    [Fact]
    public async Task Full_record_with_one_changed_field_sends_only_that_field()
    {
        var (name, description) = (_any.Create<string>(), _any.Create<string>());
        await Account("A1", new() { ["name"] = name, ["rating"] = "Hot", ["description"] = description }, []);

        await Account("A1", new() { ["name"] = name, ["rating"] = "Cold", ["description"] = description }, []);

        _org.History("Account", "A1")![^1].Select(kv => kv.Key).Should().Equal("Rating");
    }

    [Fact]
    public async Task Field_that_becomes_null_or_absent_is_cleared()
    {
        var name = _any.Create<string>();
        await Account("A1", new() { ["name"] = name, ["rating"] = "Hot", ["description"] = "x", ["isActive"] = true },
            new() { ["tier"] = "Gold", ["optIn"] = true });

        // rating is null; description, isActive and the profile's fields are absent
        (await Account("A1", new() { ["name"] = name, ["rating"] = null }, [])).Outcome.Should().Be(SyncOutcome.Saved);

        var account = _org.FindByExternalId("Account", "A1")!;
        account["Rating"].Should().BeNull();
        account["Description"].Should().BeNull();
        ((bool?)account["Active__c"]).Should().BeFalse("an empty checkbox is false");
        _org.History("Account", "A1")![^1].Select(kv => kv.Key).Should().BeEquivalentTo("Rating", "Description", "Active__c");
        _org.History("Profile__c", "P-A1")![^1].Select(kv => kv.Key).Should().BeEquivalentTo("Tier__c", "Opt_In__c");

        (await Account("A1", new() { ["name"] = name }, [])).Outcome.Should().Be(SyncOutcome.Unchanged,
            "empty fields that are already empty are not written again");
    }

    [Fact]
    public async Task New_record_is_created_without_its_empty_fields()
    {
        await Account("A1", new() { ["name"] = "Acme" }, []);

        _org.History("Account", "A1")!.Single().Select(kv => kv.Key).Should().BeEquivalentTo(
            ["Name", "External_Id__c", "Active__c"], "nothing to clear on a new record; a checkbox is sent as false");
    }

    [Fact]
    public async Task Account_event_without_a_profile_does_not_touch_the_Profile()
    {
        await Account("A1", new() { ["name"] = "Acme" }, new() { ["tier"] = "Gold" });

        var withoutProfile = Event(new() { ["name"] = "Acme", ["rating"] = "Hot" }, []);
        withoutProfile["account"]!.AsObject().Remove("profile");
        withoutProfile["account"]!["externalId"] = "A1";
        (await Upsert(AccountStrategy, ("A1", Read<AccountEvent>(withoutProfile)))).Single().Outcome.Should().Be(SyncOutcome.Saved);

        ((string?)_org.FindByExternalId("Profile__c", "P-A1")!["Tier__c"]).Should().Be("Gold");
        _org.History("Profile__c", "P-A1").Should().ContainSingle();
    }

    [Fact]
    public async Task Required_field_missing_from_the_full_record_fails_before_any_call()
    {
        await Account("A1", new() { ["name"] = "Acme" }, []);
        var before = _org.GetStats();

        var result = await Account("A1", new() { ["rating"] = "Hot" }, []);

        result.Outcome.Should().Be(SyncOutcome.PermanentFailure);
        (result.PrimaryError?.Code).Should().Be("REQUIRED_FIELD_MISSING", "a full record without a name would empty Account.Name");
        _org.GetStats().ApiCalls.Should().Be(before.ApiCalls);
    }

    /// <summary>
    /// Records built directly, not through a mapper: the typed layers can't even express an unknown or read-only
    /// field, but the generic upserter must still reject one for whatever builds its records.
    /// </summary>
    [Theory]
    [InlineData("No_Such_Field__c", "x", "INVALID_FIELD")]
    [InlineData("LastModifiedDate", "2026-01-01T00:00:00Z", "INVALID_FIELD_FOR_INSERT_UPDATE")]
    [InlineData("Phone", "0123456789012345678901234567890123456789+", "STRING_TOO_LONG")]
    [InlineData("NumberOfEmployees", "lots", "INVALID_TYPE_ON_FIELD_IN_RECORD")]
    public async Task A_value_Salesforce_would_reject_fails_only_its_own_unit_before_any_call(string field, string value, string code)
    {
        await Account("WARM", new() { ["name"] = "Warm" }, []);
        var before = _org.GetStats();

        var units = new List<(string Key, UpsertUnit Unit)>
        {
            ("BAD", RawAccount("BAD", new() { ["Name"] = "Bad", [field] = value })),
            ("GOOD", RawAccount("GOOD", new() { ["Name"] = "Good" }))
        };
        var results = await _upserter.WriteToSalesforceAsync(units,
            [.. AccountSyncService.Queries([.. units.Select(u => u.Unit)])], CancellationToken.None);

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
        var keys = new[] { "A1", "A2", "A3", "A4" };
        await Accounts([.. keys.Select(k => (k, Event(new() { ["name"] = "Before" }, new() { ["tier"] = "Gold" })))]);
        _org.InjectFault("Account", "A2", "FIELD_CUSTOM_VALIDATION_EXCEPTION", "Industry is required", int.MaxValue);

        var results = await Accounts([.. keys.Select(k => (k, Event(new() { ["name"] = "After" }, new() { ["tier"] = "Silver" })))]);

        var failed = results.Should().ContainSingle(r => r.Key == "A2").Subject;
        failed.Outcome.Should().Be(SyncOutcome.PermanentFailure);
        (failed.PrimaryError?.Code).Should().Be("FIELD_CUSTOM_VALIDATION_EXCEPTION");
        (failed.PrimaryError?.ReferenceId).Should().Be("acct", "the error names the record of the unit it is about");
        results.Where(r => r.Key != "A2").Select(r => r.Outcome).Should().AllBeEquivalentTo(SyncOutcome.Saved);
        ((string?)_org.FindByExternalId("Account", "A2")!["Name"]).Should().Be("Before");
        ((string?)_org.FindByExternalId("Account", "A3")!["Name"]).Should().Be("After");
    }

    [Fact]
    public async Task Row_lock_is_transient_for_that_unit_only()
    {
        await Accounts(("A1", Event(new() { ["name"] = "Before" }, [])), ("A2", Event(new() { ["name"] = "Before" }, [])));
        _org.InjectFault("Account", "A1", "UNABLE_TO_LOCK_ROW", "unable to obtain exclusive access", 1);

        var results = await Accounts(("A1", Event(new() { ["name"] = "After" }, [])), ("A2", Event(new() { ["name"] = "After" }, [])));

        results.Should().ContainSingle(r => r.Key == "A1").Which.Outcome.Should().Be(SyncOutcome.TransientFailure);
        results.Should().ContainSingle(r => r.Key == "A2").Which.Outcome.Should().Be(SyncOutcome.Saved);
        (await Account("A1", new() { ["name"] = "After" }, [])).Outcome.Should().Be(SyncOutcome.Saved, "the lock was released");
    }

    [Fact]
    public async Task Account_saved_but_Profile_failed_is_retried_until_both_are_in()
    {
        _org.InjectFault("Profile__c", "P-A1", "UNABLE_TO_LOCK_ROW", "unable to obtain exclusive access", 1);

        var first = await Account("A1", new() { ["name"] = "Acme" }, new() { ["tier"] = "Gold" });

        first.Outcome.Should().Be(SyncOutcome.TransientFailure, "the entity is synced only when all of its records are");
        (first.PrimaryError?.ReferenceId).Should().Be("prof");
        _org.FindByExternalId("Account", "A1").Should().NotBeNull("the Account and the Profile are not atomic");
        _org.FindByExternalId("Profile__c", "P-A1").Should().BeNull();

        (await Account("A1", new() { ["name"] = "Acme" }, new() { ["tier"] = "Gold" })).Outcome.Should().Be(SyncOutcome.Saved);
        ((string?)_org.FindByExternalId("Profile__c", "P-A1")!["Account__c"]).Should().Be(IdOf("Account", "A1"));
        _org.History("Account", "A1").Should().ContainSingle("the retry had nothing left to write on the Account");
    }

    [Fact]
    public async Task Create_that_loses_a_race_is_retried_into_an_update()
    {
        // Another writer creates A1 between our read and our create
        _org.InjectFault("Account", "A1", "DUPLICATE_VALUE", "duplicate value found: External_Id__c duplicates value A1", 1);
        var lost = await Account("A1", new() { ["name"] = "Ours" }, []);
        lost.Outcome.Should().Be(SyncOutcome.TransientFailure);
        (lost.PrimaryError?.Code).Should().Be("DUPLICATE_VALUE");
        await Upserter().WriteToSalesforceAsync([("A1", RawAccount("A1", new() { ["Name"] = "Theirs" }))],
            [new SoqlQuery("Account", "External_Id__c", ["A1"])], CancellationToken.None);

        (await Account("A1", new() { ["name"] = "Ours" }, [])).Outcome.Should().Be(SyncOutcome.Saved);

        Sub(_org.WriteRequests[^1], 0)["method"]!.GetValue<string>().Should().Be("PATCH", "the retry read the record and updated it");
        ((string?)_org.FindByExternalId("Account", "A1")!["Name"]).Should().Be("Ours");
        _org.All("Account").Should().ContainSingle();
    }

    // ---------- What is sent ----------

    [Fact]
    public async Task New_Accounts_and_Profiles_go_in_one_request_and_each_Profile_refers_to_its_Account_by_position()
    {
        var results = await Accounts(
            ("A1", Event(new() { ["name"] = "One" }, new() { ["tier"] = "Gold" })),
            ("A2", Event(new() { ["name"] = "Two" }, new() { ["tier"] = "Silver" })));

        results.Select(r => r.Outcome).Should().AllBeEquivalentTo(SyncOutcome.Saved);
        var write = _org.WriteRequests.Should().ContainSingle().Subject;
        write["allOrNone"]!.GetValue<bool>().Should().BeFalse("allOrNone is sent explicitly");
        write["compositeRequest"]!.AsArray().Should().HaveCount(2);

        var accounts = Sub(write, 0);
        ((string?)accounts["method"], (string?)accounts["url"], (string?)accounts["referenceId"]).Should()
            .Be(("POST", "/services/data/v62.0/composite/sobjects", "Account_create"));
        accounts["body"]!["allOrNone"]!.GetValue<bool>().Should().BeFalse();
        Records(accounts).Select(r => ((string?)r["attributes"]!["type"], (string?)r["External_Id__c"], (string?)r["Name"]))
            .Should().Equal(("Account", "A1", "One"), ("Account", "A2", "Two"));

        var profiles = Sub(write, 1);
        ((string?)profiles["method"], (string?)profiles["referenceId"]).Should().Be(("POST", "Profile__c_create_ref"));
        Records(profiles).Select(r => ((string?)r["External_Id__c"], (string?)r["Account__c"]))
            .Should().Equal(("P-A1", "@{Account_create[0].id}"), ("P-A2", "@{Account_create[1].id}"));

        ((string?)_org.FindByExternalId("Profile__c", "P-A2")!["Account__c"]).Should().Be(IdOf("Account", "A2"));
    }

    [Fact]
    public async Task Profile_of_an_existing_unchanged_Account_gets_its_Id_and_the_Account_is_not_written()
    {
        await Upsert(AccountStrategy, ("A1", Read<AccountEvent>(WithoutProfile("A1", "Acme"))));

        (await Account("A1", new() { ["name"] = "Acme" }, new() { ["tier"] = "Gold" })).Outcome.Should().Be(SyncOutcome.Saved);

        var write = _org.WriteRequests[^1];
        var only = write["compositeRequest"]!.AsArray().Should().ContainSingle().Subject!;
        ((string?)only["method"], (string?)only["referenceId"]).Should().Be(("POST", "Profile__c_create"));
        ((string?)Records(only).Single()["Account__c"]).Should().Be(IdOf("Account", "A1"), "the Id comes from the read");
    }

    [Fact]
    public async Task Update_is_a_PATCH_with_the_record_Id_and_only_the_changed_fields()
    {
        await Account("A1", new() { ["name"] = "Acme", ["rating"] = "Hot" }, new() { ["tier"] = "Gold" });

        await Account("A1", new() { ["name"] = "Acme", ["rating"] = "Cold" }, new() { ["tier"] = "Gold" });

        var only = _org.WriteRequests[^1]["compositeRequest"]!.AsArray().Should().ContainSingle().Subject!;
        ((string?)only["method"], (string?)only["referenceId"]).Should().Be(("PATCH", "Account_update"));
        Records(only).Single().Select(kv => kv.Key).Should().BeEquivalentTo("attributes", "id", "Rating");
        ((string?)Records(only).Single()["id"]).Should().Be(IdOf("Account", "A1"));
    }

    [Fact]
    public async Task One_new_Account_failing_takes_down_the_Profiles_that_use_references_but_dead_letters_nobody_else()
    {
        _org.InjectFault("Account", "A1", "FIELD_CUSTOM_VALIDATION_EXCEPTION", "Industry is required", int.MaxValue);

        var results = await Accounts(
            ("A1", Event(new() { ["name"] = "One" }, new() { ["tier"] = "Gold" })),
            ("A2", Event(new() { ["name"] = "Two" }, new() { ["tier"] = "Silver" })));

        var bad = results.Should().ContainSingle(r => r.Key == "A1").Subject;
        bad.Outcome.Should().Be(SyncOutcome.PermanentFailure);
        bad.Errors.Select(e => e.Code).Should().Equal(["FIELD_CUSTOM_VALIDATION_EXCEPTION"], "the Profile failed only because its Account did");
        _org.FindByExternalId("Profile__c", "P-A1").Should().BeNull();
        results.Should().ContainSingle(r => r.Key == "A2").Which.Outcome.Should().Be(SyncOutcome.NotAttempted,
            "its Account is in, its Profile's subrequest failed on A1's reference: it goes back without spending a retry");
        _org.FindByExternalId("Account", "A2").Should().NotBeNull();

        // The next claim no longer contains A1 (dead-lettered); A2's Account now exists, so its Profile gets a literal Id
        (await Account("A2", new() { ["name"] = "Two" }, new() { ["tier"] = "Silver" })).Outcome.Should().Be(SyncOutcome.Saved);
        ((string?)Sub(_org.WriteRequests[^1], 0)["referenceId"]).Should().Be("Profile__c_create");
        ((string?)_org.FindByExternalId("Profile__c", "P-A2")!["Account__c"]).Should().Be(IdOf("Account", "A2"));
    }

    [Fact]
    public async Task Linking_by_external_id_sends_the_relationship_and_a_failing_Account_costs_only_its_own_Profile()
    {
        var upserter = Upserter(new SalesforceOptions { ParentLink = ParentLink.ExternalId });
        _org.InjectFault("Account", "A1", "FIELD_CUSTOM_VALIDATION_EXCEPTION", "Industry is required", int.MaxValue);

        var results = await Accounts(upserter, null,
            ("A1", Event(new() { ["name"] = "One" }, new() { ["tier"] = "Gold" })),
            ("A2", Event(new() { ["name"] = "Two" }, new() { ["tier"] = "Silver" })));

        var profiles = Sub(_org.WriteRequests.Should().ContainSingle().Subject, 1);
        ((string?)profiles["referenceId"]).Should().Be("Profile__c_create");
        var second = Records(profiles)[1];
        second["Account__r"]!.ToJsonString().Should().Be(/*lang=json,strict*/ """{"External_Id__c":"A2"}""");
        second.ContainsKey("Account__c").Should().BeFalse();

        results.Should().ContainSingle(r => r.Key == "A2").Which.Outcome.Should().Be(SyncOutcome.Saved);
        ((string?)_org.FindByExternalId("Profile__c", "P-A2")!["Account__c"]).Should().Be(IdOf("Account", "A2"));
        var bad = results.Should().ContainSingle(r => r.Key == "A1").Subject;
        bad.Outcome.Should().Be(SyncOutcome.PermanentFailure, "the Account's own error decides, not the Profile's missing parent");
        (bad.PrimaryError?.Code).Should().Be("FIELD_CUSTOM_VALIDATION_EXCEPTION");
        _org.FindByExternalId("Profile__c", "P-A1").Should().BeNull();
    }

    [Fact]
    public async Task Linking_by_external_id_still_uses_the_Id_of_an_Account_that_already_exists()
    {
        var upserter = Upserter(new SalesforceOptions { ParentLink = ParentLink.ExternalId });
        await Upsert(upserter, AccountStrategy, null, ("A1", Read<AccountEvent>(WithoutProfile("A1", "Acme"))));

        (await Accounts(upserter, null, ("A1", Event(new() { ["name"] = "Acme" }, new() { ["tier"] = "Gold" })))).Single()
            .Outcome.Should().Be(SyncOutcome.Saved);

        var record = Records(_org.WriteRequests[^1]["compositeRequest"]!.AsArray().Should().ContainSingle().Subject!).Single();
        ((string?)record["Account__c"]).Should().Be(IdOf("Account", "A1"));
        record.ContainsKey("Account__r").Should().BeFalse();
    }

    [Fact]
    public async Task More_than_five_collections_are_split_over_requests_with_each_units_records_together()
    {
        // Account + Profile + relationship per unit: new units need 3 collections, existing ones 3 others
        UpsertUnit Unit(string key, string name)
        {
            return new(
        [
            new SObjectRecord("acct", "Account", "External_Id__c", key, new Dictionary<string, object?> { ["Name"] = name }),
            new SObjectRecord("prof", "Profile__c", "External_Id__c", $"P-{key}", new Dictionary<string, object?> { ["Tier__c"] = name },
                [new SObjectLookup("Account__c", "Account", "External_Id__c", key)]),
            new SObjectRecord("rel", "Account_Relationship__c", "External_Id__c", $"R-{key}", new Dictionary<string, object?> { ["Type__c"] = name },
                [new SObjectLookup("Account__c", "Account", "External_Id__c", key), new SObjectLookup("Related_Account__c", "Account", "External_Id__c", key)])
        ]);
        }

        Task<List<UpsertResult>> Send(params (string Key, UpsertUnit Unit)[] units)
        {
            return _upserter.WriteToSalesforceAsync(units,
                [
                    new SoqlQuery("Account", "External_Id__c", [.. units.Select(u => u.Key)]),
            new SoqlQuery("Profile__c", "External_Id__c", [.. units.Select(u => $"P-{u.Key}")]),
            new SoqlQuery("Account_Relationship__c", "External_Id__c", [.. units.Select(u => $"R-{u.Key}")])
                ], CancellationToken.None);
        }

        await Send(("OLD1", Unit("OLD1", "Gold")), ("OLD2", Unit("OLD2", "Gold")));
        var before = _org.GetStats();

        var results = await Send(("NEW1", Unit("NEW1", "Gold")), ("OLD1", Unit("OLD1", "Silver")),
            ("NEW2", Unit("NEW2", "Gold")), ("OLD2", Unit("OLD2", "Silver")));

        results.Select(r => r.Outcome).Should().AllBeEquivalentTo(SyncOutcome.Saved);
        var after = _org.GetStats();
        (after.WriteRequests - before.WriteRequests).Should().Be(2, "6 collections don't fit one request (5)");
        (after.ApiCalls - before.ApiCalls).Should().Be(3, "one read, two writes");
        var writes = _org.WriteRequests.TakeLast(2).Select(w => (Json: w.ToJsonString(), Collections: w["compositeRequest"]!.AsArray().Count)).ToList();
        writes.Select(w => w.Collections).Should().OnlyContain(n => n <= SalesforceRestClient.MaxCompositeCollections);
        // Each unit's three records are in the same request
        foreach (var key in new[] { "NEW1", "NEW2", "OLD1", "OLD2" })
        {
            var id = IdOf("Account", key)!;
            var own = writes.Where(w => w.Json.Contains($"\"{key}\"") || w.Json.Contains($"\"id\":\"{id}\"")).Should().ContainSingle(key).Subject;
            writes.Where(w => w.Json.Contains(IdOf("Profile__c", $"P-{key}")!) || w.Json.Contains($"P-{key}")).Should().Equal(own);
            writes.Where(w => w.Json.Contains(IdOf("Account_Relationship__c", $"R-{key}")!) || w.Json.Contains($"R-{key}")).Should().Equal(own);
        }
        ((string?)_org.FindByExternalId("Account_Relationship__c", "R-NEW2")!["Related_Account__c"]).Should().Be(IdOf("Account", "NEW2"));
        ((string?)_org.FindByExternalId("Profile__c", "P-OLD2")!["Tier__c"]).Should().Be("Silver");
    }

    // ---------- allOrNone ----------

    [Fact]
    public async Task With_allOrNone_one_bad_record_rolls_back_the_request_and_only_its_entity_fails()
    {
        var upserter = Upserter(new SalesforceOptions { AllOrNone = true });
        var keys = new[] { "A1", "A2", "A3" };
        await Accounts([.. keys.Select(k => (k, Event(new() { ["name"] = "Before" }, new() { ["tier"] = "Gold" })))]);
        _org.InjectFault("Account", "A2", "FIELD_CUSTOM_VALIDATION_EXCEPTION", "Industry is required", int.MaxValue);

        var results = await Accounts(upserter, null,
            [.. keys.Select(k => (k, Event(new() { ["name"] = "After" }, new() { ["tier"] = "Silver" })))]);

        var write = _org.WriteRequests[^1];
        write["allOrNone"]!.GetValue<bool>().Should().BeTrue();
        write["compositeRequest"]!.AsArray().Select(sub => sub!["body"]!["allOrNone"]!.GetValue<bool>()).Should().AllBeEquivalentTo(true);
        results.Should().ContainSingle(r => r.Key == "A2").Which.Outcome.Should().Be(SyncOutcome.PermanentFailure);
        results.Where(r => r.Key != "A2").Select(r => r.Outcome).Should().AllBeEquivalentTo(SyncOutcome.NotAttempted,
            "they were rolled back because of A2: retried at once, without spending a retry");
        _org.All("Account").Select(a => (string?)a["Name"]).Should().AllBe("Before");
        _org.All("Profile__c").Select(a => (string?)a["Tier__c"]).Should().AllBe("Gold");

        // The next claim: A2 is dead-lettered, the others go through
        var retried = await Accounts(upserter, null,
            [.. keys.Where(k => k != "A2").Select(k => (k, Event(new() { ["name"] = "After" }, new() { ["tier"] = "Silver" })))]);
        retried.Select(r => r.Outcome).Should().AllBeEquivalentTo(SyncOutcome.Saved);
    }

    [Fact]
    public async Task Failures_without_an_identifiable_cause_spend_a_retry_instead_of_looping()
    {
        var upserter = Upserter(new SalesforceOptions { AllOrNone = true });
        // Every record reports being rolled back because of another one; none is the cause
        _org.InjectFault("Account", "A1", "ALL_OR_NONE_OPERATION_ROLLED_BACK", "Record rolled back", int.MaxValue);

        var results = await Accounts(upserter, null,
            ("A1", Event(new() { ["name"] = "One" }, [])), ("A2", Event(new() { ["name"] = "Two" }, [])));

        results.Select(r => r.Outcome).Should().AllBeEquivalentTo(SyncOutcome.TransientFailure,
            "released as 'not attempted' they would be claimed and rolled back again forever");
        results.Select(r => r.PrimaryError?.Code).Should().AllBe("UNKNOWN_ERROR");
    }

    // ---------- The event's type against what Salesforce has ----------

    [Theory]
    [InlineData(EventType.Inserted, false, "POST")]
    [InlineData(EventType.Updated, true, "PATCH")]
    [InlineData(EventType.Inserted, true, "PATCH")]   // mismatch: the record is already there
    [InlineData(EventType.Updated, false, "POST")]    // mismatch: the record is missing
    public async Task Create_or_update_is_decided_by_Salesforce_whatever_the_event_type_says(EventType type, bool exists, string verb)
    {
        if (exists)
        {
            await Account("A1", new() { ["name"] = "Before", ["rating"] = "Hot" }, []);
        }

        var before = _org.WriteRequests.Count;

        var result = (await Accounts(_upserter, type, ("A1", Event(new() { ["name"] = "After" }, [])))).Single();

        result.Outcome.Should().Be(SyncOutcome.Saved);
        _org.WriteRequests.Should().HaveCount(before + 1);
        var account = Sub(_org.WriteRequests[^1], 0);
        ((string?)account["method"]).Should().Be(verb);
        var stored = _org.FindByExternalId("Account", "A1")!;
        ((string?)stored["Name"]).Should().Be("After");
        stored["Rating"].Should().BeNull("the event is the full record, whichever way it is written");
        if (exists)
        {
            Records(account).Single().Select(kv => kv.Key).Should().BeEquivalentTo("attributes", "id", "Name", "Rating");
        }

        _org.All("Account").Should().ContainSingle();
    }

    [Theory]
    [InlineData(EventType.Inserted)]
    [InlineData(EventType.Updated)]
    public async Task Event_that_matches_Salesforce_already_writes_nothing(EventType type)
    {
        await Account("A1", new() { ["name"] = "Same" }, new() { ["tier"] = "Gold" });
        var before = _org.GetStats().WriteRequests;

        var result = (await Accounts(_upserter, type, ("A1", Event(new() { ["name"] = "Same" }, new() { ["tier"] = "Gold" })))).Single();

        result.Outcome.Should().Be(SyncOutcome.Unchanged, "a redelivered insert creates no duplicate and updates nothing");
        _org.GetStats().WriteRequests.Should().Be(before);
    }

    [Theory]
    [InlineData(EventType.Inserted, true)]
    [InlineData(EventType.Updated, false)]
    public async Task Event_type_mismatch_can_be_dead_lettered_instead(EventType type, bool exists)
    {
        var upserter = Upserter(sync: new SyncOptions { EventTypeMismatch = EventTypeMismatch.DeadLetter });
        if (exists)
        {
            await Account("A1", new() { ["name"] = "Before" }, []);
        }

        var before = _org.GetStats().WriteRequests;

        var results = await Accounts(upserter, type,
            ("A1", Event(new() { ["name"] = "After" }, [])),
            ("A2", Event(new() { ["name"] = "Fine" }, [])));

        var mismatch = results.Should().ContainSingle(r => r.Key == "A1").Subject;
        mismatch.Outcome.Should().Be(SyncOutcome.PermanentFailure);
        (mismatch.PrimaryError?.Code).Should().Be(SalesforceErrors.EventTypeMismatch);
        (_org.FindByExternalId("Account", "A1")?["Name"]?.GetValue<string>()).Should().Be(exists ? "Before" : null);
        // A2 is new: fine for "inserted", itself a mismatch for "updated"
        results.Should().ContainSingle(r => r.Key == "A2").Which.Outcome.Should().Be(
            type == EventType.Inserted ? SyncOutcome.Saved : SyncOutcome.PermanentFailure);
        (_org.GetStats().WriteRequests - before).Should().Be(type == EventType.Inserted ? 1 : 0);
    }

    [Fact]
    public async Task Profile_moved_to_another_Account_is_relinked()
    {
        await Account("A1", new() { ["name"] = "One" }, []);
        await Account("A2", new() { ["name"] = "Two" }, []);

        // The same profile, now owned by A2
        var result = (await Accounts(("A2", Event(new() { ["name"] = "Two" }, [], profileId: "P-A1")))).Single();

        result.Outcome.Should().Be(SyncOutcome.Saved);
        ((string?)_org.FindByExternalId("Profile__c", "P-A1")!["Account__c"]).Should().Be(IdOf("Account", "A2"));
    }

    [Fact]
    public async Task Large_requests_are_sent_gzip_compressed()
    {
        var items = Enumerable.Range(0, 20)
            .Select(i => ($"A{i:D2}", Event(new() { ["name"] = $"Account number {i}", ["description"] = new string('x', 200) }, [])))
            .ToArray();

        (await Accounts(items)).Select(r => r.Outcome).Should().AllBeEquivalentTo(SyncOutcome.Saved,
            "the org decompressed and applied them");
    }

    // ---------- Account relationships: the same engine, another strategy ----------

    [Fact]
    public async Task Relationship_links_two_existing_Accounts()
    {
        await Accounts(("A1", Event(new() { ["name"] = "Parent" }, [])), ("A2", Event(new() { ["name"] = "Child" }, [])));

        var result = await Relationship("R1", "A1", "A2", new() { ["type"] = "Parent", ["isActive"] = true });

        result.Outcome.Should().Be(SyncOutcome.Saved);
        var rel = _org.FindByExternalId("Account_Relationship__c", "R1")!;
        ((string?)rel["Account__c"]).Should().Be(IdOf("Account", "A1"));
        ((string?)rel["Related_Account__c"]).Should().Be(IdOf("Account", "A2"));
        ((string?)rel["Type__c"]).Should().Be("Parent");
    }

    [Fact]
    public async Task Relationship_whose_Account_is_not_synced_yet_is_retried_later()
    {
        await Account("A1", new() { ["name"] = "Parent" }, []);
        var writesBefore = _org.GetStats().WriteRequests;

        var early = await Relationship("R1", "A1", "A2", new() { ["type"] = "Parent" });

        early.Outcome.Should().Be(SyncOutcome.TransientFailure);
        (early.PrimaryError?.Code).Should().Be(SalesforceErrors.DependencyNotSynced);
        (early.PrimaryError?.Message).Should().Contain("A2");
        _org.GetStats().WriteRequests.Should().Be(writesBefore, "nothing was sent");

        await Account("A2", new() { ["name"] = _any.Create<string>() }, []);
        (await Relationship("R1", "A1", "A2", new() { ["type"] = "Parent" })).Outcome.Should().Be(SyncOutcome.Saved);
    }

    /// <summary>The read found both Accounts, but one is gone (or not visible) by the time Salesforce applies the write.</summary>
    [Theory]
    [InlineData("INVALID_CROSS_REFERENCE_KEY", "invalid cross reference id")]
    [InlineData("ENTITY_IS_DELETED", "entity is deleted")]
    public async Task Relationship_whose_Account_Salesforce_cannot_find_at_write_time_waits_the_same_way(string code, string message)
    {
        await Accounts(("A1", Event(new() { ["name"] = "Parent" }, [])), ("A2", Event(new() { ["name"] = "Child" }, [])));
        _org.InjectFault("Account_Relationship__c", "R1", code, message, 1);

        var rejected = await Relationship("R1", "A1", "A2", new() { ["type"] = "Parent" });

        rejected.Outcome.Should().Be(SyncOutcome.TransientFailure, $"{code} on a lookup is not a reason to dead-letter");
        (rejected.PrimaryError?.Code).Should().Be(SalesforceErrors.DependencyNotSynced);
        (rejected.PrimaryError?.Message).Should().StartWith(code);
        ((string?)Records(Sub(_org.WriteRequests[^1], 0)).Single()["Account__c"]).Should().Be(IdOf("Account", "A1"),
            "a relationship is its own request, so its Accounts are always literal Ids from the read");

        (await Relationship("R1", "A1", "A2", new() { ["type"] = "Parent" })).Outcome.Should().Be(SyncOutcome.Saved);
    }

    [Fact]
    public async Task Relationship_update_sends_only_what_changed_and_relinks()
    {
        await Accounts(("A1", Event(new() { ["name"] = "1" }, [])), ("A2", Event(new() { ["name"] = "2" }, [])),
            ("A3", Event(new() { ["name"] = "3" }, [])));
        await Relationship("R1", "A1", "A2", new() { ["type"] = "Partner", ["isActive"] = true });

        await Relationship("R1", "A1", "A2", new() { ["type"] = "Reseller", ["isActive"] = true });
        _org.History("Account_Relationship__c", "R1")![^1].Select(kv => kv.Key).Should().Equal("Type__c");

        await Relationship("R1", "A1", "A3", new() { ["type"] = "Reseller", ["isActive"] = true });
        _org.History("Account_Relationship__c", "R1")![^1].Select(kv => kv.Key).Should().Equal("Related_Account__c");
        ((string?)_org.FindByExternalId("Account_Relationship__c", "R1")!["Related_Account__c"]).Should().Be(IdOf("Account", "A3"));

        (await Relationship("R1", "A1", "A3", new() { ["type"] = "Reseller", ["isActive"] = true })).Outcome
            .Should().Be(SyncOutcome.Unchanged);
    }

    // ---------- Helpers ----------

    private async Task<UpsertResult> Account(string key, Dictionary<string, object?> account, Dictionary<string, object?> profile)
    {
        return (await Accounts((key, Event(account, profile)))).Single();
    }

    private Task<List<UpsertResult>> Accounts(params (string Key, JsonObject Event)[] items)
    {
        return Accounts(_upserter, null, items);
    }

    /// <summary>Profiles are keyed "P-{account}" unless the event says otherwise.</summary>
    private Task<List<UpsertResult>> Accounts(SalesforceBatchUpsertService upserter, EventType? type, params (string Key, JsonObject Event)[] items)
    {
        return Upsert(upserter, AccountStrategy, type, [.. items.Select(i =>
            {
                var account = i.Event["account"]!.AsObject();
                account["externalId"] = i.Key;
                account["profile"]!["externalId"] ??= $"P-{i.Key}";
                return (i.Key, Read<AccountEvent>(i.Event));
            })]);
    }

    private async Task<UpsertResult> Relationship(string key, string account, string related, Dictionary<string, object?> fields)
    {
        var rel = Body(fields);
        rel["externalId"] = key;
        rel["accountExternalId"] = account;
        rel["relatedAccountExternalId"] = related;
        return (await Upsert(RelationshipStrategy,
            (key, Read<AccountRelationshipEvent>(new JsonObject { ["accountRelationship"] = rel })))).Single();
    }

    private Task<List<UpsertResult>> Upsert<TEvent>(UpsertStrategy<TEvent> strategy, params (string Key, TEvent Event)[] items)
    {
        return Upsert(_upserter, strategy, null, items);
    }

    /// <summary>What the engine does with a strategy, minus the inbox: map (with the event's type), query, upsert.</summary>
    private static Task<List<UpsertResult>> Upsert<TEvent>(SalesforceBatchUpsertService upserter, UpsertStrategy<TEvent> strategy,
        EventType? type, params (string Key, TEvent Event)[] items)
    {
        var units = items.Select(i => (i.Key, strategy.Map(i.Key, i.Event) with { EventType = type })).ToList();
        return upserter.WriteToSalesforceAsync(units, [.. strategy.Queries([.. units.Select(u => u.Item2)])], CancellationToken.None);
    }

    private static JsonObject WithoutProfile(string key, string name)
    {
        var e = Event(new() { ["name"] = name }, []);
        e["account"]!.AsObject().Remove("profile");
        e["account"]!["externalId"] = key;
        return e;
    }

    /// <summary>The i-th subrequest of a recorded composite write.</summary>
    private static JsonObject Sub(JsonObject write, int i)
    {
        return write["compositeRequest"]![i]!.AsObject();
    }

    private static List<JsonObject> Records(JsonNode collection)
    {
        return [.. collection["body"]!["records"]!.AsArray().Select(r => r!.AsObject())];
    }

    /// <summary>An account event envelope (externalId and the profile's default id are filled in by <see cref="Accounts"/>).</summary>
    private static JsonObject Event(Dictionary<string, object?> account, Dictionary<string, object?> profile, string? profileId = null)
    {
        var body = Body(account);
        body["profile"] = Body(profile);
        if (profileId is not null)
        {
            body["profile"]!["externalId"] = profileId;
        }

        return new JsonObject { ["meta"] = new JsonObject { ["time"] = "2026-10-14T01:02:03.456Z", ["type"] = "updated" }, ["account"] = body };
    }

    private static UpsertUnit RawAccount(string key, Dictionary<string, object?> fields)
    {
        return new([new SObjectRecord("acct", "Account", "External_Id__c", key, fields)]);
    }

    private static JsonObject Body(Dictionary<string, object?> fields)
    {
        return JsonSerializer.SerializeToNode(fields)!.AsObject();
    }

    /// <summary>The event as the engine reads it from the inbox.</summary>
    private static TEvent Read<TEvent>(JsonObject json)
    {
        return json.Deserialize<TEvent>(SalesforceBatchUpsertService.EventJson)!;
    }

    private string? IdOf(string sobject, string externalId)
    {
        return (string?)_org.FindByExternalId(sobject, externalId)?["Id"];
    }
}