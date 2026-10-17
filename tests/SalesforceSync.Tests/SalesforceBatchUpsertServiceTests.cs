using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Options;
using SalesforceSync.FakeSalesforce;
using SalesforceSync.Salesforce;
using SalesforceSync.Sync;

namespace SalesforceSync.Tests;

/// <summary>
/// The generic Salesforce half of the engine (Describe, composite read, diff, foreign keys and lookups, collections
/// write, classification) driven by the real Account, AccountRelationship and Contact strategies and mappings (Text
/// foreign keys), and by test-only objects with a real lookup (<see cref="LookupStrategies"/>), against the in-memory org.
/// Events are written as the source sends them (camelCase payload names, full records); assertions read Salesforce names.
/// </summary>
public sealed class SalesforceBatchUpsertServiceTests
{
    private static readonly UpsertStrategy<AccountEvent> AccountStrategy =
        AccountSync.Strategy;

    private static readonly UpsertStrategy<AccountRelationshipEvent> RelationshipStrategy =
        AccountRelationshipSync.Strategy;

    private static readonly UpsertStrategy<ContactEvent> ContactStrategy =
        ContactSync.Strategy;

    private readonly FakeSalesforceStore _org = new(new FakeSalesforceOptions { EnforceDailyLimit = false });
    private readonly Fixture _any = new();
    private readonly SalesforceBatchUpsertService _upserter;

    public SalesforceBatchUpsertServiceTests()
    {
        _upserter = Upserter();
    }

    /// <summary>An upserter on the same org, with its own caches.</summary>
    private SalesforceBatchUpsertService Upserter(SalesforceOptions? salesforceOptions = null)
    {
        var options = Options.Create(salesforceOptions ?? new SalesforceOptions());
        var http = SalesforceStep.Http(new GzipRequestHandler(options) { InnerHandler = new FakeSalesforceHandler(_org) });
        return SalesforceStep.Service(http, options);
    }

    // ---------- Accounts ----------

    [Fact]
    public async Task Creates_Account_and_linked_Profile()
    {
        var name = _any.Create<string>();

        var result = await Account("A1", new() { ["name"] = name }, new() { ["tier"] = "Gold" });

        result.Outcome.Should().Be(SyncOutcome.Saved);
        ((string?)_org.FindByExternalId("Account", "A1")!["Name"]).Should().Be(name);
        ((string?)_org.FindByExternalId("Profile__c", "P-A1")!["AccountId__c"]).Should().Be("A1",
            "the Profile holds its Account's external id, as text");
    }

    [Fact]
    public async Task A_batch_of_new_Accounts_and_Profiles_costs_one_composite_read_and_one_composite_write()
    {
        await Account("A1", new() { ["name"] = _any.Create<string>() }, []);  // warms the describe cache
        var before = _org.GetStats();

        await Accounts(
            ("A2", Event(new() { ["name"] = _any.Create<string>() }, new() { ["tier"] = "Gold" })),
            ("A3", Event(new() { ["name"] = _any.Create<string>() }, new() { ["tier"] = "Silver" })));

        var after = _org.GetStats();
        (after.ApiCalls - before.ApiCalls).Should().Be(2);
        (after.CompositeRequests - before.CompositeRequests).Should().Be(2, "the read, then the Accounts' and the Profiles' upserts in one");
        (after.UpsertRequests - before.UpsertRequests).Should().Be(0, "two objects are never upserts of their own");
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
            ["Name", "ExternalId__c", "Active__c", "RecordTypeId"],
            "nothing to clear on a new record; a checkbox is sent as false, the record type always");
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
            [.. AccountSync.Queries([.. units.Select(u => u.Unit)])],
            AccountInboxRow.MaxValuesPerQuery, AccountInboxRow.MaxRecordsPerWrite, CancellationToken.None);

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
        ((string?)_org.FindByExternalId("Profile__c", "P-A1")!["AccountId__c"]).Should().Be("A1");
        _org.History("Account", "A1").Should().ContainSingle("the retry had nothing left to write on the Account");
    }

    [Fact]
    public async Task Create_that_loses_a_race_is_retried_into_an_update()
    {
        // Another writer creates A1 between our read and our create
        _org.InjectFault("Account", "A1", "DUPLICATE_VALUE", "duplicate value found: ExternalId__c duplicates value A1", 1);
        var lost = await Account("A1", new() { ["name"] = "Ours" }, []);
        lost.Outcome.Should().Be(SyncOutcome.TransientFailure);
        (lost.PrimaryError?.Code).Should().Be("DUPLICATE_VALUE");
        await Upserter().WriteToSalesforceAsync([("A1", RawAccount("A1", new() { ["Name"] = "Theirs" }))],
            [new SoqlQuery("Account", "ExternalId__c", ["A1"])],
            AccountInboxRow.MaxValuesPerQuery, AccountInboxRow.MaxRecordsPerWrite, CancellationToken.None);

        (await Account("A1", new() { ["name"] = "Ours" }, [])).Outcome.Should().Be(SyncOutcome.Saved);

        ((string?)Upserted(_org.WriteRequests[^1], "Account").Single()["Name"]).Should().Be("Ours");
        _org.History("Account", "A1").Should().HaveCount(2, "the retry read the record and updated it");
        ((string?)_org.FindByExternalId("Account", "A1")!["Name"]).Should().Be("Ours");
        _org.All("Account").Should().ContainSingle();
    }

    // ---------- What is sent ----------

    [Fact]
    public async Task New_Accounts_and_Profiles_are_one_Composite_request_the_Account_upsert_then_the_Profile_upsert_carrying_the_Accounts_key()
    {
        var results = await Accounts(
            ("A1", Event(new() { ["name"] = "One" }, new() { ["tier"] = "Gold" })),
            ("A2", Event(new() { ["name"] = "Two" }, new() { ["tier"] = "Silver" })));

        results.Select(r => r.Outcome).Should().AllBeEquivalentTo(SyncOutcome.Saved);
        var write = _org.WriteRequests.Should().ContainSingle("two objects: one Composite request").Subject;
        write["allOrNone"]!.GetValue<bool>().Should().BeFalse("allOrNone is sent explicitly");
        write["compositeRequest"]!.AsArray().Select(s => (string?)s!["referenceId"]).Should().Equal("Account", "Profile__c");
        Upserted(write, "Account").Select(r => ((string?)r["ExternalId__c"], (string?)r["Name"]))
            .Should().Equal(("A1", "One"), ("A2", "Two"));
        Upserted(write, "Profile__c").Select(r => ((string?)r["ExternalId__c"], (string?)r["AccountId__c"]))
            .Should().Equal(("P-A1", "A1"), ("P-A2", "A2"));
        write.ToJsonString().Should().NotContain("@{", "a foreign key needs no reference");
    }

    [Fact]
    public async Task Profile_of_an_existing_unchanged_Account_is_written_alone_with_the_Accounts_key()
    {
        await Upsert(AccountStrategy, ("A1", Read<AccountEvent>(WithoutProfile("A1", "Acme"))));

        (await Account("A1", new() { ["name"] = "Acme" }, new() { ["tier"] = "Gold" })).Outcome.Should().Be(SyncOutcome.Saved);

        var record = Upserted(_org.WriteRequests[^1], "Profile__c").Should().ContainSingle(
            "only the Profile is written").Subject;
        ((string?)record["ExternalId__c"], (string?)record["AccountId__c"]).Should().Be(("P-A1", "A1"));
    }

    [Fact]
    public async Task Change_of_an_Account_alone_is_one_upsert_by_external_id_with_only_the_changed_fields()
    {
        await Account("A1", new() { ["name"] = "Acme", ["rating"] = "Hot" }, new() { ["tier"] = "Gold" });
        var before = _org.GetStats();

        (await Account("A1", new() { ["name"] = "Acme", ["rating"] = "Cold" }, new() { ["tier"] = "Gold" })).Outcome
            .Should().Be(SyncOutcome.Saved);

        var record = Upserted(_org.WriteRequests[^1], "Account").Should().ContainSingle().Subject;
        record.Select(kv => kv.Key).Should().BeEquivalentTo(["attributes", "ExternalId__c", "Rating"],
            "the Profile is unchanged; the Account carries its external id (which finds it) and what differs, no Id");
        ((string?)record["ExternalId__c"], (string?)record["Rating"]).Should().Be(("A1", "Cold"));
        var after = _org.GetStats();
        (after.ApiCalls - before.ApiCalls).Should().Be(2, "one read, one write: the same as a Composite write");
        (after.UpsertRequests - before.UpsertRequests).Should().Be(1);
        (after.CompositeRequests - before.CompositeRequests).Should().Be(1, "only the read is a Composite request");
        _org.History("Account", "A1")![^1].Select(kv => kv.Key).Should().Equal("Rating");
    }

    [Fact]
    public async Task Account_only_and_Profile_only_changes_are_one_Composite_request_Accounts_first()
    {
        await Accounts(("A1", Event(new() { ["name"] = "One" }, new() { ["tier"] = "Gold" })),
            ("A2", Event(new() { ["name"] = "Two" }, new() { ["tier"] = "Gold" })));
        var before = _org.GetStats();

        var results = await Accounts(("A1", Event(new() { ["name"] = "One!" }, new() { ["tier"] = "Gold" })),
            ("A2", Event(new() { ["name"] = "Two" }, new() { ["tier"] = "Silver" })));

        results.Select(r => r.Outcome).Should().AllBeEquivalentTo(SyncOutcome.Saved);
        var after = _org.GetStats();
        (after.ApiCalls - before.ApiCalls).Should().Be(2, "one read, then one write for both objects");
        (after.UpsertRequests - before.UpsertRequests).Should().Be(0);
        (after.CompositeRequests - before.CompositeRequests).Should().Be(2, "the read, and the Account and Profile upserts");
        Sub(_org.WriteRequests[^1], 0)["referenceId"]!.GetValue<string>().Should().Be("Account", "parents first");
        ((string?)Upserted(_org.WriteRequests[^1], "Account").Single()["Name"]).Should().Be("One!");
        ((string?)Upserted(_org.WriteRequests[^1], "Profile__c").Single()["Tier__c"]).Should().Be("Silver");
    }

    [Fact]
    public async Task An_entity_written_in_two_upserts_is_saved_only_when_both_are_and_costs_the_others_nothing()
    {
        await Accounts(("A1", Event(new() { ["name"] = "One" }, new() { ["tier"] = "Gold" })),
            ("A2", Event(new() { ["name"] = "Two" }, new() { ["tier"] = "Gold" })));
        _org.InjectFault("Profile__c", "P-A1", "UNABLE_TO_LOCK_ROW", "unable to obtain exclusive access", 1);

        var results = (await Accounts(("A1", Event(new() { ["name"] = "One!" }, new() { ["tier"] = "Silver" })),
            ("A2", Event(new() { ["name"] = "Two!" }, new() { ["tier"] = "Gold" })))).ToDictionary(r => r.Key);

        (results["A1"].Outcome, results["A1"].PrimaryError?.ReferenceId).Should().Be((SyncOutcome.TransientFailure, "prof"),
            "its Account is in, its Profile is not");
        results["A2"].Outcome.Should().Be(SyncOutcome.Saved, "the failed Profile is A1's alone");
        ((string?)_org.FindByExternalId("Account", "A1")!["Name"]).Should().Be("One!");
        (await Account("A1", new() { ["name"] = "One!" }, new() { ["tier"] = "Silver" })).Outcome.Should().Be(SyncOutcome.Saved);
        Upserted(_org.WriteRequests[^1], "Profile__c").Should().ContainSingle("the retry writes only what is still missing");
    }

    [Fact]
    public async Task A_new_Account_failing_takes_its_Profile_down_with_it_and_dead_letters_nobody_else()
    {
        _org.InjectFault("Account", "A1", "FIELD_CUSTOM_VALIDATION_EXCEPTION", "Industry is required", int.MaxValue);

        var results = (await Accounts(
            ("A1", Event(new() { ["name"] = "One" }, new() { ["tier"] = "Gold" })),
            ("A2", Event(new() { ["name"] = "Two" }, new() { ["tier"] = "Silver" })))).ToDictionary(r => r.Key);

        results["A1"].Outcome.Should().Be(SyncOutcome.PermanentFailure);
        results["A1"].Errors.Select(e => e.Code).Should().Equal(["FIELD_CUSTOM_VALIDATION_EXCEPTION"], "the Account's own error alone");
        results["A2"].Outcome.Should().Be(SyncOutcome.Saved);
        Upserted(_org.WriteRequests[^1], "Profile__c").Select(r => (string?)r["ExternalId__c"]).Should().Equal(["P-A1", "P-A2"],
            "A1's Profile is in the same Composite request: the org decides, and rejects it without its Account");
        _org.FindByExternalId("Profile__c", "P-A1").Should().BeNull();
    }

    [Fact]
    public async Task More_than_five_collections_are_split_over_requests_with_each_units_records_together()
    {
        // Account + Contact (a lookup) + Profile (a foreign key) per unit: a new unit needs the Account, Contact_ref (its
        // Account is new) and level-1 Profile upserts, an existing one a level-1 Contact upsert instead of Contact_ref,
        // and one without its Account level-0 Contact and Profile upserts: 6 in all
        UpsertUnit Unit(string key, string name, bool withAccount = true)
        {
            var account = new SObjectRecord("acct", "Account", "ExternalId__c", key, new Dictionary<string, object?> { ["Name"] = name });
            return new(
            [
                .. withAccount ? [account] : Array.Empty<SObjectRecord>(),
                new SObjectRecord("contact", "Contact", "ExternalId__c", $"C-{key}", new Dictionary<string, object?> { ["LastName"] = name },
                    [new SObjectLookup("AccountId", "Account", "ExternalId__c", key)]),
                new SObjectRecord("prof", "Profile__c", "ExternalId__c", $"P-{key}", new Dictionary<string, object?> { ["Tier__c"] = name, ["AccountId__c"] = key },
                    ForeignKeys: [new SObjectLookup("AccountId__c", "Account", "ExternalId__c", key)])
            ]);
        }

        Task<List<UpsertResult>> Send(params (string Key, UpsertUnit Unit)[] units)
        {
            return _upserter.WriteToSalesforceAsync(units,
            [
                new SoqlQuery("Account", "ExternalId__c", [.. units.Select(u => u.Key)]),
                new SoqlQuery("Contact", "ExternalId__c", [.. units.Select(u => $"C-{u.Key}")]),
                new SoqlQuery("Profile__c", "ExternalId__c", [.. units.Select(u => $"P-{u.Key}")])
            ], AccountInboxRow.MaxValuesPerQuery, AccountInboxRow.MaxRecordsPerWrite, CancellationToken.None);
        }

        await Send(("OLD1", Unit("OLD1", "Gold")), ("OLD2", Unit("OLD2", "Gold")));
        var before = _org.GetStats();

        var results = await Send(("NEW1", Unit("NEW1", "Gold")), ("NEW2", Unit("NEW2", "Gold")),
            ("OLD1", Unit("OLD1", "Silver", withAccount: false)), ("OLD2", Unit("OLD2", "Silver")));

        results.Select(r => r.Outcome).Should().AllBeEquivalentTo(SyncOutcome.Saved);
        var after = _org.GetStats();
        (after.ApiCalls - before.ApiCalls).Should().Be(3, "one read; 6 upserts don't fit one Composite request (5): two");
        var writes = _org.WriteRequests.TakeLast(2).Select(w => w["compositeRequest"]!.AsArray()).ToList();
        writes[0].Select(s => (string?)s!["referenceId"]).Should().Equal("Account", "Contact", "Profile__c", "Contact_ref", "Profile__c_4");
        writes[0].SelectMany(s => Records(s!)).Select(r => (string?)r["ExternalId__c"]).Should().BeEquivalentTo(
            ["NEW1", "NEW2", "C-NEW1", "C-NEW2", "P-NEW1", "P-NEW2", "C-OLD1", "P-OLD1"], "each unit's records are in the same request");
        writes[1].Select(s => (string?)s!["referenceId"]).Should().Equal("Account", "Contact", "Profile__c");
        writes[1].SelectMany(s => Records(s!)).Select(r => (string?)r["ExternalId__c"]).Should().Equal("OLD2", "C-OLD2", "P-OLD2");
        ((string?)_org.FindByExternalId("Contact", "C-NEW2")!["AccountId"]).Should().Be(IdOf("Account", "NEW2"));
        ((string?)_org.FindByExternalId("Profile__c", "P-NEW2")!["AccountId__c"]).Should().Be("NEW2");
        ((string?)_org.FindByExternalId("Profile__c", "P-OLD2")!["Tier__c"]).Should().Be("Silver");
    }

    // ---------- Query URL length ----------

    [Fact]
    public async Task Ids_too_long_for_five_queries_are_read_with_more_Composite_requests()
    {
        // 100 Accounts and 100 Profiles with 250-character ids: about 45 ids fit a query's URL, so 3 queries each
        var units = Enumerable.Range(0, 100).Select(i => ($"{i:D3}{new string('a', 247)}", Event(new() { ["name"] = $"N{i}" }, []))).ToArray();
        await Account("WARM", new() { ["name"] = "Warm" }, []); // warms the describe cache
        var before = _org.GetStats();

        (await Accounts(units)).Select(r => r.Outcome).Should().AllBeEquivalentTo(SyncOutcome.Saved);
        var afterCreate = _org.GetStats();
        (await Accounts(units)).Select(r => r.Outcome).Should().AllBeEquivalentTo(SyncOutcome.Unchanged,
            "the rows of both reads are found by their reference ids");

        var after = _org.GetStats();
        (afterCreate.CompositeRequests - before.CompositeRequests).Should().Be(3, "6 queries: two reads; then one write");
        (after.ApiCalls - afterCreate.ApiCalls).Should().Be(2, "the two reads; nothing to write");
        (after.Queries - afterCreate.Queries).Should().Be(6);
    }

    [Fact]
    public async Task A_query_URL_over_the_budget_is_refused_before_any_call_and_one_at_it_is_sent()
    {
        var client = new SalesforceRestClient(SalesforceStep.Http(new FakeSalesforceHandler(_org)));
        static string Soql(int urlBytes)
        {
            var empty = SalesforceRestClient.Soql("Account", ["Id"], "ExternalId__c", [""]);
            return SalesforceRestClient.Soql("Account", ["Id"], "ExternalId__c",
                [new string('a', urlBytes - SalesforceRestClient.QueryUrl(empty).Length)]);
        }

        var before = _org.GetStats();
        var tooLong = () => client.QueryAsync([new CompositeQuery("q0", Soql(SalesforceRestClient.MaxUrlBytes + 1))], CancellationToken.None);
        await tooLong.Should().ThrowAsync<ArgumentException>().WithMessage("The URL is * bytes; a call allows *");
        _org.GetStats().ApiCalls.Should().Be(before.ApiCalls);

        var atBudget = Soql(SalesforceRestClient.MaxUrlBytes);
        SalesforceRestClient.QueryUrl(atBudget).Length.Should().Be(SalesforceRestClient.MaxUrlBytes);
        (await client.QueryAsync([new CompositeQuery("q0", atBudget)], CancellationToken.None))["q0"].Should().BeEmpty();
    }

    [Fact]
    public async Task The_mock_org_refuses_a_URI_over_16384_bytes_as_Salesforce_does()
    {
        var http = SalesforceStep.Http(new FakeSalesforceHandler(_org));
        var subrequest = new JsonObject
        {
            ["compositeRequest"] = new JsonArray(new JsonObject
            {
                ["method"] = "GET",
                ["url"] = $"/services/data/v62.0/query?q={new string('a', FakeSalesforceStore.MaxUriAndHeadersBytes)}",
                ["referenceId"] = "q0"
            })
        };

        using var direct = await http.GetAsync($"/services/data/v62.0/query?q={new string('a', FakeSalesforceStore.MaxUriAndHeadersBytes)}");
        using var composite = await http.PostAsync("/services/data/v62.0/composite",
            new StringContent(subrequest.ToJsonString(), System.Text.Encoding.UTF8, "application/json"));

        direct.StatusCode.Should().Be(System.Net.HttpStatusCode.RequestUriTooLong);
        var answer = JsonNode.Parse(await composite.Content.ReadAsStringAsync())!["compositeResponse"]![0]!;
        ((int)answer["httpStatusCode"]!, (string?)answer["body"]![0]!["errorCode"]).Should().Be((414, "URI_TOO_LONG"));
    }

    // ---------- Real lookups (test-only objects): Id resolution and @{reference} ----------

    [Fact]
    public async Task A_new_parent_and_its_new_child_by_lookup_go_in_one_Composite_request_linked_by_reference()
    {
        var results = await Upsert(LookupStrategies.AccountWithContact,
            ("A1", Read<AccountEvent>(WithoutProfile("A1", "One"))), ("A2", Read<AccountEvent>(WithoutProfile("A2", "Two"))));

        results.Select(r => r.Outcome).Should().AllBeEquivalentTo(SyncOutcome.Saved);
        var write = _org.WriteRequests.Should().ContainSingle().Subject;
        write["allOrNone"]!.GetValue<bool>().Should().BeFalse("allOrNone is sent explicitly");
        ((string?)Sub(write, 0)["method"], (string?)Sub(write, 0)["url"], (string?)Sub(write, 0)["referenceId"]).Should()
            .Be(("PATCH", "/services/data/v62.0/composite/sobjects/Account/ExternalId__c", "Account"));
        Records(Sub(write, 0)).Select(r => (string?)r["ExternalId__c"]).Should().Equal("A1", "A2");
        ((string?)Sub(write, 1)["referenceId"]).Should().Be("Contact_ref");
        Records(Sub(write, 1)).Select(r => ((string?)r["ExternalId__c"], (string?)r["AccountId"]))
            .Should().Equal(("C-A1", "@{Account[0].id}"), ("C-A2", "@{Account[1].id}"));
        ((string?)_org.FindByExternalId("Contact", "C-A2")!["AccountId"]).Should().Be(IdOf("Account", "A2"));
    }

    [Fact]
    public async Task Any_strategy_writes_one_object_as_one_upsert_and_several_as_one_Composite_request()
    {
        // The rule is the engine's, not the example objects': a strategy of other classes gets it as they are
        static AccountEvent Changed(string name, string rating)
        {
            var e = WithoutProfile("A1", name);
            e["account"]!["rating"] = rating;
            return Read<AccountEvent>(e);
        }

        await Upsert(LookupStrategies.AccountWithContact, ("A1", Changed("One", "Hot")));
        var before = _org.GetStats();

        (await Upsert(LookupStrategies.AccountWithContact, ("A1", Changed("One", "Cold")))).Single().Outcome.Should().Be(SyncOutcome.Saved);
        var oneObject = _org.GetStats();
        (await Upsert(LookupStrategies.AccountWithContact, ("A1", Changed("Two", "Cold")))).Single().Outcome.Should().Be(SyncOutcome.Saved);
        var twoObjects = _org.GetStats();

        (oneObject.UpsertRequests - before.UpsertRequests, oneObject.CompositeRequests - before.CompositeRequests)
            .Should().Be((1, 1), "only the Account changed: the read, then its upsert as a call of its own");
        (twoObjects.UpsertRequests - oneObject.UpsertRequests, twoObjects.CompositeRequests - oneObject.CompositeRequests)
            .Should().Be((0, 2), "the Account's name and its Contact's last name changed: the read, then one Composite request");
        _org.WriteRequests[^1]["compositeRequest"]!.AsArray().Select(s => (string?)s!["referenceId"]).Should().Equal("Account", "Contact");
        ((string?)Upserted(_org.WriteRequests[^1], "Contact").Single()["LastName"]).Should().Be("Two");
    }

    [Fact]
    public async Task A_new_parent_failing_takes_down_the_children_that_use_references_but_dead_letters_nobody_else()
    {
        _org.InjectFault("Account", "A1", "FIELD_CUSTOM_VALIDATION_EXCEPTION", "Industry is required", int.MaxValue);

        var results = (await Upsert(LookupStrategies.AccountWithContact,
            ("A1", Read<AccountEvent>(WithoutProfile("A1", "One"))), ("A2", Read<AccountEvent>(WithoutProfile("A2", "Two"))))).ToDictionary(r => r.Key);

        results["A1"].Outcome.Should().Be(SyncOutcome.PermanentFailure);
        results["A1"].Errors.Select(e => e.Code).Should().Equal(["FIELD_CUSTOM_VALIDATION_EXCEPTION"], "the Contact failed only because its Account did");
        results["A2"].Outcome.Should().Be(SyncOutcome.NotAttempted,
            "its Account is in, its Contact's subrequest failed on A1's reference: it goes back without spending a retry");

        // The next claim no longer contains A1 (dead-lettered); A2's Account now exists, so its Contact gets a literal Id
        (await Upsert(LookupStrategies.AccountWithContact, ("A2", Read<AccountEvent>(WithoutProfile("A2", "Two"))))).Single()
            .Outcome.Should().Be(SyncOutcome.Saved);
        ((string?)Upserted(_org.WriteRequests[^1], "Contact").Single()["AccountId"]).Should().Be(IdOf("Account", "A2"));
    }

    [Fact]
    public async Task A_lookup_to_a_record_of_another_inbox_gets_the_Id_the_read_found_or_waits_for_it()
    {
        await Account("A1", new() { ["name"] = "Acme" }, []);

        var saved = await LookupContact("C1", "A1");
        var early = await LookupContact("C2", "MISSING");

        saved.Outcome.Should().Be(SyncOutcome.Saved);
        ((string?)Upserted(_org.WriteRequests[^1], "Contact").Single()["AccountId"]).Should().Be(IdOf("Account", "A1"), "the Id comes from the read");
        (early.Outcome, early.PrimaryError?.Code).Should().Be((SyncOutcome.TransientFailure, SalesforceErrors.DependencyNotSynced));
        _org.FindByExternalId("Contact", "C2").Should().BeNull("nothing is sent for it");
        (await LookupContact("C1", "A1")).Outcome.Should().Be(SyncOutcome.Unchanged, "AccountId already holds that Id");
    }

    /// <summary>The read found the Account, but it is gone (or not visible) by the time Salesforce applies the write.</summary>
    [Theory]
    [InlineData("INVALID_CROSS_REFERENCE_KEY", "invalid cross reference id")]
    [InlineData("ENTITY_IS_DELETED", "entity is deleted")]
    public async Task A_lookup_target_Salesforce_cannot_find_at_write_time_waits_the_same_way(string code, string message)
    {
        await Account("A1", new() { ["name"] = "Acme" }, []);
        _org.InjectFault("Contact", "C1", code, message, 1);

        var rejected = await LookupContact("C1", "A1");

        rejected.Outcome.Should().Be(SyncOutcome.TransientFailure, $"{code} on a lookup is not a reason to dead-letter");
        (rejected.PrimaryError?.Code).Should().Be(SalesforceErrors.DependencyNotSynced);
        (rejected.PrimaryError?.Message).Should().StartWith(code);
        (await LookupContact("C1", "A1")).Outcome.Should().Be(SyncOutcome.Saved);
    }

    private async Task<UpsertResult> LookupContact(string key, string account)
    {
        var contact = new JsonObject { ["externalId"] = key, ["accountExternalId"] = account, ["lastName"] = "Doe" };
        return (await Upsert(LookupStrategies.Contact, (key, Read<ContactEvent>(new JsonObject { ["contact"] = contact })))).Single();
    }

    // ---------- Record types ----------

    [Fact]
    public async Task Describe_returns_the_record_types_of_the_object()
    {
        var http = SalesforceStep.Http(new FakeSalesforceHandler(_org));

        var schema = (await new SalesforceRestClient(http).DescribeAsync("Account", null, CancellationToken.None))!;

        schema.RecordTypes.Should().ContainKey("business").WhoseValue.Should().Be(
            new SalesforceRecordType("Business", FakeSchema.RecordTypeIdOf("Account", "Business"), Active: true, Available: true));
        schema.RecordTypes["Master"].Available.Should().BeFalse();
        schema.Find(SObjectSchema.RecordTypeIdField)!.Type.Should().Be("reference");
    }

    [Fact]
    public async Task Every_record_is_created_with_its_classes_record_type()
    {
        await Account("A1", new() { ["name"] = "One" }, new() { ["tier"] = "Gold" });
        await Account("A2", new() { ["name"] = "Two" }, []);

        (await Relationship("R1", "A1", "A2", [])).Outcome.Should().Be(SyncOutcome.Saved);

        ((string?)_org.FindByExternalId("Account", "A1")!["RecordTypeId"]).Should().Be(FakeSchema.RecordTypeIdOf("Account", "Business"));
        ((string?)_org.FindByExternalId("Profile__c", "P-A1")!["RecordTypeId"]).Should().Be(FakeSchema.RecordTypeIdOf("Profile__c", "Standard"));
        ((string?)_org.FindByExternalId("Account_Relationship__c", "R1")!["RecordTypeId"])
            .Should().Be(FakeSchema.RecordTypeIdOf("Account_Relationship__c", "Standard"));
    }

    [Fact]
    public async Task The_record_type_is_written_only_when_Salesforce_has_another()
    {
        await Account("A1", new() { ["name"] = "Acme" }, []);
        (await Account("A1", new() { ["name"] = "Acme" }, [])).Outcome.Should().Be(SyncOutcome.Unchanged, "the record type is the same");

        _org.Edit("Account", "A1", new JsonObject { ["RecordTypeId"] = FakeSchema.RecordTypeIdOf("Account", "Master") });
        (await Account("A1", new() { ["name"] = "Acme" }, [])).Outcome.Should().Be(SyncOutcome.Saved);

        _org.History("Account", "A1")![^1].Select(kv => kv.Key).Should().Equal("RecordTypeId");
        ((string?)_org.FindByExternalId("Account", "A1")!["RecordTypeId"]).Should().Be(FakeSchema.RecordTypeIdOf("Account", "Business"));
    }

    [Fact]
    public async Task A_record_type_the_org_lacks_fails_before_any_call_is_retried_and_describes_again()
    {
        await Account("A1", new() { ["name"] = "Acme" }, []);  // warms the describe cache
        var before = _org.GetStats();

        var result = (await _upserter.WriteToSalesforceAsync(
            [("X1", new UpsertUnit([SObjectRecord.From("acct", new UnknownRecordTypeAccount { ExternalId = "X1" })]))],
            [new SoqlQuery("Account", "ExternalId__c", ["X1"])],
            AccountInboxRow.MaxValuesPerQuery, AccountInboxRow.MaxRecordsPerWrite, CancellationToken.None)).Single();

        result.Outcome.Should().Be(SyncOutcome.TransientFailure, "the record type may be created or assigned any time");
        result.PrimaryError!.Code.Should().Be(SalesforceErrors.InvalidRecordType);
        result.PrimaryError.Message.Should().Contain("'Nope'").And.Contain("Account");
        var afterFailure = _org.GetStats();
        (afterFailure.ApiCalls - before.ApiCalls).Should().Be(0, "nothing is read or written");

        await Account("A2", new() { ["name"] = "Two" }, []);
        var after = _org.GetStats();
        (after.Describes - afterFailure.Describes).Should().Be(1, "the cached describe of Account was invalidated");
        (after.DescribesNotModified - afterFailure.DescribesNotModified).Should().Be(0, "an invalidated describe is unconditional");
    }

    [Fact]
    public async Task A_record_type_Id_Salesforce_rejects_is_retried_not_dead_lettered()
    {
        var unknownId = "012FAKE00000099";   // deactivated or unassigned after the describe, say

        var result = (await _upserter.WriteToSalesforceAsync(
            [("A1", RawAccount("A1", new() { ["Name"] = "Acme", ["RecordTypeId"] = unknownId }))],
            [new SoqlQuery("Account", "ExternalId__c", ["A1"])],
            AccountInboxRow.MaxValuesPerQuery, AccountInboxRow.MaxRecordsPerWrite, CancellationToken.None)).Single();

        result.Outcome.Should().Be(SyncOutcome.TransientFailure);
        result.PrimaryError!.Code.Should().Be(SalesforceErrors.InvalidRecordType);
        result.PrimaryError.Message.Should().StartWith("INVALID_CROSS_REFERENCE_KEY");
        _org.FindByExternalId("Account", "A1").Should().BeNull();
    }

    /// <summary>Salesforce often names no field on this error; the message still identifies the record type.</summary>
    [Fact]
    public async Task A_record_type_rejection_without_fields_is_an_invalid_record_type_and_describes_again()
    {
        await Account("A1", new() { ["name"] = "Acme" }, []);
        var before = _org.GetStats();
        _org.InjectFault("Profile__c", "P-A2", "INVALID_CROSS_REFERENCE_KEY",
            "Record Type ID: this ID value isn't valid for the user: 012000000000001AAA", 1);

        var rejected = await Account("A2", new() { ["name"] = "Two" }, []);

        rejected.Outcome.Should().Be(SyncOutcome.TransientFailure);
        (rejected.PrimaryError?.Code).Should().Be(SalesforceErrors.InvalidRecordType,
            "not a missing parent, although the profile has a foreign key");
        (await Account("A2", new() { ["name"] = "Two" }, [])).Outcome.Should().Be(SyncOutcome.Saved);
        (_org.GetStats().Describes - before.Describes).Should().BeGreaterThan(0, "the cached describe was invalidated");
    }

    [SObject("Account", ExternalIdField = "ExternalId__c", RecordType = "Nope")]
    private sealed class UnknownRecordTypeAccount
    {
        [JsonPropertyName("ExternalId__c")] public string? ExternalId { get; set; }
    }

    // ---------- Records that fail without a cause ----------

    [Fact]
    public async Task Failures_without_an_identifiable_cause_spend_a_retry_instead_of_looping()
    {
        // The Account reports being skipped because of another record, but no record is the cause
        _org.InjectFault("Account", "A1", "PROCESSING_HALTED", "Halted", int.MaxValue);

        var result = await Account("A1", new() { ["name"] = "One" }, []);

        result.Outcome.Should().Be(SyncOutcome.TransientFailure,
            "released as 'not attempted' it would be claimed and skipped again forever");
        (result.PrimaryError?.Code).Should().Be("UNKNOWN_ERROR");
    }

    // ---------- Create or update: decided by what Salesforce has ----------

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Create_or_update_is_decided_by_what_Salesforce_has(bool exists)
    {
        if (exists)
        {
            await Account("A1", new() { ["name"] = "Before", ["rating"] = "Hot" }, []);
        }

        var before = _org.WriteRequests.Count;

        var result = await Account("A1", new() { ["name"] = "After" }, []);

        result.Outcome.Should().Be(SyncOutcome.Saved);
        if (exists)
        {
            // Only the Account changed: an upsert carrying what differs
            _org.WriteRequests.Should().HaveCount(before + 1);
            Upserted(_org.WriteRequests[^1], "Account").Single().Select(kv => kv.Key)
                .Should().BeEquivalentTo("attributes", "ExternalId__c", "Name", "Rating");
        }
        else
        {
            // A new Account and its new Profile: one Composite request, the Account's upsert first, then the Profile's
            _org.WriteRequests.Should().HaveCount(before + 1);
            Upserted(_org.WriteRequests[^1], "Account").Single()["ExternalId__c"]!.GetValue<string>().Should().Be("A1");
            Upserted(_org.WriteRequests[^1], "Profile__c").Single()["AccountId__c"]!.GetValue<string>().Should().Be("A1");
        }

        var stored = _org.FindByExternalId("Account", "A1")!;
        ((string?)stored["Name"]).Should().Be("After");
        stored["Rating"].Should().BeNull("the event is the full record, whichever way it is written");
        _org.All("Account").Should().ContainSingle();
    }

    [Fact]
    public async Task Event_that_matches_Salesforce_already_writes_nothing()
    {
        await Account("A1", new() { ["name"] = "Same" }, new() { ["tier"] = "Gold" });
        var before = _org.GetStats().WriteRequests;

        var result = await Account("A1", new() { ["name"] = "Same" }, new() { ["tier"] = "Gold" });

        result.Outcome.Should().Be(SyncOutcome.Unchanged, "a redelivered insert creates no duplicate and updates nothing");
        _org.GetStats().WriteRequests.Should().Be(before);
    }

    [Fact]
    public async Task Profile_moved_to_another_Account_is_relinked()
    {
        await Account("A1", new() { ["name"] = "One" }, []);
        await Account("A2", new() { ["name"] = "Two" }, []);

        // The same profile, now owned by A2
        var result = (await Accounts(("A2", Event(new() { ["name"] = "Two" }, [], profileId: "P-A1")))).Single();

        result.Outcome.Should().Be(SyncOutcome.Saved);
        _org.History("Profile__c", "P-A1")![^1].Select(kv => kv.Key).Should().Equal(["AccountId__c"], "the changed key is written");
        ((string?)_org.FindByExternalId("Profile__c", "P-A1")!["AccountId__c"]).Should().Be("A2");
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
        var rel = Upserted(_org.WriteRequests[^1], "Account_Relationship__c").Should().ContainSingle(
            "one upsert, with the Accounts' keys as text").Subject;
        ((string?)rel["AccountId__c"], (string?)rel["RelatedAccountId__c"], (string?)rel["Type__c"]).Should().Be(("A1", "A2", "Parent"));
        ((string?)_org.FindByExternalId("Account_Relationship__c", "R1")!["RelatedAccountId__c"]).Should().Be("A2");
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

    /// <summary>
    /// The read found both Accounts, but one is gone by the time Salesforce applies the write, and the org's automation
    /// rejects the relationship: with its message configured, that waits like a missing parent; without, it is the
    /// validation error it looks like.
    /// </summary>
    [Theory]
    [InlineData(true, SyncOutcome.TransientFailure, SalesforceErrors.DependencyNotSynced)]
    [InlineData(false, SyncOutcome.PermanentFailure, FakeSchema.ParentMissingCode)]
    public async Task Relationship_the_org_rejects_for_a_missing_Account_waits_if_its_message_is_configured(bool configured,
        SyncOutcome outcome, string code)
    {
        var upserter = Upserter(new SalesforceOptions { ParentMissingMessages = configured ? [FakeSchema.ParentMissingMessage] : [] });
        await Accounts(("A1", Event(new() { ["name"] = "Parent" }, [])), ("A2", Event(new() { ["name"] = "Child" }, [])));
        _org.InjectFault("Account_Relationship__c", "R1", FakeSchema.ParentMissingCode, $"{FakeSchema.ParentMissingMessage}: Account A2", 1);

        var rel = new JsonObject { ["externalId"] = "R1", ["accountExternalId"] = "A1", ["relatedAccountExternalId"] = "A2" };
        var unit = RelationshipStrategy.Map("R1", Read<AccountRelationshipEvent>(new JsonObject { ["accountRelationship"] = rel }));
        var rejected = (await upserter.WriteToSalesforceAsync([("R1", unit)], [.. RelationshipStrategy.Queries([unit])],
            AccountRelationshipInboxRow.MaxValuesPerQuery, AccountRelationshipInboxRow.MaxRecordsPerWrite, CancellationToken.None)).Single();

        (rejected.Outcome, rejected.PrimaryError?.Code).Should().Be((outcome, code));
    }

    [Fact]
    public async Task The_org_rejects_a_foreign_key_whose_parent_does_not_exist()
    {
        var client = new SalesforceRestClient(SalesforceStep.Http(new FakeSalesforceHandler(_org)));
        var profile = new Dictionary<string, object?> { ["attributes"] = new { type = "Profile__c" }, ["ExternalId__c"] = "P1", ["AccountId__c"] = "NOPE" };

        var result = (await Upsert(client, "Profile__c", "ExternalId__c", [profile], CancellationToken.None)).Single();

        result.Success.Should().BeFalse("the mock org simulates the org's automation");
        var error = result.Errors.Should().ContainSingle().Subject;
        (error.Code, error.Fields).Should().BeEquivalentTo((FakeSchema.ParentMissingCode, new[] { "AccountId__c" }));
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
        _org.History("Account_Relationship__c", "R1")![^1].Select(kv => kv.Key).Should().Equal("RelatedAccountId__c");
        ((string?)_org.FindByExternalId("Account_Relationship__c", "R1")!["RelatedAccountId__c"]).Should().Be("A3");

        (await Relationship("R1", "A1", "A3", new() { ["type"] = "Reseller", ["isActive"] = true })).Outcome
            .Should().Be(SyncOutcome.Unchanged);
    }

    // ---------- Contacts: the same engine, an Account named by its external id ----------

    [Fact]
    public async Task New_Contact_is_one_upsert_with_its_Accounts_key_and_its_record_type()
    {
        await Account("A1", new() { ["name"] = "Acme" }, []);
        var (first, last) = (_any.Create<string>()[..20], _any.Create<string>()[..20]);

        var result = await Contact("C1", "A1", new() { ["firstName"] = first, ["lastName"] = last, ["email"] = "ada@example.com" });

        result.Outcome.Should().Be(SyncOutcome.Saved);
        var record = Upserted(_org.WriteRequests[^1], "Contact").Should().ContainSingle(
            "the Account exists (the read checked), and its key is plain text").Subject;
        JsonNode.DeepEquals(record, new JsonObject
        {
            ["attributes"] = new JsonObject { ["type"] = "Contact" },
            ["ExternalId__c"] = "C1",
            ["FirstName"] = first,
            ["LastName"] = last,
            ["Email"] = "ada@example.com",
            ["AccountId__c"] = "A1",
            ["RecordTypeId"] = FakeSchema.RecordTypeIdOf("Contact", "Standard")
        }).Should().BeTrue("a new record's non-empty fields: {0}", record.ToJsonString());
        ((string?)_org.FindByExternalId("Contact", "C1")!["RecordTypeId"]).Should().Be(FakeSchema.RecordTypeIdOf("Contact", "Standard"));
    }

    [Fact]
    public async Task Unchanged_Contact_is_read_and_not_written()
    {
        await Account("A1", new() { ["name"] = "Acme" }, []);
        await Contact("C1", "A1", new() { ["lastName"] = "Lovelace", ["phone"] = "+1 555 0101" });
        var before = _org.GetStats();

        (await Contact("C1", "A1", new() { ["lastName"] = "Lovelace", ["phone"] = "+1 555 0101" })).Outcome
            .Should().Be(SyncOutcome.Unchanged, "AccountId__c already holds the Account's key");

        var after = _org.GetStats();
        (after.ApiCalls - before.ApiCalls).Should().Be(1, "the read only");
        (after.WriteRequests - before.WriteRequests).Should().Be(0);
        _org.History("Contact", "C1").Should().ContainSingle();
    }

    [Fact]
    public async Task Changed_Contact_of_an_existing_Account_is_one_upsert_with_only_the_changed_fields()
    {
        await Account("A1", new() { ["name"] = "One" }, []);
        await Account("A2", new() { ["name"] = "Two" }, []);
        await Contact("C1", "A1", new() { ["lastName"] = "Lovelace", ["phone"] = "+1 555 0101" });
        var before = _org.GetStats();

        (await Contact("C1", "A2", new() { ["lastName"] = "Lovelace" })).Outcome.Should().Be(SyncOutcome.Saved);

        var record = Upserted(_org.WriteRequests[^1], "Contact").Should().ContainSingle().Subject;
        record.Select(kv => kv.Key).Should().BeEquivalentTo(["attributes", "ExternalId__c", "Phone", "AccountId__c"],
            "the cleared Phone and the new Account's key, nothing else");
        ((string?)record["AccountId__c"]).Should().Be("A2");
        var after = _org.GetStats();
        (after.ApiCalls - before.ApiCalls).Should().Be(2, "one read, one write");
        (after.UpsertRequests - before.UpsertRequests).Should().Be(1);
        _org.All("Contact").Should().ContainSingle("matched by its external id");
        _org.FindByExternalId("Contact", "C1")!["Phone"].Should().BeNull();
    }

    [Fact]
    public async Task Contact_whose_Account_is_not_in_Salesforce_waits_for_it()
    {
        var early = await Contact("C1", "A1", new() { ["lastName"] = "Early" });

        early.Outcome.Should().Be(SyncOutcome.TransientFailure, "the read found no Account with that external id");
        (early.PrimaryError?.Code).Should().Be(SalesforceErrors.DependencyNotSynced);
        _org.WriteRequests.Should().BeEmpty("nothing is sent for it");

        await Account("A1", new() { ["name"] = "Acme" }, []);
        (await Contact("C1", "A1", new() { ["lastName"] = "Early" })).Outcome.Should().Be(SyncOutcome.Saved);
        ((string?)_org.FindByExternalId("Contact", "C1")!["AccountId__c"]).Should().Be("A1");
    }

    [Fact]
    public async Task A_contact_value_Salesforce_would_reject_fails_only_its_own_entity_before_the_call()
    {
        await Account("A1", new() { ["name"] = "Acme" }, []);
        await Contact("WARM", "A1", new() { ["lastName"] = "Warm" });

        var results = (await Contacts(("NONAME", "A1", new() { ["firstName"] = "No last name" }),
            ("LONG", "A1", new() { ["lastName"] = "Long", ["phone"] = new string('1', 41) }),
            ("GOOD", "A1", new() { ["lastName"] = "Good" }))).ToDictionary(r => r.Key);

        (results["NONAME"].Outcome, results["NONAME"].PrimaryError?.Code).Should().Be((SyncOutcome.PermanentFailure, "REQUIRED_FIELD_MISSING"));
        (results["LONG"].Outcome, results["LONG"].PrimaryError?.Code).Should().Be((SyncOutcome.PermanentFailure, "STRING_TOO_LONG"));
        results["GOOD"].Outcome.Should().Be(SyncOutcome.Saved);
        Upserted(_org.WriteRequests[^1], "Contact").Select(r => (string?)r["ExternalId__c"]).Should().Equal("GOOD");
    }

    [Theory]
    [InlineData("FIELD_CUSTOM_VALIDATION_EXCEPTION", SyncOutcome.PermanentFailure)]
    [InlineData("UNABLE_TO_LOCK_ROW", SyncOutcome.TransientFailure)]
    public async Task A_contact_Salesforce_rejects_costs_only_its_own_entity(string code, SyncOutcome outcome)
    {
        await Account("A1", new() { ["name"] = "Acme" }, []);
        _org.InjectFault("Contact", "C2", code, "rejected", 1);

        var results = (await Contacts(("C1", "A1", new() { ["lastName"] = "One" }), ("C2", "A1", new() { ["lastName"] = "Two" }),
            ("C3", "A1", new() { ["lastName"] = "Three" }))).ToDictionary(r => r.Key);

        (results["C2"].Outcome, results["C2"].PrimaryError?.Code, results["C2"].PrimaryError?.ReferenceId).Should().Be((outcome, code, "contact"));
        results["C1"].Outcome.Should().Be(SyncOutcome.Saved);
        results["C3"].Outcome.Should().Be(SyncOutcome.Saved);
    }

    [Fact]
    public async Task A_contact_upsert_whose_answer_is_lost_has_an_unknown_outcome()
    {
        await Account("A1", new() { ["name"] = "Acme" }, []);
        _org.LoseResponsesFor("Contact", "C1", 1);

        var upsert = () => Contact("C1", "A1", new() { ["lastName"] = "Lost" });

        await upsert.Should().ThrowAsync<UnknownOutcomeException>("an older upsert landing late could overwrite a newer one");
        _org.FindByExternalId("Contact", "C1").Should().NotBeNull("the org applied it; only the answer was lost");
    }

    [Fact]
    public async Task The_upsert_call_answers_per_record_in_order_with_created_or_updated()
    {
        await Account("A1", new() { ["name"] = "Acme" }, []);
        var client = new SalesforceRestClient(SalesforceStep.Http(new FakeSalesforceHandler(_org)));
        static IReadOnlyDictionary<string, object?> Record(string key, string? lastName)
        {
            return new Dictionary<string, object?>
            {
                ["attributes"] = new { type = "Contact" },
                ["ExternalId__c"] = key,
                ["LastName"] = lastName,
                ["AccountId__c"] = "A1"
            };
        }

        var first = await Upsert(client, "Contact", "ExternalId__c", [Record("C1", "Doe"), Record("C2", null)], CancellationToken.None);
        var second = await Upsert(client, "Contact", "ExternalId__c", [Record("C1", "Doe")], CancellationToken.None);

        first.Select(r => (r.Success, r.Created)).Should().Equal((true, true), (false, false));
        first[0].Id.Should().Be(IdOf("Contact", "C1"));
        var error = first[1].Errors.Should().ContainSingle().Subject;
        (error.Code, error.Message).Should().Be(("REQUIRED_FIELD_MISSING", "Required fields are missing: [LastName]"));
        second.Single().Should().Match<RecordResult>(r => r.Success && r.Created == false && r.Id == IdOf("Contact", "C1"));
        var unknownField = () => Upsert(client, "Contact", "No_Such__c", [Record("C1", "Doe")], CancellationToken.None);
        (await unknownField.Should().ThrowAsync<SalesforceResponseException>()).Which.StatusCode.Should().Be(System.Net.HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task A_contact_record_type_Salesforce_rejects_is_retried_and_describes_again()
    {
        await Account("A1", new() { ["name"] = "Acme" }, []);
        await Contact("WARM", "A1", new() { ["lastName"] = "Warm" });
        var before = _org.GetStats();
        _org.InjectFault("Contact", "C1", "INVALID_CROSS_REFERENCE_KEY", "Record Type ID: this ID value isn't valid for the user: 012000000000001AAA", 1);

        var rejected = await Contact("C1", "A1", new() { ["lastName"] = "One" });

        (rejected.Outcome, rejected.PrimaryError?.Code).Should().Be((SyncOutcome.TransientFailure, SalesforceErrors.InvalidRecordType));
        (await Contact("C1", "A1", new() { ["lastName"] = "One" })).Outcome.Should().Be(SyncOutcome.Saved);
        (_org.GetStats().Describes - before.Describes).Should().BeGreaterThan(0, "the cached describe was invalidated");
    }

    // ---------- Helpers ----------

    private async Task<UpsertResult> Account(string key, Dictionary<string, object?> account, Dictionary<string, object?> profile)
    {
        return (await Accounts((key, Event(account, profile)))).Single();
    }

    /// <summary>Profiles are keyed "P-{account}" unless the event says otherwise.</summary>
    private Task<List<UpsertResult>> Accounts(params (string Key, JsonObject Event)[] items)
    {
        return Upsert(AccountStrategy, [.. items.Select(i =>
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

    private async Task<UpsertResult> Contact(string key, string account, Dictionary<string, object?> fields)
    {
        return (await Contacts((key, account, fields))).Single();
    }

    private Task<List<UpsertResult>> Contacts(params (string Key, string Account, Dictionary<string, object?> Fields)[] items)
    {
        return Upsert(ContactStrategy, [.. items.Select(i =>
        {
            var contact = Body(i.Fields);
            contact["externalId"] = i.Key;
            contact["accountExternalId"] = i.Account;
            return (i.Key, Read<ContactEvent>(new JsonObject { ["contact"] = contact }));
        })]);
    }

    /// <summary>What the engine does with a strategy, minus the inbox: map, then query and upsert.</summary>
    private Task<List<UpsertResult>> Upsert<TEvent>(UpsertStrategy<TEvent> strategy, params (string Key, TEvent Event)[] items)
    {
        var units = items.Select(i => (i.Key, strategy.Map(i.Key, i.Event))).ToList();
        return _upserter.WriteToSalesforceAsync(units, [.. strategy.Queries([.. units.Select(u => u.Item2)])],
            AccountInboxRow.MaxValuesPerQuery, AccountInboxRow.MaxRecordsPerWrite, CancellationToken.None);
    }

    private static JsonObject WithoutProfile(string key, string name)
    {
        var e = Event(new() { ["name"] = name }, []);
        e["account"]!.AsObject().Remove("profile");
        e["account"]!["externalId"] = key;
        return e;
    }

    /// <summary>
    /// The records of the sObject Collections upsert of <paramref name="sobject"/> by ExternalId__c in a recorded write:
    /// the write itself, or its subrequest if it is a Composite request.
    /// </summary>
    private static List<JsonObject> Upserted(JsonObject write, string sobject)
    {
        var url = $"/services/data/v62.0/composite/sobjects/{sobject}/ExternalId__c";
        var upsert = write["compositeRequest"] is JsonArray subrequests
            ? subrequests.Select(s => s!.AsObject()).Should().ContainSingle(s => (string?)s["url"] == url).Subject
            : write;
        ((string?)upsert["method"], (string?)upsert["url"]).Should().Be(("PATCH", url), $"the write is an upsert of {sobject}");
        upsert["body"]!["allOrNone"]!.GetValue<bool>().Should().BeFalse("allOrNone is sent explicitly");
        var records = Records(upsert);
        records.Should().OnlyContain(r => (string?)r["attributes"]!["type"] == sobject).And.OnlyContain(r => !r.ContainsKey("id"));
        return records;
    }

    /// <summary>One upsert through the client, a call of its own: its per-record results.</summary>
    private static async Task<IReadOnlyList<RecordResult>> Upsert(SalesforceRestClient client, string sobject, string externalIdField,
        IReadOnlyList<IReadOnlyDictionary<string, object?>> records, CancellationToken ct)
    {
        return (await client.UpsertAsync([new CollectionRequest(sobject, sobject, externalIdField, records)], ct)).Single().Records!;
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
        return new([new SObjectRecord("acct", "Account", "ExternalId__c", key, fields)]);
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