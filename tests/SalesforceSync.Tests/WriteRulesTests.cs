using System.Text.Json;
using Microsoft.Extensions.Options;
using SalesforceSync.Salesforce;
using SalesforceSync.Sync;
using SalesforceSync.Sync.Upsert;

namespace SalesforceSync.Tests;

/// <summary>
/// The write path's rules on their own, from hand-built Salesforce state and results: <see cref="WritePlanner.Changes"/>,
/// <see cref="WritePlanner"/> and <see cref="WriteResultInterpreter"/>. No org, no HTTP.
/// </summary>
public sealed class WriteRulesTests
{
    private const string AccountId = "001000000000001AAA";
    private const string ProfileId = "a00000000000001AAA";

    private static readonly Dictionary<string, SObjectSchema> Schemas = new(StringComparer.OrdinalIgnoreCase)
    {
        ["Account"] = new("Account",
        [
            Field("Id", "id", createable: false, updateable: false),
            Field("External_Id__c", "string"),
            Field("Name", "string"),
            Field("Active__c", "boolean", nillable: false),
            Field("Locked__c", "string", updateable: false)
        ]),
        ["Profile__c"] = new("Profile__c",
        [
            Field("Id", "id", createable: false, updateable: false),
            Field("External_Id__c", "string"),
            Field("Tier__c", "string"),
            Field("Account__c", "reference")
        ])
    };

    // ---------- WritePlanner.Changes ----------

    public static TheoryData<string, object?, string, string[]> Diffs()
    {
        return new()
    {
        { "equal", "Acme", /*lang=json,strict*/ """{"Name":"Acme","Active__c":false}""", [] },
        { "changed", "Globex", /*lang=json,strict*/ """{"Name":"Acme","Active__c":false}""", ["Name"] },
        { "cleared to null", null, /*lang=json,strict*/ """{"Name":"Acme","Active__c":false}""", ["Name"] },
        { "empty and null stay equal", null, /*lang=json,strict*/ """{"Name":null,"Active__c":false}""", [] },
        { "a checkbox that is null is false", "Acme", /*lang=json,strict*/ """{"Name":"Acme","Active__c":true}""", ["Active__c"] }
    };
    }

    [Theory, MemberData(nameof(Diffs))]
    public void Only_fields_whose_normalized_value_differs_are_sent(string what, object? name, string stored, string[] expected)
    {
        var record = Normalized(Account("A1", name, active: null)).Records[0];
        var problems = new List<SalesforceError>();

        var changes = WritePlanner.Changes(record, Current(stored), problems);

        changes.Keys.Should().BeEquivalentTo(expected, what);
        problems.Should().BeEmpty();
    }

    [Fact]
    public void A_new_record_gets_its_non_empty_fields_and_a_checkbox_is_sent_as_false()
    {
        var record = Normalized(Account("A1", "Acme", active: null)).Records[0];

        var changes = WritePlanner.Changes(record, null, []);

        changes.Should().BeEquivalentTo(new Dictionary<string, object?> { ["Name"] = "Acme", ["Active__c"] = false });
    }

    [Fact]
    public void A_field_Salesforce_does_not_let_us_update_is_a_problem_only_if_it_differs()
    {
        var record = Normalized(Account("A1", "Acme", locked: "new")).Records[0];
        var problems = new List<SalesforceError>();

        WritePlanner.Changes(record, Current(/*lang=json,strict*/ """{"Name":"Acme","Active__c":false,"Locked__c":"new"}"""), problems);
        problems.Should().BeEmpty();

        WritePlanner.Changes(record, Current(/*lang=json,strict*/ """{"Name":"Acme","Active__c":false,"Locked__c":"old"}"""), problems);
        problems.Should().ContainSingle().Which.Code.Should().Be("INVALID_FIELD_FOR_INSERT_UPDATE");
    }

    // ---------- WritePlanner ----------

    [Fact]
    public void Create_update_and_unchanged_are_decided_by_what_Salesforce_has()
    {
        var current = Rows(("Account", $$"""{"Id":"{{AccountId}}","External_Id__c":"OLD","Name":"Acme","Active__c":false}"""),
            ("Account", /*lang=json,strict*/ """{"Id":"001000000000002AAA","External_Id__c":"SAME","Name":"Same","Active__c":false}"""));

        var plan = WritePlanner.Plan(
            [Normalized(Account("NEW", "New")), Normalized(Account("OLD", "Changed")), Normalized(Account("SAME", "Same"))],
            current);

        plan.NotSent.Should().ContainSingle().Which.Should().Match<UpsertResult>(r => r.Key == "SAME" && r.Outcome == SyncOutcome.Unchanged);
        var bodies = plan.Requests.Should().ContainSingle().Subject.BuildRequests();
        bodies.Select(b => (b.ReferenceId, b.Create)).Should().Equal(("Account_create", true), ("Account_update", false));
        bodies[0].Records.Single()["External_Id__c"].Should().Be("NEW", "a create carries its external id");
        bodies[1].Records.Single().Should().ContainKey("id").WhoseValue.Should().Be(AccountId);
        bodies[1].Records.Single().Keys.Should().BeEquivalentTo(["attributes", "id", "Name"], "an update sends only what differs");
    }

    [Theory]
    [InlineData("new parent, by reference", "", "@{Account_create[0].id}")]
    [InlineData("existing parent, literal Id", AccountId, AccountId)]
    public void A_child_is_linked_to_its_parent_by_reference_or_Id(string what, string existingAccountId, string value)
    {
        var current = existingAccountId == ""
            ? CurrentRecords.None
            : Rows(("Account", $$"""{"Id":"{{existingAccountId}}","External_Id__c":"A1","Name":"Acme","Active__c":false}"""));

        var plan = WritePlanner.Plan([Normalized(AccountWithProfile("A1"))], current);

        var profile = plan.Requests.Single().BuildRequests().Single(r => r.ReferenceId.StartsWith("Profile__c")).Records.Single();
        profile.Should().ContainKey("Account__c", what).WhoseValue.Should().Be(value);
    }

    [Fact]
    public void A_child_already_linked_to_its_parent_is_not_written()
    {
        var current = Rows(
            ("Account", $$"""{"Id":"{{AccountId}}","External_Id__c":"A1","Name":"Acme","Active__c":false}"""),
            ("Profile__c", $$"""{"Id":"{{ProfileId}}","External_Id__c":"P-A1","Tier__c":"Gold","Account__c":"{{AccountId[..15]}}"}"""));

        var plan = WritePlanner.Plan([Normalized(AccountWithProfile("A1"))], current);

        plan.Requests.Should().BeEmpty("a 15-character Id is the same record as its 18-character form");
        plan.NotSent.Should().ContainSingle().Which.Outcome.Should().Be(SyncOutcome.Unchanged);
    }

    [Fact]
    public void A_lookup_target_that_is_neither_in_the_unit_nor_in_Salesforce_waits()
    {
        var orphan = new UpsertUnit([Profile("P1", "Gold", "MISSING")]);

        var plan = WritePlanner.Plan([Normalized(orphan)], CurrentRecords.None);

        var result = plan.NotSent.Should().ContainSingle().Subject;
        (result.Outcome, result.PrimaryError!.Code).Should().Be((SyncOutcome.TransientFailure, SalesforceErrors.DependencyNotSynced));
        plan.Requests.Should().BeEmpty();
    }

    [Fact]
    public void A_field_that_must_change_but_is_not_updateable_fails_the_unit()
    {
        var current = Rows(("Account", $$"""{"Id":"{{AccountId}}","External_Id__c":"A1","Name":"Acme","Active__c":false,"Locked__c":"old"}"""));

        var plan = WritePlanner.Plan([Normalized(Account("A1", "Acme", locked: "new"))], current);

        var result = plan.NotSent.Should().ContainSingle().Subject;
        (result.Outcome, result.PrimaryError!.Code).Should().Be((SyncOutcome.PermanentFailure, "INVALID_FIELD_FOR_INSERT_UPDATE"));
    }

    // ---------- CompositeWrite ----------

    [Fact]
    public void Building_a_request_twice_gives_the_same_request()
    {
        var plan = WritePlanner.Plan([Normalized(AccountWithProfile("A1")), Normalized(AccountWithProfile("A2"))], CurrentRecords.None);
        var request = plan.Requests.Single();

        JsonSerializer.Serialize(request.BuildRequests()).Should().Be(JsonSerializer.Serialize(request.BuildRequests()));
        request.Collections.Select(c => c.ReferenceId).Should().Equal("Account_create", "Profile__c_create_ref");
    }

    // ---------- WriteResultInterpreter ----------

    [Fact]
    public void Every_record_saved_means_the_unit_is_saved()
    {
        var request = Request(AccountWithProfile("A1"));

        var results = Interpret(request, Ok("Account_create"), Ok("Profile__c_create_ref"));

        results.Single().Outcome.Should().Be(SyncOutcome.Saved);
    }

    public static TheoryData<string, CollectionResult[], SyncOutcome, string> SingleRecordResults()
    {
        return new()
    {
        { "no answer for the subrequest", [], SyncOutcome.TransientFailure, "UNKNOWN_ERROR" },
        { "the subrequest failed as a whole, even with a permanent code",
            [new CollectionResult("Account_create", null, [new SalesforceError("Account_create", "INVALID_FIELD", "x")])],
            SyncOutcome.TransientFailure, "INVALID_FIELD" },
        { "fewer results than records", [new CollectionResult("Account_create", [], [])], SyncOutcome.TransientFailure, "UNKNOWN_ERROR" },
        { "a record failed without details", [new CollectionResult("Account_create", [new RecordResult(false, null, [])], [])],
            SyncOutcome.TransientFailure, "UNKNOWN_ERROR" },
        { "a validation rule", [Failed("Account_create", "FIELD_CUSTOM_VALIDATION_EXCEPTION")], SyncOutcome.PermanentFailure, "FIELD_CUSTOM_VALIDATION_EXCEPTION" },
        { "a create that lost a race", [Failed("Account_create", "DUPLICATE_VALUE")], SyncOutcome.TransientFailure, "DUPLICATE_VALUE" },
        { "only skipped, nobody says why", [Failed("Account_create", "PROCESSING_HALTED")], SyncOutcome.TransientFailure, "UNKNOWN_ERROR" }
    };
    }

    [Theory, MemberData(nameof(SingleRecordResults))]
    public void A_records_result_decides_its_units_outcome(string what, CollectionResult[] answers, SyncOutcome outcome, string code)
    {
        var result = Interpret(Request(Account("A1", "Acme")), answers).Single();

        (result.Outcome, result.PrimaryError?.Code).Should().Be((outcome, code), what);
    }

    [Theory]
    [InlineData(new[] { "UNABLE_TO_LOCK_ROW", "PROCESSING_HALTED" }, SyncOutcome.TransientFailure, "UNABLE_TO_LOCK_ROW")]
    [InlineData(new[] { "REQUIRED_FIELD_MISSING", "PROCESSING_HALTED" }, SyncOutcome.PermanentFailure, "REQUIRED_FIELD_MISSING")]
    [InlineData(new[] { "UNABLE_TO_LOCK_ROW", "REQUIRED_FIELD_MISSING" }, SyncOutcome.PermanentFailure, "REQUIRED_FIELD_MISSING")]
    [InlineData(new[] { "SOMETHING_NEW" }, SyncOutcome.TransientFailure, "SOMETHING_NEW")]
    public void A_records_own_errors_are_permanent_if_any_is_and_skipping_is_not_one_of_them(string[] codes, SyncOutcome outcome, string code)
    {
        var result = Interpret(Request(Account("A1", "Acme")), Failed("Account_create", codes)).Single();

        (result.Outcome, result.PrimaryError?.Code).Should().Be((outcome, code));
    }

    [Fact]
    public void A_record_only_skipped_because_of_another_records_failure_is_not_attempted()
    {
        var results = Interpret(Request(Account("A1", "Acme"), Account("A2", "Acme")), new CollectionResult("Account_create",
        [
            new RecordResult(false, null, [new SalesforceError(null, "UNABLE_TO_LOCK_ROW", "locked")]),
            new RecordResult(false, null, [new SalesforceError(null, "PROCESSING_HALTED", "skipped")])
        ], [])).ToDictionary(r => r.Key);

        results["A1"].Outcome.Should().Be(SyncOutcome.TransientFailure);
        results["A2"].Outcome.Should().Be(SyncOutcome.NotAttempted);
    }

    [Fact]
    public void Permanent_codes_are_configurable()
    {
        var options = new SalesforceOptions { PermanentErrorCodes = ["UNABLE_TO_LOCK_ROW"] };

        Interpret(options, Request(Account("A1", "Acme")), Failed("Account_create", "UNABLE_TO_LOCK_ROW")).Single().Outcome
            .Should().Be(SyncOutcome.PermanentFailure);
        Interpret(options, Request(Account("A1", "Acme")), Failed("Account_create", "REQUIRED_FIELD_MISSING")).Single().Outcome
            .Should().Be(SyncOutcome.TransientFailure, "a code is permanent only when listed, and setting the list replaces the defaults");
    }

    [Fact]
    public void A_record_deleted_between_the_read_and_the_update_is_retried_not_dead_lettered()
    {
        var current = Rows(("Account", $$"""{"Id":"{{AccountId}}","External_Id__c":"A1","Name":"Old","Active__c":false}"""));
        var request = WritePlanner.Plan([Normalized(Account("A1", "New"))], current).Requests.Single();

        var result = Interpret(request, Failed("Account_update", "ENTITY_IS_DELETED")).Single();

        result.Outcome.Should().Be(SyncOutcome.TransientFailure, "the next read finds it gone and creates it");
    }

    [Fact]
    public void A_failed_parent_decides_its_childs_unit_and_its_broken_reference_costs_the_other_children_nothing()
    {
        var request = Request(AccountWithProfile("A1"), AccountWithProfile("A2"));

        var results = Interpret(request,
            new CollectionResult("Account_create",
            [
                new RecordResult(false, null, [new SalesforceError(null, "FIELD_CUSTOM_VALIDATION_EXCEPTION", "bad")]),
                new RecordResult(true, AccountId, [])
            ], []),
            // A reference without a value fails the whole subrequest of children that use references
            new CollectionResult("Profile__c_create_ref", null, [new SalesforceError("Profile__c_create_ref", "PROCESSING_HALTED", "No value for Account_create[0].id")])
        ).ToDictionary(r => r.Key);

        results["A1"].Outcome.Should().Be(SyncOutcome.PermanentFailure);
        results["A1"].Errors.Should().OnlyContain(e => e.ReferenceId == "acct", "the child's own error is the parent's fault");
        results["A2"].Outcome.Should().Be(SyncOutcome.NotAttempted, "its Account is in; its Profile went down with A1's reference");
    }

    [Fact]
    public void Salesforce_rejecting_a_lookup_target_that_is_gone_means_wait_for_it()
    {
        var current = Rows(("Account", $$"""{"Id":"{{AccountId}}","External_Id__c":"A1","Name":"Acme","Active__c":false}"""));
        var request = WritePlanner.Plan([Normalized(new UpsertUnit([Profile("P1", "Gold", "A1")]))], current).Requests.Single();

        var result = Interpret(request, new CollectionResult("Profile__c_create",
            [new RecordResult(false, null, [new SalesforceError(null, "INVALID_CROSS_REFERENCE_KEY", "invalid cross reference id", ["Account__c"])])], [])).Single();

        (result.Outcome, result.PrimaryError!.Code).Should().Be((SyncOutcome.TransientFailure, SalesforceErrors.DependencyNotSynced));
    }

    // ---------- Helpers ----------

    private static SalesforceField Field(string name, string type, bool createable = true, bool updateable = true, bool nillable = true)
    {
        return new(name, type, 255, 0, 0, createable, updateable, nillable);
    }

    private static UpsertUnit Account(string key, object? name, bool? active = false, string? locked = null)
    {
        var fields = new Dictionary<string, object?> { ["Name"] = name, ["Active__c"] = active };
        if (locked is not null)
        {
            fields["Locked__c"] = locked;
        }

        return new UpsertUnit([new SObjectRecord("acct", "Account", "External_Id__c", key, fields)]);
    }

    private static SObjectRecord Profile(string key, string tier, string accountKey)
    {
        return new("prof", "Profile__c", "External_Id__c", key, new Dictionary<string, object?> { ["Tier__c"] = tier },
                [new SObjectLookup("Account__c", "Account", "External_Id__c", accountKey)]);
    }

    private static UpsertUnit AccountWithProfile(string key)
    {
        return new([Account(key, "Acme").Records[0], Profile($"P-{key}", "Gold", key)]);
    }

    private static NormalizedUnit Normalized(UpsertUnit unit)
    {
        var normalized = RecordNormalizer.Normalize(unit.Records[0].ExternalId, unit, Schemas, out var problems);
        problems.Should().BeEmpty();
        return normalized;
    }

    private static CurrentRecord Current(string json)
    {
        return new(JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(json)!
        .ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.OrdinalIgnoreCase));
    }

    private static CurrentRecords Rows(params (string SObject, string Json)[] rows)
    {
        return new(rows.Select(r => (r.SObject, Current(r.Json))));
    }

    /// <summary>The one request for these new units.</summary>
    private static CompositeWrite Request(params UpsertUnit[] units)
    {
        return WritePlanner.Plan([.. units.Select(Normalized)], CurrentRecords.None).Requests.Single();
    }

    private static List<UpsertResult> Interpret(CompositeWrite request, params CollectionResult[] answers)
    {
        return Interpret(new SalesforceOptions(), request, answers);
    }

    private static List<UpsertResult> Interpret(SalesforceOptions options, CompositeWrite request, params CollectionResult[] answers)
    {
        return new WriteResultInterpreter(new SalesforceErrors(Options.Create(options))).Interpret(request, answers);
    }

    private static CollectionResult Ok(string referenceId)
    {
        return new(referenceId, [new RecordResult(true, AccountId, [])], []);
    }

    private static CollectionResult Failed(string referenceId, params string[] codes)
    {
        return new(referenceId, [new RecordResult(false, null, [.. codes.Select(code => new SalesforceError(null, code, "failed"))])], []);
    }
}