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
    private const string ContactId = "003000000000001AAA";

    private static readonly Dictionary<string, SObjectSchema> Schemas = new(StringComparer.OrdinalIgnoreCase)
    {
        ["Account"] = new("Account",
        [
            Field("Id", "id", createable: false, updateable: false),
            Field("ExternalId__c", "string"),
            Field("Name", "string"),
            Field("Active__c", "boolean", nillable: false),
            Field("Locked__c", "string", updateable: false)
        ]),
        // A foreign key: AccountId__c holds the Account's external id
        ["Profile__c"] = new("Profile__c",
        [
            Field("Id", "id", createable: false, updateable: false),
            Field("ExternalId__c", "string"),
            Field("Tier__c", "string"),
            Field("AccountId__c", "string")
        ]),
        // A lookup: AccountId holds the Account's Salesforce Id
        ["Contact"] = new("Contact",
        [
            Field("Id", "id", createable: false, updateable: false),
            Field("ExternalId__c", "string"),
            Field("LastName", "string"),
            Field("AccountId", "reference")
        ]),
        // Objects the sync has no strategy for: the write rule knows no object by name
        ["Widget__c"] = new("Widget__c", [Field("Id", "id", createable: false, updateable: false), Field("Key__c", "string"), Field("Label__c", "string")]),
        ["Gadget__c"] = new("Gadget__c", [Field("Id", "id", createable: false, updateable: false), Field("Key__c", "string"), Field("Widget__c", "string")])
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
        var current = Rows(("Account", $$"""{"Id":"{{AccountId}}","ExternalId__c":"OLD","Name":"Acme","Active__c":false}"""),
            ("Account", /*lang=json,strict*/ """{"Id":"001000000000002AAA","ExternalId__c":"SAME","Name":"Same","Active__c":false}"""));

        var plan = WritePlanner.Plan(
            [Normalized(Account("NEW", "New")), Normalized(Account("OLD", "Changed")), Normalized(Account("SAME", "Same"))],
            current);

        plan.NotSent.Should().ContainSingle().Which.Should().Match<UpsertResult>(r => r.Key == "SAME" && r.Outcome == SyncOutcome.Unchanged);
        var upsert = plan.Requests.Should().ContainSingle().Subject.BuildRequests().Should().ContainSingle().Subject;
        upsert.ReferenceId.Should().Be("Account");
        upsert.Records.Select(r => r["ExternalId__c"]).Should().Equal("NEW", "OLD");
        upsert.Records[0].Keys.Should().BeEquivalentTo(["attributes", "ExternalId__c", "Name", "Active__c"], "a create sends every field");
        upsert.Records[1].Keys.Should().BeEquivalentTo(["attributes", "ExternalId__c", "Name"], "an update sends only what differs, never an Id");
    }

    [Theory]
    [InlineData("new parent, by reference", "", "@{Account[0].id}")]
    [InlineData("existing parent, literal Id", AccountId, AccountId)]
    public void A_child_is_linked_to_its_parent_by_reference_or_Id(string what, string existingAccountId, string value)
    {
        var current = existingAccountId == ""
            ? CurrentRecords.None
            : Rows(("Account", $$"""{"Id":"{{existingAccountId}}","ExternalId__c":"A1","Name":"Acme","Active__c":false}"""));

        var plan = WritePlanner.Plan([Normalized(AccountWithContact("A1"))], current);

        var contact = plan.Requests.Single().BuildRequests().Single(r => r.ReferenceId.StartsWith("Contact")).Records.Single();
        contact.Should().ContainKey("AccountId", what).WhoseValue.Should().Be(value);
    }

    [Fact]
    public void A_child_already_linked_to_its_parent_is_not_written()
    {
        var current = Rows(
            ("Account", $$"""{"Id":"{{AccountId}}","ExternalId__c":"A1","Name":"Acme","Active__c":false}"""),
            ("Contact", $$"""{"Id":"{{ContactId}}","ExternalId__c":"C-A1","LastName":"Doe","AccountId":"{{AccountId[..15]}}"}"""));

        var plan = WritePlanner.Plan([Normalized(AccountWithContact("A1"))], current);

        plan.Requests.Should().BeEmpty("a 15-character Id is the same record as its 18-character form");
        plan.NotSent.Should().ContainSingle().Which.Outcome.Should().Be(SyncOutcome.Unchanged);
    }

    [Theory]
    [InlineData("a lookup")]
    [InlineData("a foreign key")]
    public void A_parent_that_is_neither_in_the_unit_nor_in_Salesforce_waits_and_nothing_is_sent(string what)
    {
        var orphan = new UpsertUnit([what == "a lookup" ? Contact("C1", "MISSING") : Profile("P1", "Gold", "MISSING")]);

        var plan = WritePlanner.Plan([Normalized(orphan)], CurrentRecords.None);

        var result = plan.NotSent.Should().ContainSingle().Subject;
        (result.Outcome, result.PrimaryError!.Code).Should().Be((SyncOutcome.TransientFailure, SalesforceErrors.DependencyNotSynced), what);
        result.PrimaryError.Message.Should().Contain("Account ExternalId__c=MISSING is not in Salesforce yet");
        plan.Requests.Should().BeEmpty();
    }

    // ---------- Foreign keys ----------

    [Fact]
    public void A_new_Account_and_its_new_Profile_are_one_request_of_two_upserts_Account_first_with_the_key_as_text()
    {
        var request = Request(AccountWithProfile("A1"), AccountWithProfile("A2"));

        var upserts = request.BuildRequests();

        upserts.Select(u => (u.ReferenceId, u.SObject, u.ExternalIdField)).Should().Equal(
            ("Account", "Account", "ExternalId__c"), ("Profile__c", "Profile__c", "ExternalId__c"));
        JsonSerializer.Serialize(upserts[1].Records).Should().Be(JsonSerializer.Serialize(new object[]
        {
            new Dictionary<string, object?> { ["attributes"] = new { type = "Profile__c" }, ["ExternalId__c"] = "P-A1", ["Tier__c"] = "Gold", ["AccountId__c"] = "A1" },
            new Dictionary<string, object?> { ["attributes"] = new { type = "Profile__c" }, ["ExternalId__c"] = "P-A2", ["Tier__c"] = "Gold", ["AccountId__c"] = "A2" }
        }), "the Account's key is written as the text it is, no reference");
        request.Collections[1].Writes.Select(w => w.Parents.Single()).Should().Equal(request.Collections[0].Writes,
            "each Profile follows its own new Account");
    }

    [Fact]
    public void Writes_of_one_object_are_one_upsert_and_writes_of_several_objects_one_request_of_an_upsert_each_whatever_the_objects()
    {
        var widget = Widget("W1");
        var gadget = new SObjectRecord("gadget", "Gadget__c", "Key__c", "G1", new Dictionary<string, object?> { ["Widget__c"] = "W1" },
            ForeignKeys: [new SObjectLookup("Widget__c", "Widget__c", "Key__c", "W1")]);

        var widgetsOnly = WritePlanner.Plan([Normalized(new UpsertUnit([widget])), Normalized(new UpsertUnit([Widget("W2")]))],
            CurrentRecords.None).Requests.Should().ContainSingle().Subject;
        var both = WritePlanner.Plan([Normalized(new UpsertUnit([widget, gadget]))], CurrentRecords.None).Requests.Should().ContainSingle().Subject;

        widgetsOnly.BuildRequests().Select(u => (u.ReferenceId, u.Url, u.Records.Count)).Should().Equal(
            [("Widget__c", "/services/data/v62.0/composite/sobjects/Widget__c/Key__c", 2)], "one object: one upsert, a call of its own");
        both.BuildRequests().Select(u => (u.ReferenceId, u.Url)).Should().Equal(
            [("Widget__c", "/services/data/v62.0/composite/sobjects/Widget__c/Key__c"), ("Gadget__c", "/services/data/v62.0/composite/sobjects/Gadget__c/Key__c")],
            "two objects: one request (Composite) of an upsert each, the parent first by its foreign key");

        static SObjectRecord Widget(string key)
        {
            return new("widget", "Widget__c", "Key__c", key, new Dictionary<string, object?> { ["Label__c"] = "w" });
        }
    }

    [Theory]
    [InlineData("A1", false)]
    [InlineData("A2", true)]
    public void A_foreign_key_is_written_only_when_its_text_differs(string storedKey, bool written)
    {
        var current = Rows(
            ("Account", $$"""{"Id":"{{AccountId}}","ExternalId__c":"A1","Name":"Acme","Active__c":false}"""),
            ("Account", /*lang=json,strict*/ """{"Id":"001000000000002AAA","ExternalId__c":"A2","Name":"Acme","Active__c":false}"""),
            ("Profile__c", $$"""{"Id":"a00000000000001AAA","ExternalId__c":"P-A1","Tier__c":"Gold","AccountId__c":"{{storedKey}}"}"""));

        var plan = WritePlanner.Plan([Normalized(AccountWithProfile("A1"))], current);

        if (written)
        {
            var upsert = plan.Requests.Single().BuildRequests().Should().ContainSingle().Subject;
            upsert.Records.Single().Keys.Should().BeEquivalentTo(["attributes", "ExternalId__c", "AccountId__c"]);
            upsert.Records.Single()["AccountId__c"].Should().Be("A1");
        }
        else
        {
            plan.Requests.Should().BeEmpty();
            plan.NotSent.Single().Outcome.Should().Be(SyncOutcome.Unchanged);
        }
    }

    [Fact]
    public void A_child_whose_new_parent_failed_in_an_earlier_upsert_is_sent_the_org_decides_and_the_parent_decides_the_unit()
    {
        var request = Request(AccountWithProfile("A1"), AccountWithProfile("A2"));
        request.BuildRequests()[1].Records.Should().HaveCount(2, "both Profiles are in the Composite request, A1's too");

        var results = Interpret(request,
            new CollectionResult("Account",
            [
                new RecordResult(false, null, [new SalesforceError(null, "FIELD_CUSTOM_VALIDATION_EXCEPTION", "bad")]),
                new RecordResult(true, AccountId, [], true)
            ], []),
            new CollectionResult("Profile__c",
            [
                new RecordResult(false, null, [new SalesforceError(null, "FIELD_CUSTOM_VALIDATION_EXCEPTION", "Parent Account A1 does not exist")]),
                new RecordResult(true, "a00000000000002AAA", [], true)
            ], [])).ToDictionary(r => r.Key);

        (results["A1"].Outcome, results["A1"].Errors.Single().ReferenceId).Should().Be((SyncOutcome.PermanentFailure, "acct"),
            "the org rejecting the Profile is the Account's fault");
        results["A2"].Outcome.Should().Be(SyncOutcome.Saved);
    }

    [Theory]
    [InlineData(new string[0], SyncOutcome.PermanentFailure, "FIELD_CUSTOM_VALIDATION_EXCEPTION")]
    [InlineData(new[] { "other", "PARENT ACCOUNT" }, SyncOutcome.TransientFailure, SalesforceErrors.DependencyNotSynced)]
    public void The_orgs_message_for_a_missing_parent_can_be_configured_to_mean_wait_for_it(string[] messages, SyncOutcome outcome, string code)
    {
        var current = Rows(("Account", $$"""{"Id":"{{AccountId}}","ExternalId__c":"A1","Name":"Acme","Active__c":false}"""));
        var request = WritePlanner.Plan([Normalized(new UpsertUnit([Profile("P1", "Gold", "A1")]))], current).Requests.Single();
        var errors = new SalesforceErrors(Options.Create(new SalesforceOptions { ParentMissingMessages = messages }));

        var result = new WriteResultInterpreter(errors).Interpret(request,
        [
            new CollectionResult("Profile__c", [new RecordResult(false, null,
                [new SalesforceError(null, "FIELD_CUSTOM_VALIDATION_EXCEPTION", "Parent Account A1 does not exist")])], [])
        ]).Single();

        (result.Outcome, result.PrimaryError!.Code).Should().Be((outcome, code), "any code, as long as the message matches (case-insensitive)");
    }

    [Fact]
    public void A_field_that_must_change_but_is_not_updateable_fails_the_unit()
    {
        var current = Rows(("Account", $$"""{"Id":"{{AccountId}}","ExternalId__c":"A1","Name":"Acme","Active__c":false,"Locked__c":"old"}"""));

        var plan = WritePlanner.Plan([Normalized(Account("A1", "Acme", locked: "new"))], current);

        var result = plan.NotSent.Should().ContainSingle().Subject;
        (result.Outcome, result.PrimaryError!.Code).Should().Be((SyncOutcome.PermanentFailure, "INVALID_FIELD_FOR_INSERT_UPDATE"));
    }

    // ---------- CompositeWrite ----------

    [Fact]
    public void Building_a_request_twice_gives_the_same_request()
    {
        var plan = WritePlanner.Plan([Normalized(AccountWithContact("A1")), Normalized(AccountWithContact("A2"))], CurrentRecords.None);
        var request = plan.Requests.Single();

        JsonSerializer.Serialize(request.BuildRequests()).Should().Be(JsonSerializer.Serialize(request.BuildRequests()));
        request.Collections.Select(c => c.ReferenceId).Should().Equal("Account", "Contact_ref");
    }

    // ---------- WriteResultInterpreter ----------

    [Fact]
    public void Every_record_saved_means_the_unit_is_saved()
    {
        var request = Request(AccountWithContact("A1"));

        var results = Interpret(request, Ok("Account"), Ok("Contact_ref"));

        results.Single().Outcome.Should().Be(SyncOutcome.Saved);
    }

    public static TheoryData<string, CollectionResult[], SyncOutcome, string> SingleRecordResults()
    {
        return new()
    {
        { "no answer for the subrequest", [], SyncOutcome.TransientFailure, "UNKNOWN_ERROR" },
        { "the subrequest failed as a whole, even with a permanent code",
            [new CollectionResult("Account", null, [new SalesforceError("Account", "INVALID_FIELD", "x")])],
            SyncOutcome.TransientFailure, "INVALID_FIELD" },
        { "fewer results than records", [new CollectionResult("Account", [], [])], SyncOutcome.TransientFailure, "UNKNOWN_ERROR" },
        { "a record failed without details", [new CollectionResult("Account", [new RecordResult(false, null, [])], [])],
            SyncOutcome.TransientFailure, "UNKNOWN_ERROR" },
        { "a validation rule", [Failed("Account", "FIELD_CUSTOM_VALIDATION_EXCEPTION")], SyncOutcome.PermanentFailure, "FIELD_CUSTOM_VALIDATION_EXCEPTION" },
        { "a create that lost a race", [Failed("Account", "DUPLICATE_VALUE")], SyncOutcome.TransientFailure, "DUPLICATE_VALUE" },
        { "only skipped, nobody says why", [Failed("Account", "PROCESSING_HALTED")], SyncOutcome.TransientFailure, "UNKNOWN_ERROR" }
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
        var result = Interpret(Request(Account("A1", "Acme")), Failed("Account", codes)).Single();

        (result.Outcome, result.PrimaryError?.Code).Should().Be((outcome, code));
    }

    [Fact]
    public void A_record_only_skipped_because_of_another_records_failure_is_not_attempted()
    {
        var results = Interpret(Request(Account("A1", "Acme"), Account("A2", "Acme")), new CollectionResult("Account",
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

        Interpret(options, Request(Account("A1", "Acme")), Failed("Account", "UNABLE_TO_LOCK_ROW")).Single().Outcome
            .Should().Be(SyncOutcome.PermanentFailure);
        Interpret(options, Request(Account("A1", "Acme")), Failed("Account", "REQUIRED_FIELD_MISSING")).Single().Outcome
            .Should().Be(SyncOutcome.TransientFailure, "a code is permanent only when listed, and setting the list replaces the defaults");
    }

    [Fact]
    public void A_record_deleted_between_the_read_and_the_update_is_retried_not_dead_lettered()
    {
        var current = Rows(("Account", $$"""{"Id":"{{AccountId}}","ExternalId__c":"A1","Name":"Old","Active__c":false}"""));
        var request = WritePlanner.Plan([Normalized(Account("A1", "New"))], current).Requests.Single();

        var result = Interpret(request, Failed("Account", "ENTITY_IS_DELETED")).Single();

        result.Outcome.Should().Be(SyncOutcome.TransientFailure, "the next read finds it gone and creates it");
    }

    [Fact]
    public void A_failed_parent_decides_its_childs_unit_and_its_broken_reference_costs_the_other_children_nothing()
    {
        var request = Request(AccountWithContact("A1"), AccountWithContact("A2"));

        var results = Interpret(request,
            new CollectionResult("Account",
            [
                new RecordResult(false, null, [new SalesforceError(null, "FIELD_CUSTOM_VALIDATION_EXCEPTION", "bad")]),
                new RecordResult(true, AccountId, [])
            ], []),
            // A reference without a value fails the whole subrequest of children that use references
            new CollectionResult("Contact_ref", null, [new SalesforceError("Contact_ref", "PROCESSING_HALTED", "No value for Account[0].id")])
        ).ToDictionary(r => r.Key);

        results["A1"].Outcome.Should().Be(SyncOutcome.PermanentFailure);
        results["A1"].Errors.Should().OnlyContain(e => e.ReferenceId == "acct", "the child's own error is the parent's fault");
        results["A2"].Outcome.Should().Be(SyncOutcome.NotAttempted, "its Account is in; its Contact went down with A1's reference");
    }

    [Fact]
    public void Salesforce_rejecting_a_lookup_target_that_is_gone_means_wait_for_it()
    {
        var current = Rows(("Account", $$"""{"Id":"{{AccountId}}","ExternalId__c":"A1","Name":"Acme","Active__c":false}"""));
        var request = WritePlanner.Plan([Normalized(new UpsertUnit([Contact("C1", "A1")]))], current).Requests.Single();

        var result = Interpret(request, new CollectionResult("Contact",
            [new RecordResult(false, null, [new SalesforceError(null, "INVALID_CROSS_REFERENCE_KEY", "invalid cross reference id", ["AccountId"])])], [])).Single();

        (result.Outcome, result.PrimaryError!.Code).Should().Be((SyncOutcome.TransientFailure, SalesforceErrors.DependencyNotSynced));
    }

    // ---------- Requests: one upsert per level and object ----------

    [Fact]
    public void A_request_of_one_object_without_references_is_one_upsert_carrying_the_planned_fields()
    {
        var current = Rows(("Account", $$"""{"Id":"{{AccountId}}","ExternalId__c":"A1","Name":"Old","Active__c":false}"""));
        var request = WritePlanner.Plan([Normalized(Account("A1", "New")), Normalized(Account("A2", "Created"))], current).Requests.Single();

        var upsert = request.BuildRequests().Should().ContainSingle().Subject;

        (upsert.ReferenceId, upsert.SObject, upsert.ExternalIdField).Should().Be(("Account", "Account", "ExternalId__c"));
        JsonSerializer.Serialize(upsert.Records).Should().Be(JsonSerializer.Serialize(new object[]
        {
            new Dictionary<string, object?> { ["attributes"] = new { type = "Account" }, ["ExternalId__c"] = "A1", ["Name"] = "New" },
            new Dictionary<string, object?> { ["attributes"] = new { type = "Account" }, ["ExternalId__c"] = "A2", ["Name"] = "Created", ["Active__c"] = false }
        }), "a new record whole, an existing one by its external id with what differs, never an Id");
    }

    [Fact]
    public void Account_only_and_Profile_only_changes_are_one_request_of_two_upserts_Accounts_first()
    {
        var current = Rows(
            ("Account", $$"""{"Id":"{{AccountId}}","ExternalId__c":"A1","Name":"Old","Active__c":false}"""),
            ("Account", /*lang=json,strict*/ """{"Id":"001000000000002AAA","ExternalId__c":"A2","Name":"Acme","Active__c":false}"""),
            ("Profile__c", /*lang=json,strict*/ """{"Id":"a00000000000002AAA","ExternalId__c":"P-A2","Tier__c":"Silver","AccountId__c":"A2"}"""));
        var request = WritePlanner.Plan([Normalized(Account("A1", "New")), Normalized(AccountWithProfile("A2"))], current).Requests.Single();

        var upserts = request.BuildRequests();

        upserts.Select(u => (u.ReferenceId, u.SObject, u.Records.Count)).Should().Equal(("Account", "Account", 1), ("Profile__c", "Profile__c", 1));
        JsonSerializer.Serialize(upserts[1].Records).Should().Be(JsonSerializer.Serialize(new object[]
        {
            new Dictionary<string, object?> { ["attributes"] = new { type = "Profile__c" }, ["ExternalId__c"] = "P-A2", ["Tier__c"] = "Gold" }
        }), "only what differs: the Profile already points at its Account");
    }

    [Fact]
    public void A_new_child_linked_to_its_new_parent_by_reference_shares_the_request_with_the_other_writes()
    {
        Request(AccountWithContact("A1")).BuildRequests().Select(u => u.ReferenceId).Should().Equal("Account", "Contact_ref");

        var current = Rows(("Account", $$"""{"Id":"{{AccountId}}","ExternalId__c":"A2","Name":"Old","Active__c":false}"""));
        var request = WritePlanner.Plan([Normalized(AccountWithContact("A1")), Normalized(Account("A2", "New"))], current).Requests.Single();
        request.BuildRequests().Select(u => (u.ReferenceId, u.Records.Count)).Should().Equal([("Account", 2), ("Contact_ref", 1)],
            "A2's update goes in the same upsert as A1's create");
    }

    [Fact]
    public void More_than_200_records_of_one_object_are_upserted_200_per_request_each_record_once()
    {
        var existing = Enumerable.Range(0, 150).Select(i => ("Account", $$"""{"Id":"001{{i:D12}}","ExternalId__c":"OLD{{i}}","Name":"Old","Active__c":false}"""));
        var units = Enumerable.Range(0, 150).Select(i => Normalized(Account($"NEW{i}", "New")))
            .Concat(Enumerable.Range(0, 150).Select(i => Normalized(Account($"OLD{i}", "New")))).ToList();

        var requests = WritePlanner.Plan(units, Rows([.. existing])).Requests;

        requests.Select(r => r.BuildRequests().Single().Records.Count).Should().Equal([200, 100], "one upsert each");
        requests.SelectMany(r => r.Collections.SelectMany(c => c.Writes)).Should().HaveCount(300).And.OnlyHaveUniqueItems();
    }

    public static TheoryData<string, CollectionResult, SyncOutcome> ProfileAnswers()
    {
        return new()
        {
            { "saved", Ok("Profile__c"), SyncOutcome.Saved },
            { "a validation rule", Failed("Profile__c", "FIELD_CUSTOM_VALIDATION_EXCEPTION"), SyncOutcome.PermanentFailure },
            { "a row lock", Failed("Profile__c", "UNABLE_TO_LOCK_ROW"), SyncOutcome.TransientFailure }
        };
    }

    [Theory, MemberData(nameof(ProfileAnswers))]
    public void An_entity_written_in_two_upserts_gets_one_outcome_and_the_other_objects_records_keep_theirs(string what,
        CollectionResult profileAnswer, SyncOutcome outcome)
    {
        var current = Rows(
            ("Account", $$"""{"Id":"{{AccountId}}","ExternalId__c":"A1","Name":"Old","Active__c":false}"""),
            ("Account", /*lang=json,strict*/ """{"Id":"001000000000002AAA","ExternalId__c":"A2","Name":"Old","Active__c":false}"""),
            ("Profile__c", /*lang=json,strict*/ """{"Id":"a00000000000001AAA","ExternalId__c":"P-A1","Tier__c":"Silver","AccountId__c":"A1"}"""));
        var request = WritePlanner.Plan([Normalized(AccountWithProfile("A1")), Normalized(Account("A2", "Acme"))], current).Requests.Single();
        var results = Interpret(request,
            new CollectionResult("Account", [new RecordResult(true, AccountId, [], false), new RecordResult(true, "001000000000002AAA", [], false)], []),
            profileAnswer).ToDictionary(r => r.Key);

        results.Should().HaveCount(2);
        results["A1"].Outcome.Should().Be(outcome, $"its Account is saved and its Profile {what}");
        results["A2"].Outcome.Should().Be(SyncOutcome.Saved, "its Account was saved; the Profile upsert is not its business");
    }

    [Fact]
    public void An_update_sent_as_an_upsert_that_created_the_record_is_retried_to_write_the_rest()
    {
        var current = Rows(("Account", $$"""{"Id":"{{AccountId}}","ExternalId__c":"A1","Name":"Old","Active__c":false}"""));
        var request = WritePlanner.Plan([Normalized(Account("A1", "New"))], current).Requests.Single();

        var result = Interpret(request, new CollectionResult("Account", [new RecordResult(true, AccountId, [], Created: true)], [])).Single();

        (result.Outcome, result.PrimaryError!.Code).Should().Be((SyncOutcome.TransientFailure, "ENTITY_IS_DELETED"),
            "the record was deleted after the read, and the upsert created it from the changed fields alone");
    }

    [Theory]
    [InlineData(false, SyncOutcome.PermanentFailure)]
    [InlineData(true, SyncOutcome.TransientFailure)]
    public void A_required_field_missing_on_an_update_sent_as_an_upsert_means_the_record_is_gone(bool exists, SyncOutcome outcome)
    {
        var current = exists ? Rows(("Account", $$"""{"Id":"{{AccountId}}","ExternalId__c":"A1","Name":"Old","Active__c":false}""")) : CurrentRecords.None;
        var request = WritePlanner.Plan([Normalized(Account("A1", "New"))], current).Requests.Single();
        var result = Interpret(request, Failed("Account", "REQUIRED_FIELD_MISSING")).Single();

        result.Outcome.Should().Be(outcome);
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

        return new UpsertUnit([new SObjectRecord("acct", "Account", "ExternalId__c", key, fields)]);
    }

    /// <summary>A Profile naming its Account by a foreign key.</summary>
    private static SObjectRecord Profile(string key, string? tier, string accountKey)
    {
        return new("prof", "Profile__c", "ExternalId__c", key, new Dictionary<string, object?> { ["Tier__c"] = tier, ["AccountId__c"] = accountKey },
                ForeignKeys: [new SObjectLookup("AccountId__c", "Account", "ExternalId__c", accountKey)]);
    }

    /// <summary>A Contact looking up its Account.</summary>
    private static SObjectRecord Contact(string key, string accountKey)
    {
        return new("contact", "Contact", "ExternalId__c", key, new Dictionary<string, object?> { ["LastName"] = "Doe" },
                [new SObjectLookup("AccountId", "Account", "ExternalId__c", accountKey)]);
    }

    private static UpsertUnit AccountWithProfile(string key)
    {
        return new([Account(key, "Acme").Records[0], Profile($"P-{key}", "Gold", key)]);
    }

    private static UpsertUnit AccountWithContact(string key)
    {
        return new([Account(key, "Acme").Records[0], Contact($"C-{key}", key)]);
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