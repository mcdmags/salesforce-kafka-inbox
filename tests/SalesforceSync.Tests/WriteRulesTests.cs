using System.Text.Json;
using Microsoft.Extensions.Options;
using SalesforceSync.Infrastructure.Salesforce;
using SalesforceSync.ToSalesforce;
using static SalesforceSync.ToSalesforce.Differ;
using static SalesforceSync.ToSalesforce.Normalizer;
using static SalesforceSync.ToSalesforce.Reader;

namespace SalesforceSync.Tests;

/// <summary>
/// The write path's rules alone, from hand-built Salesforce state and results: the diff and plan
/// (<see cref="Differ.Plan"/>), the calls (<see cref="Sender.WriteAsync"/>, on a stub client) and the reading of the
/// answers (<see cref="Interpreter.Interpret"/>). No org, no HTTP.
/// </summary>
public sealed class WriteRulesTests
{
    private const string AccountId = "001000000000001AAA";
    private const string ContactId = "003000000000001AAA";

    // ---------- The diff ----------

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

        var fields = Diff(record, Current(stored));

        fields.Keys.Should().BeEquivalentTo(expected, what);
    }

    [Fact]
    public void A_new_record_gets_its_non_empty_fields_and_a_checkbox_that_is_null_is_left_to_its_default()
    {
        var record = Normalized(Account("A1", "Acme", active: null)).Records[0];

        var fields = Diff(record, null);

        fields.Should().BeEquivalentTo(new Dictionary<string, object?> { ["Name"] = "Acme" });
    }

    // ---------- The plan ----------

    [Fact]
    public void Create_update_and_unchanged_are_decided_by_what_Salesforce_has()
    {
        var current = Rows(("Account", $$"""{"Id":"{{AccountId}}","ExternalId__c":"OLD","Name":"Acme","Active__c":false}"""),
            ("Account", /*lang=json,strict*/ """{"Id":"001000000000002AAA","ExternalId__c":"SAME","Name":"Same","Active__c":false}"""));

        var (planned, decided) = Differ.Plan(
            [Normalized(Account("NEW", "New")), Normalized(Account("OLD", "Changed")), Normalized(Account("SAME", "Same"))], current);

        decided.Should().ContainSingle().Which.Should().Match<SyncResult>(r => r.Key == "SAME" && r.Outcome == SyncOutcome.Unchanged);
        var changes = planned.Select(u => u.Changes.Single()).ToList();
        changes.Select(c => (c.Record.Record.ExternalId, c.Id, c.IsCreate)).Should().Equal(("NEW", null, true), ("OLD", AccountId, false));
        changes[0].Body().Keys.Should().BeEquivalentTo(["attributes", "ExternalId__c", "Name", "Active__c"], "a create sends every field");
        changes[1].Body().Keys.Should().BeEquivalentTo(["attributes", "ExternalId__c", "Name"], "an update sends only what differs, never an Id");
    }

    [Fact]
    public void A_parent_in_the_same_unit_or_in_Salesforce_is_enough_one_in_neither_makes_the_unit_wait()
    {
        var accountInSalesforce = Rows(("Account", $$"""{"Id":"{{AccountId}}","ExternalId__c":"A1","Name":"Acme","Active__c":false}"""));

        Differ.Plan([Normalized(AccountWithProfile("A1"))], CurrentRecords.None).Decided.Should().BeEmpty("the unit creates its Account");
        Differ.Plan([Normalized(new SyncUnit([Profile("P1", "Gold", "A1")]))], accountInSalesforce).Decided.Should().BeEmpty();

        var (planned, decided) = Differ.Plan([Normalized(new SyncUnit([Profile("P1", "Gold", "MISSING")]))], CurrentRecords.None);

        var result = decided.Should().ContainSingle().Subject;
        (result.Outcome, result.PrimaryError!.Code).Should().Be((SyncOutcome.TransientFailure, SalesforceErrors.DependencyNotSynced));
        result.PrimaryError.Message.Should().Contain("Account ExternalId__c=MISSING is not in Salesforce yet");
        planned.Should().BeEmpty("nothing is sent for it");
    }

    [Fact]
    public void A_record_to_delete_that_Salesforce_has_is_deleted_by_its_Id_and_one_it_does_not_have_costs_nothing()
    {
        var current = Rows(("Contact", $$"""{"Id":"{{ContactId}}","ExternalId__c":"C1"}"""));

        var (planned, decided) = Differ.Plan([Normalized(Deleting("C1")), Normalized(Deleting("C2"))], current);

        var change = planned.Single().Changes.Single();
        (change.IsDelete, change.Id).Should().Be((true, ContactId));
        decided.Should().ContainSingle().Which.Should().Match<SyncResult>(r => r.Key == "C2" && r.Outcome == SyncOutcome.Unchanged);
    }

    // ---------- The calls ----------

    [Fact]
    public async Task Objects_are_upserted_one_call_each_parents_first_whatever_the_order_of_the_records()
    {
        var salesforce = new StubSalesforce();
        var unit = new SyncUnit([Profile("P-A1", "Gold", "A1"), Account("A1", "Acme").Records[0]]);

        var results = await Write(salesforce, [Normalized(unit)]);

        salesforce.Calls.Select(c => c.Describe()).Should().Equal("upsert Account ExternalId__c [A1]", "upsert Profile__c ExternalId__c [P-A1]");
        results.Single().Outcome.Should().Be(SyncOutcome.Saved);
    }

    [Fact]
    public async Task Units_with_the_same_object_share_its_upsert_and_up_to_200_records_go_in_one_call()
    {
        var salesforce = new StubSalesforce();
        var units = Enumerable.Range(0, 5).Select(i => Normalized(AccountWithProfile($"A{i}"))).ToList();

        await Write(salesforce, units, maxRecordsPerWrite: 200);
        salesforce.Calls.Select(c => c.Describe()).Should().Equal(
            "upsert Account ExternalId__c [A0, A1, A2, A3, A4]", "upsert Profile__c ExternalId__c [P-A0, P-A1, P-A2, P-A3, P-A4]");

        salesforce.Calls.Clear();
        await Write(salesforce, units, maxRecordsPerWrite: 2);
        salesforce.Calls.Select(c => c.Describe()).Should().Equal(
            "upsert Account ExternalId__c [A0, A1]", "upsert Account ExternalId__c [A2, A3]", "upsert Account ExternalId__c [A4]",
            "upsert Profile__c ExternalId__c [P-A0, P-A1]", "upsert Profile__c ExternalId__c [P-A2, P-A3]", "upsert Profile__c ExternalId__c [P-A4]");
    }

    [Fact]
    public async Task Deletes_go_last_in_the_order_of_the_records_and_200_to_a_call()
    {
        var salesforce = new StubSalesforce();
        var unit = new SyncUnit(
        [
            new SObjectRecord("prof", "Profile__c", "ExternalId__c", "P-A1", new Dictionary<string, object?>(), Delete: true),
            new SObjectRecord("acct", "Account", "ExternalId__c", "A1", new Dictionary<string, object?>(), Delete: true)
        ]);
        var current = Rows(("Account", $$"""{"Id":"{{AccountId}}","ExternalId__c":"A1"}"""),
            ("Profile__c", /*lang=json,strict*/ """{"Id":"a00000000000001AAA","ExternalId__c":"P-A1"}"""));

        await Write(salesforce, [Normalized(Account("A2", "Acme")), Normalized(unit)], current, maxRecordsPerWrite: 1);

        salesforce.Calls.Select(c => c.Describe()).Should().Equal(
            "upsert Account ExternalId__c [A2]", "delete [a00000000000001AAA]", $"delete [{AccountId}]");
    }

    [Fact]
    public async Task A_unit_that_failed_in_one_call_is_not_sent_in_the_next_and_the_others_are()
    {
        var salesforce = new StubSalesforce { Failing = { ["A1"] = "FIELD_CUSTOM_VALIDATION_EXCEPTION" } };

        var results = (await Write(salesforce, [Normalized(AccountWithProfile("A1")), Normalized(AccountWithProfile("A2"))]))
            .ToDictionary(r => r.Key);

        salesforce.Calls.Select(c => c.Describe()).Should().Equal(
            "upsert Account ExternalId__c [A1, A2]", "upsert Profile__c ExternalId__c [P-A2]");
        (results["A1"].Outcome, results["A1"].PrimaryError!.Code).Should().Be((SyncOutcome.PermanentFailure, "FIELD_CUSTOM_VALIDATION_EXCEPTION"));
        results["A1"].Errors.Should().OnlyContain(e => e.ReferenceId == "acct");
        results["A2"].Outcome.Should().Be(SyncOutcome.Saved);
    }

    [Fact]
    public async Task An_entity_written_in_two_calls_gets_one_outcome_the_worst_of_its_records()
    {
        var salesforce = new StubSalesforce { Failing = { ["P-A1"] = "UNABLE_TO_LOCK_ROW" } };

        var results = (await Write(salesforce, [Normalized(AccountWithProfile("A1")), Normalized(Account("A2", "Acme"))])).ToDictionary(r => r.Key);

        results["A1"].Outcome.Should().Be(SyncOutcome.TransientFailure, "its Account is saved and its Profile hit a row lock");
        results["A2"].Outcome.Should().Be(SyncOutcome.Saved, "the Profile call is not its business");
    }

    [Fact]
    public async Task A_call_Salesforce_answers_with_too_few_results_fails_the_records_it_did_not_answer_for()
    {
        var salesforce = new StubSalesforce { AnswerWithNothing = true };

        var result = (await Write(salesforce, [Normalized(Account("A1", "Acme"))])).Single();

        (result.Outcome, result.PrimaryError!.Code).Should().Be((SyncOutcome.TransientFailure, "UNKNOWN_ERROR"));
    }

    [Fact]
    public async Task Objects_that_point_at_each_other_cannot_be_ordered()
    {
        var a = new SObjectRecord("a", "Widget__c", "Key__c", "W1", new Dictionary<string, object?> { ["Label__c"] = "x" },
            ForeignKeys: [new ForeignKey("Label__c", "Gadget__c", "Key__c", "G1")]);
        var b = new SObjectRecord("b", "Gadget__c", "Key__c", "G1", new Dictionary<string, object?> { ["Widget__c"] = "W1" },
            ForeignKeys: [new ForeignKey("Widget__c", "Widget__c", "Key__c", "W1")]);

        var write = () => Write(new StubSalesforce(), [Normalized(new SyncUnit([a, b]))]);

        await write.Should().ThrowAsync<InvalidOperationException>().WithMessage("*point at each other*");
    }

    // ---------- Reading the answers ----------

    [Fact]
    public void A_saved_record_is_saved()
    {
        Interpret(Account("A1", "Acme"), Ok()).Status.Should().Be(SyncOutcome.Saved);
    }

    public static TheoryData<string, RecordResult?, SyncOutcome, string> RecordResults()
    {
        return new()
        {
            { "no answer", null, SyncOutcome.TransientFailure, "UNKNOWN_ERROR" },
            { "a failure without details", new RecordResult(false, null, []), SyncOutcome.TransientFailure, "UNKNOWN_ERROR" },
            { "a validation rule", Failed("FIELD_CUSTOM_VALIDATION_EXCEPTION"), SyncOutcome.PermanentFailure, "FIELD_CUSTOM_VALIDATION_EXCEPTION" },
            { "a create that lost a race", Failed("DUPLICATE_VALUE"), SyncOutcome.TransientFailure, "DUPLICATE_VALUE" },
            { "a row lock", Failed("UNABLE_TO_LOCK_ROW"), SyncOutcome.TransientFailure, "UNABLE_TO_LOCK_ROW" },
            { "a code nobody knows", Failed("SOMETHING_NEW"), SyncOutcome.TransientFailure, "SOMETHING_NEW" },
            { "a permanent and a transient error", Failed("UNABLE_TO_LOCK_ROW", "REQUIRED_FIELD_MISSING"), SyncOutcome.PermanentFailure, "REQUIRED_FIELD_MISSING" }
        };
    }

    [Theory, MemberData(nameof(RecordResults))]
    public void A_records_result_decides_whether_a_retry_can_help(string what, RecordResult? result, SyncOutcome status, string code)
    {
        var (actual, errors) = Interpret(Account("A1", "Acme"), result);

        (actual, errors.First().Code).Should().Be((status, code), what);
    }

    [Fact]
    public void Permanent_codes_are_configurable()
    {
        var options = new SalesforceOptions { PermanentErrorCodes = ["UNABLE_TO_LOCK_ROW"] };

        Interpret(Account("A1", "Acme"), Failed("UNABLE_TO_LOCK_ROW"), options).Status.Should().Be(SyncOutcome.PermanentFailure);
        Interpret(Account("A1", "Acme"), Failed("REQUIRED_FIELD_MISSING"), options).Status.Should().Be(SyncOutcome.TransientFailure,
            "a code is permanent only when listed, and setting the list replaces the defaults");
    }

    [Theory]
    [InlineData(new string[0], SyncOutcome.PermanentFailure, "FIELD_CUSTOM_VALIDATION_EXCEPTION")]
    [InlineData(new[] { "other", "PARENT ACCOUNT" }, SyncOutcome.TransientFailure, SalesforceErrors.DependencyNotSynced)]
    public void The_orgs_message_for_a_missing_parent_can_be_configured_to_mean_wait_for_it(string[] messages, SyncOutcome status, string code)
    {
        var result = new RecordResult(false, null, [new SalesforceError(null, "FIELD_CUSTOM_VALIDATION_EXCEPTION", "Parent Account A1 does not exist")]);

        var (actual, errors) = Interpret(new SyncUnit([Profile("P1", "Gold", "A1")]), result, new SalesforceOptions { ParentMissingMessages = messages });

        (actual, errors.First().Code).Should().Be((status, code), "any code, as long as the message matches (case-insensitive)");
    }

    [Fact]
    public void An_update_sent_as_an_upsert_that_created_the_record_is_retried_to_write_the_rest()
    {
        var update = Update("A1", "New");

        var (status, errors) = Interpret(update, new RecordResult(true, AccountId, [], Created: true));

        (status, errors.Single().Code).Should().Be((SyncOutcome.TransientFailure, "ENTITY_IS_DELETED"),
            "the record was deleted after the read, and the upsert created it from the changed fields alone");
    }

    [Theory]
    [InlineData("ENTITY_IS_DELETED")]
    [InlineData("INVALID_CROSS_REFERENCE_KEY")]
    [InlineData("REQUIRED_FIELD_MISSING")]
    public void The_codes_that_mean_the_record_is_gone_make_an_update_transient_and_a_create_permanent(string code)
    {
        Interpret(Update("A1", "New"), Failed(code)).Status.Should().Be(SyncOutcome.TransientFailure,
            "an update of a record deleted after the read is retried: the next read finds it gone and creates it");
        Interpret(Account("A1", "New"), Failed(code)).Status.Should().Be(SyncOutcome.PermanentFailure);
    }

    [Fact]
    public void The_org_rejecting_the_record_type_is_retried_not_dead_lettered()
    {
        var result = new RecordResult(false, null, [new SalesforceError(null, "INVALID_CROSS_REFERENCE_KEY", "Record Type ID: this ID value isn't valid", ["RecordTypeId"])]);

        var (status, errors) = Interpret(Account("A1", "Acme"), result);

        (status, errors.Single().Code).Should().Be((SyncOutcome.TransientFailure, SalesforceErrors.InvalidRecordType));
    }

    public static TheoryData<string, string, SyncOutcome, string?> DeleteResults()
    {
        return new()
        {
            { "ENTITY_IS_DELETED", "entity is deleted", SyncOutcome.Saved, null },
            { "INVALID_CROSS_REFERENCE_KEY", "invalid cross reference id", SyncOutcome.Saved, null },
            { "NOT_FOUND", "The requested resource does not exist", SyncOutcome.Saved, null },
            { "DELETE_FAILED", "entity is deleted", SyncOutcome.Saved, null },
            { "DELETE_FAILED", "Your attempt to delete Acme could not be completed because it is associated with the following contacts.",
                SyncOutcome.TransientFailure, SalesforceErrors.DependencyNotSynced },
            { "DELETE_FAILED", "Cannot delete: a validation rule", SyncOutcome.PermanentFailure, "DELETE_FAILED" },
            { "INSUFFICIENT_ACCESS_OR_READONLY", "insufficient access rights on object id", SyncOutcome.PermanentFailure, "INSUFFICIENT_ACCESS_OR_READONLY" },
            { "UNABLE_TO_LOCK_ROW", "unable to obtain exclusive access to this record", SyncOutcome.TransientFailure, "UNABLE_TO_LOCK_ROW" },
            { "ENTITY_IS_LOCKED", "the entity is locked for editing (an approval)", SyncOutcome.TransientFailure, "ENTITY_IS_LOCKED" }
        };
    }

    [Theory, MemberData(nameof(DeleteResults))]
    public void A_deletes_result_is_read_like_any_other_and_a_record_gone_already_is_deleted(string code, string message,
        SyncOutcome status, string? decidingCode)
    {
        var delete = DeleteOf("C1", ContactId);

        var (actual, errors) = Interpret(delete, new RecordResult(false, null, [new SalesforceError(null, code, message)]));

        (actual, status == SyncOutcome.Saved ? null : errors.First().Code).Should().Be((status, decidingCode), $"{code}: {message}");
    }

    // ---------- Helpers ----------

    /// <summary>A delete of a Contact by its external id, as a strategy maps a removed contact.</summary>
    private static SyncUnit Deleting(string key)
    {
        return new([new SObjectRecord("contact", "Contact", "ExternalId__c", key, new Dictionary<string, object?>(), Delete: true)]);
    }

    private static SyncUnit Account(string key, object? name, bool? active = false)
    {
        var fields = new Dictionary<string, object?> { ["Name"] = name, ["Active__c"] = active };
        return new SyncUnit([new SObjectRecord("acct", "Account", "ExternalId__c", key, fields)]);
    }

    /// <summary>A Profile naming its Account by a foreign key.</summary>
    private static SObjectRecord Profile(string key, string? tier, string accountKey)
    {
        return new("prof", "Profile__c", "ExternalId__c", key, new Dictionary<string, object?> { ["Tier__c"] = tier, ["AccountId__c"] = accountKey },
            ForeignKeys: [new ForeignKey("AccountId__c", "Account", "ExternalId__c", accountKey)]);
    }

    private static SyncUnit AccountWithProfile(string key)
    {
        return new([Account(key, "Acme").Records[0], Profile($"P-{key}", "Gold", key)]);
    }

    private static NormalizedUnit Normalized(SyncUnit unit)
    {
        return new NormalizedUnit(unit.Records[0].ExternalId,
            [.. unit.Records.Select(r => new NormalizedRecord(r, r.Fields.ToDictionary(f => f.Key, f => FieldValues.Normalize(f.Value))))]);
    }

    private static CurrentRecord Current(string json)
    {
        return new(JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(json)!
            .ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.OrdinalIgnoreCase));
    }

    private static CurrentRecords Rows(params (string SObject, string Json)[] rows)
    {
        return new(rows.Select(r => (r.SObject, JsonDocument.Parse(r.Json).RootElement)));
    }

    /// <summary>Plans the units against what Salesforce has and sends the plan.</summary>
    private static async Task<List<SyncResult>> Write(StubSalesforce salesforce, IReadOnlyList<NormalizedUnit> units,
        CurrentRecords? current = null, int maxRecordsPerWrite = 200)
    {
        var (planned, _) = Differ.Plan(units, current ?? CurrentRecords.None);
        return await new Sender(salesforce, new Interpreter(new SalesforceErrors(Options.Create(new SalesforceOptions())))).WriteAsync(planned, maxRecordsPerWrite, CancellationToken.None);
    }

    /// <summary>A change of a record Salesforce has (an update).</summary>
    private static Change Update(string key, string name)
    {
        return new Change(key, Normalized(Account(key, name)).Records[0], AccountId, new Dictionary<string, object?> { ["Name"] = name });
    }

    private static Change DeleteOf(string key, string id)
    {
        return new Change(key, Normalized(Deleting(key)).Records[0], id, new Dictionary<string, object?>());
    }

    private static (SyncOutcome Status, List<SalesforceError> Errors) Interpret(SyncUnit unit, RecordResult? result, SalesforceOptions? options = null)
    {
        return Interpret(new Change(unit.Records[0].ExternalId, Normalized(unit).Records[0], null, new Dictionary<string, object?>()), result, options);
    }

    private static (SyncOutcome Status, List<SalesforceError> Errors) Interpret(Change change, RecordResult? result, SalesforceOptions? options = null)
    {
        return new Interpreter(new SalesforceErrors(Options.Create(options ?? new SalesforceOptions()))).Interpret(change, result);
    }

    private static RecordResult Ok()
    {
        return new(true, AccountId, []);
    }

    private static RecordResult Failed(params string[] codes)
    {
        return new(false, null, [.. codes.Select(code => new SalesforceError(null, code, "failed"))]);
    }

    /// <summary>
    /// A Salesforce that records its calls and answers each record: saved, or failed with the code given for its external
    /// id in <see cref="Failing"/>.
    /// </summary>
    private sealed class StubSalesforce : ISalesforceRestClient
    {
        public List<Call> Calls { get; } = [];
        public Dictionary<string, string> Failing { get; } = [];
        public bool AnswerWithNothing { get; set; }

        public string DataPath => "/services/data/v62.0";

        public Task<List<JsonElement>> SelectAsync(string soql, CancellationToken ct)
        {
            throw new NotSupportedException();
        }

        public Task<List<TRow>> SelectAsync<TRow>(string soql, CancellationToken ct)
        {
            throw new NotSupportedException();
        }

        public Task<IReadOnlyList<RecordResult>> UpsertAsync(string sobject, string externalIdField, IReadOnlyList<SObjectBody> records,
            CancellationToken ct)
        {
            var keys = records.Select(r => (string)r[externalIdField]!).ToList();
            Calls.Add(new Call($"upsert {sobject} {externalIdField}", keys));
            return Task.FromResult(Answer(keys));
        }

        public Task<IReadOnlyList<RecordResult>> DeleteAsync(IReadOnlyList<string> ids, CancellationToken ct)
        {
            Calls.Add(new Call("delete", ids));
            return Task.FromResult(Answer(ids));
        }

        private IReadOnlyList<RecordResult> Answer(IReadOnlyList<string> keys)
        {
            return AnswerWithNothing
                ? []
                : [.. keys.Select(k => Failing.TryGetValue(k, out var code) ? Failed(code) : new RecordResult(true, "001000000000009AAA", []))];
        }

        public sealed record Call(string What, IReadOnlyList<string> Keys)
        {
            public string Describe()
            {
                return $"{What} [{string.Join(", ", Keys)}]";
            }
        }
    }
}
