using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Extensions.Options;
using SalesforceSync.FakeSalesforce;
using SalesforceSync.Infrastructure.Salesforce;
using SalesforceSync.ToSalesforce;

namespace SalesforceSync.Tests;

/// <summary>Salesforce calls the send steps are made of, vs the mock org.</summary>
public sealed class SalesforceCallsTests
{
    private static readonly CancellationToken Ct = CancellationToken.None;
    private static readonly SalesforceObject ContactObject = new("Contact", "ExternalId__c");

    private readonly FakeSalesforceStore _org = new(new FakeSalesforceOptions { EnforceDailyLimit = false });
    private readonly SalesforceCalls _calls;

    public SalesforceCallsTests()
    {
        _calls = SalesforceStep.Calls(SalesforceStep.Http(new FakeSalesforceHandler(_org)), Options.Create(new SalesforceOptions()));
    }

    // ---------- Changes: what to send for a record ----------

    [Fact]
    public void A_new_record_gets_its_non_empty_fields()
    {
        var changes = SalesforceCalls.Changes(new() { ["Name"] = "Acme", ["Rating"] = null, ["Phone"] = "" }, null);

        changes.Should().Equal(new Dictionary<string, object?> { ["Name"] = "Acme" }, "a null would override a field's default");
    }

    [Fact]
    public void An_existing_record_gets_only_the_fields_that_differ()
    {
        var now = Row(new JsonObject { ["Name"] = "Acme", ["Rating"] = "Hot", ["NumberOfEmployees"] = 250 });

        var changes = SalesforceCalls.Changes(new() { ["Name"] = "Acme", ["Rating"] = "Cold", ["NumberOfEmployees"] = 250 }, now);

        changes.Should().Equal(new Dictionary<string, object?> { ["Rating"] = "Cold" });
    }

    [Fact]
    public void A_record_that_is_as_wanted_has_nothing_to_send()
    {
        var now = Row(new JsonObject
        {
            ["Name"] = "Acme",
            ["AnnualRevenue"] = 1250000.5,
            ["Active__c"] = false,
            ["Customer_Since__c"] = "2020-01-15",
            ["Last_Contacted__c"] = "2026-10-12T01:02:03.000+0000",
            ["Description"] = null
        });

        var changes = SalesforceCalls.Changes(new()
        {
            ["Name"] = "Acme",
            ["AnnualRevenue"] = 1250000.5m,
            ["Active__c"] = null, // unset checkbox = false in Salesforce
            ["Customer_Since__c"] = new DateOnly(2020, 1, 15),
            ["Last_Contacted__c"] = new DateTimeOffset(2026, 10, 12, 1, 2, 3, 456, TimeSpan.Zero), // Salesforce keeps whole seconds
            ["Description"] = ""
        }, now);

        changes.Should().BeEmpty();
    }

    [Fact]
    public void A_null_clears_a_field_Salesforce_has()
    {
        var now = Row(new JsonObject { ["Name"] = "Acme", ["Rating"] = "Hot" });

        var changes = SalesforceCalls.Changes(new() { ["Name"] = "Acme", ["Rating"] = null }, now);

        changes.Should().Equal(new Dictionary<string, object?> { ["Rating"] = null });
    }

    [Fact]
    public void Values_are_sent_in_the_form_Salesforce_takes()
    {
        var changes = SalesforceCalls.Changes(new()
        {
            ["Customer_Since__c"] = new DateOnly(2020, 1, 15),
            ["Last_Contacted__c"] = new DateTimeOffset(2026, 10, 12, 3, 2, 3, 456, TimeSpan.FromHours(2)),
            ["NumberOfEmployees"] = 250
        }, null);

        changes.Should().Equal(new Dictionary<string, object?>
        {
            ["Customer_Since__c"] = "2020-01-15",
            ["Last_Contacted__c"] = "2026-10-12T01:02:03.000Z",
            ["NumberOfEmployees"] = 250m
        });
    }

    [Fact]
    public void A_field_Salesforce_has_no_value_for_and_a_changed_one_are_both_sent()
    {
        var now = Row(new JsonObject { ["FirstName"] = "Ana", ["MiddleName"] = null, ["LastName"] = "Cruz" });

        var changes = SalesforceCalls.Changes(new() { ["FirstName"] = "Anna", ["MiddleName"] = "Lee", ["LastName"] = "Cruz" }, now);

        changes.Should().Equal(new Dictionary<string, object?> { ["FirstName"] = "Anna", ["MiddleName"] = "Lee" });
    }

    [Fact]
    public void A_field_missing_from_Salesforce_s_row_is_sent()
    {
        // A row read before the field was added to the mapping (or one the read didn't select) has no such property
        var now = Row(new JsonObject { ["FirstName"] = "Ana", ["LastName"] = "Cruz" });

        var changes = SalesforceCalls.Changes(new() { ["FirstName"] = "Ana", ["MiddleName"] = "Lee", ["LastName"] = "Cruz" }, now);

        changes.Should().Equal(new Dictionary<string, object?> { ["MiddleName"] = "Lee" });
    }

    [Fact]
    public void A_change_of_letter_case_alone_is_sent()
    {
        var now = Row(new JsonObject { ["FirstName"] = "ana" });

        var changes = SalesforceCalls.Changes(new() { ["FirstName"] = "Ana" }, now);

        changes.Should().Equal(new Dictionary<string, object?> { ["FirstName"] = "Ana" });
    }

    [Fact]
    public async Task A_new_field_and_a_changed_one_are_read_found_different_and_written()
    {
        var failures = new Dictionary<string, List<Failure>>();
        await _calls.UpsertAsync(ContactObject, [("C1", "C1", new Dictionary<string, object?> { ["FirstName"] = "Ana", ["LastName"] = "Cruz" })], failures, Ct);
        var wanted = new WantedRecord("C1", "C1", new() { ["FirstName"] = "Anna", ["MiddleName"] = "Lee", ["LastName"] = "Cruz" });

        var now = await _calls.ReadCurrentAsync(ContactObject, [wanted], ["C1"], Ct);
        var changed = SalesforceCalls.Changed([wanted], now);
        await _calls.UpsertAsync(ContactObject, changed, failures, Ct);

        now["C1"].GetProperty("MiddleName").ValueKind.Should().Be(JsonValueKind.Null, "the read selects the new field; Salesforce has no value yet");
        changed.Should().ContainSingle().Which.Fields.Should().Equal(new Dictionary<string, object?> { ["FirstName"] = "Anna", ["MiddleName"] = "Lee" });
        failures.Should().BeEmpty();
        var contact = _org.FindByExternalId("Contact", "C1")!;
        ((string?)contact["FirstName"], (string?)contact["MiddleName"], (string?)contact["LastName"]).Should().Be(("Anna", "Lee", "Cruz"));
    }

    // ---------- Record types ----------

    [Fact]
    public async Task A_record_type_is_found_by_its_developer_name_and_an_unknown_one_is_null()
    {
        (await _calls.RecordTypeIdAsync(AccountSync.AccountObject, "Business", Ct)).Should().NotBeNullOrEmpty();
        (await _calls.RecordTypeIdAsync(AccountSync.AccountObject, "Nonexistent", Ct)).Should().BeNull();
    }

    // ---------- Read ----------

    [Fact]
    public async Task A_read_finds_records_by_external_id_with_their_Id_and_skips_the_missing()
    {
        await CreateAsync("A1", "A2");

        var found = await _calls.ReadAsync(AccountSync.AccountObject, ["Name"], ["A1", "A2", "A3"], Ct);

        found.Keys.Should().BeEquivalentTo(["A1", "A2"]);
        found["A1"].GetProperty("Id").GetString().Should().NotBeNullOrEmpty();
        found["A1"].GetProperty("Name").GetString().Should().Be("Acme");
    }

    [Fact]
    public async Task A_read_asks_for_200_external_ids_at_a_time()
    {
        var ids = Enumerable.Range(0, 450).Select(i => $"ACC-{i:D4}").ToList();
        var before = _org.GetStats().Queries;

        var found = await _calls.ReadAsync(AccountSync.AccountObject, ["Name"], [.. ids, .. ids], Ct);

        found.Should().BeEmpty();
        (_org.GetStats().Queries - before).Should().Be(3, "450 distinct ids: 200 + 200 + 50");
    }

    [Fact]
    public async Task A_read_of_long_external_ids_splits_the_query_to_fit_the_URL_limit()
    {
        var ids = Enumerable.Range(0, 200).Select(i => $"ACC-{i:D4}-{new string('x', 200)}").ToList();
        await CreateAsync(ids[0], ids[199]);
        var before = _org.GetStats().Queries;

        var found = await _calls.ReadAsync(AccountSync.AccountObject, ["Name"], ids, Ct);

        found.Keys.Should().BeEquivalentTo([ids[0], ids[199]]);
        (_org.GetStats().Queries - before).Should().BeGreaterThan(1, "200 ids of 210 characters don't fit one URL");
    }

    [Fact]
    public async Task A_read_of_a_field_the_org_lacks_is_a_mapping_error()
    {
        var read = () => _calls.ReadAsync(AccountSync.AccountObject, ["Bogus__c"], ["A1"], Ct);

        (await read.Should().ThrowAsync<MappingErrorException>()).Which.Errors.Should().ContainSingle()
            .Which.Error.Code.Should().Be("INVALID_FIELD");
    }

    // ---------- Upsert ----------

    [Fact]
    public async Task An_upsert_creates_a_record_and_then_updates_it_by_its_external_id()
    {
        await CreateAsync("A1");
        await UpsertNamedAsync(["A1"], "Renamed");

        _org.All("Account").Should().ContainSingle().Which["Name"]!.GetValue<string>().Should().Be("Renamed");
        _org.GetStats().Should().Match<FakeSalesforceStats>(s => s.Creates == 1 && s.Updates == 1);
    }

    [Fact]
    public async Task Each_object_is_read_and_matched_by_its_own_external_id_field()
    {
        // Name stands in for an org whose Account is keyed by another field than ExternalId__c
        var byName = new SalesforceObject("Account", "Name");

        await _calls.UpsertAsync(byName, [("E1", "Acme", new Dictionary<string, object?> { ["Description"] = "first" })], [], Ct);
        await _calls.UpsertAsync(byName, [("E1", "Acme", new Dictionary<string, object?> { ["Description"] = "second" })], [], Ct);
        var found = await _calls.ReadAsync(byName, ["Description"], ["Acme"], Ct);

        _org.All("Account").Should().ContainSingle("the second upsert matched the first record by Name")
            .Which["Description"]!.GetValue<string>().Should().Be("second");
        found.Should().ContainKey("Acme").WhoseValue.GetProperty("Description").GetString().Should().Be("second");
    }

    [Fact]
    public async Task An_upsert_of_many_records_is_one_call_per_200()
    {
        var before = _org.GetStats().UpsertRequests;

        await CreateAsync([.. Enumerable.Range(0, 450).Select(i => $"ACC-{i:D4}")]);

        (_org.GetStats().UpsertRequests - before).Should().Be(3);
        _org.All("Account").Should().HaveCount(450);
    }

    [Theory]
    [InlineData("FIELD_CUSTOM_VALIDATION_EXCEPTION", true)]
    [InlineData("STRING_TOO_LONG", true)]
    [InlineData("UNABLE_TO_LOCK_ROW", false)]
    public async Task A_record_a_call_failed_is_a_failure_of_its_entity_and_permanent_only_when_the_data_is_wrong(string code, bool permanent)
    {
        _org.InjectFault("Account", "BAD", code, "nope", times: 1);
        var failures = new Dictionary<string, List<Failure>>();

        await _calls.UpsertAsync(AccountSync.AccountObject, [("E-BAD", "BAD", Fields("x")), ("E-GOOD", "GOOD", Fields("y"))], failures, Ct);

        failures.Keys.Should().Equal(["E-BAD"], "the other record was saved");
        failures["E-BAD"].Should().ContainSingle().Which.Should().Match<Failure>(f => f.Permanent == permanent && f.Error.StartsWith(code));
        _org.FindByExternalId("Account", "GOOD").Should().NotBeNull();
    }

    [Fact]
    public async Task A_parent_the_org_says_is_missing_is_waited_for_not_given_up_on()
    {
        var options = new SalesforceOptions { ParentMissingMessages = ["parent is not synced"] };
        var calls = SalesforceStep.Calls(SalesforceStep.Http(new FakeSalesforceHandler(_org)), Options.Create(options));
        _org.InjectFault("Account", "A1", "FIELD_CUSTOM_VALIDATION_EXCEPTION", "The parent is not synced yet", times: 1);
        var failures = new Dictionary<string, List<Failure>>();

        await calls.UpsertAsync(AccountSync.AccountObject, [("E1", "A1", Fields("x"))], failures, Ct);

        failures["E1"].Should().ContainSingle().Which.Should().Match<Failure>(f => !f.Permanent && f.Error.StartsWith("DEPENDENCY_NOT_SYNCED"));
    }

    [Fact]
    public async Task A_field_the_org_lacks_is_a_mapping_error()
    {
        var upsert = () => _calls.UpsertAsync(AccountSync.AccountObject, [("E1", "A1", new Dictionary<string, object?> { ["Bogus__c"] = "x" })], [], Ct);

        await upsert.Should().ThrowAsync<MappingErrorException>();
    }

    // ---------- Delete ----------

    [Fact]
    public async Task A_delete_removes_the_record_by_its_Id()
    {
        await CreateAsync("A1", "A2");
        var id = (await _calls.ReadAsync(AccountSync.AccountObject, [], ["A1"], Ct))["A1"].GetProperty("Id").GetString()!;
        var failures = new Dictionary<string, List<Failure>>();

        await _calls.DeleteAsync([("E1", id)], failures, Ct);

        failures.Should().BeEmpty();
        _org.FindByExternalId("Account", "A1").Should().BeNull();
        _org.FindByExternalId("Account", "A2").Should().NotBeNull();
    }

    [Fact]
    public async Task A_delete_of_a_record_that_is_gone_already_is_what_it_wanted()
    {
        await CreateAsync("A1");
        var id = (await _calls.ReadAsync(AccountSync.AccountObject, [], ["A1"], Ct))["A1"].GetProperty("Id").GetString()!;
        await _calls.DeleteAsync([("E1", id)], [], Ct);
        var failures = new Dictionary<string, List<Failure>>();

        await _calls.DeleteAsync([("E1", id)], failures, Ct);

        failures.Should().BeEmpty();
    }

    [Fact]
    public async Task A_delete_refused_because_records_still_name_it_waits_for_their_deletes()
    {
        var org = new FakeSalesforceStore(new FakeSalesforceOptions { EnforceDailyLimit = false, RefuseDeletingReferencedRecords = true });
        var calls = SalesforceStep.Calls(SalesforceStep.Http(new FakeSalesforceHandler(org)), Options.Create(new SalesforceOptions()));
        await calls.UpsertAsync(AccountSync.AccountObject, [("E1", "A1", Fields("x"))], [], Ct);
        await calls.UpsertAsync(ContactObject, [("C1", "C1", new Dictionary<string, object?> { ["LastName"] = "Doe", ["AccountId__c"] = "A1" })], [], Ct);
        var id = (await calls.ReadAsync(AccountSync.AccountObject, [], ["A1"], Ct))["A1"].GetProperty("Id").GetString()!;
        var failures = new Dictionary<string, List<Failure>>();

        await calls.DeleteAsync([("E1", id)], failures, Ct);

        failures["E1"].Should().ContainSingle().Which.Should().Match<Failure>(f => !f.Permanent && f.Error.StartsWith("DEPENDENCY_NOT_SYNCED"));
        org.FindByExternalId("Account", "A1").Should().NotBeNull();
    }

    // ---------- Helpers ----------

    private static Dictionary<string, object?> Fields(string name)
    {
        return new Dictionary<string, object?> { ["Name"] = name };
    }

    private async Task CreateAsync(params string[] keys)
    {
        await UpsertNamedAsync(keys, "Acme");
    }

    private async Task UpsertNamedAsync(string[] keys, string name)
    {
        var failures = new Dictionary<string, List<Failure>>();
        await _calls.UpsertAsync(AccountSync.AccountObject, keys.Select(k => (k, k, Fields(name))), failures, Ct);
        failures.Should().BeEmpty();
    }

    private static JsonElement Row(JsonObject fields)
    {
        return JsonDocument.Parse(fields.ToJsonString()).RootElement.Clone();
    }
}