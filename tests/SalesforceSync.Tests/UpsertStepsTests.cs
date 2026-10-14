using SalesforceSync.Salesforce;
using SalesforceSync.Sync;
using SalesforceSync.Sync.Upsert;

namespace SalesforceSync.Tests;

/// <summary>
/// The pure steps of <see cref="SalesforceBatchUpsertService"/>'s Salesforce step on their own: normalizing against the schema, building the
/// read and packing writes into Composite requests. No org, no HTTP.
/// </summary>
public sealed class UpsertStepsTests
{
    private static readonly SObjectSchema Account = new("Account",
    [
        new SalesforceField("Id", "id", 18, 0, 0, false, false, false),
        new SalesforceField("External_Id__c", "string", 255, 0, 0, true, true, true),
        new SalesforceField("Name", "string", 5, 0, 0, true, true, false)
    ]);

    private static readonly SObjectSchema Profile = new("Profile__c",
    [
        new SalesforceField("Id", "id", 18, 0, 0, false, false, false),
        new SalesforceField("External_Id__c", "string", 255, 0, 0, true, true, true),
        new SalesforceField("Account__c", "reference", 18, 0, 0, true, true, true)
    ]);

    private static readonly Dictionary<string, SObjectSchema> Schemas = new(StringComparer.OrdinalIgnoreCase)
    {
        ["Account"] = Account,
        ["Profile__c"] = Profile
    };

    // ---------- RecordNormalizer ----------

    [Fact]
    public void A_unit_Salesforce_would_accept_has_no_problems()
    {
        var unit = new UpsertUnit([AccountRecord("A1", "Acme")]);

        var normalized = RecordNormalizer.Normalize("A1", unit, Schemas, out var problems);

        problems.Should().BeEmpty();
        normalized.Key.Should().Be("A1");
        normalized.Records[0].Record.Should().BeSameAs(unit.Records[0]);
        normalized.Records[0].Fields.Should().ContainSingle().Which.Key.Name.Should().Be("Name");
    }

    [Fact]
    public void Every_problem_of_a_unit_is_reported_with_its_records_reference_id()
    {
        var unit = new UpsertUnit(
        [
            new SObjectRecord("acct", "Account", "External_Id__c", "A1",
                new Dictionary<string, object?> { ["Name"] = "too long", ["Nope__c"] = 1 }),
            new SObjectRecord("prof", "Profile__c", "External_Id__c", "P1", new Dictionary<string, object?>(),
                [new SObjectLookup("External_Id__c", "Account", "External_Id__c", "A1")])
        ]);

        RecordNormalizer.Normalize("A1", unit, Schemas, out var problems);

        problems.Select(p => (p.ReferenceId, p.Code)).Should().BeEquivalentTo(
        [
            ("acct", "STRING_TOO_LONG"), ("acct", "INVALID_FIELD"), ("prof", "INVALID_FIELD")
        ]);
        problems.Last().Message.Should().Contain("Profile__c.External_Id__c is not a lookup");
    }

    // ---------- ReadQueryBuilder ----------

    [Fact]
    public void A_query_selects_what_the_diff_and_the_lookups_need_and_is_chunked_by_100_values()
    {
        var units = Normalized(new UpsertUnit(
        [
            AccountRecord("A1", "Acme"),
            new SObjectRecord("prof", "Profile__c", "External_Id__c", "P1", new Dictionary<string, object?>(),
                [new SObjectLookup("Account__c", "Account", "External_Id__c", "A1")])
        ]));
        var ids = Enumerable.Range(0, 150).Select(i => $"A{i}").ToList();

        var queries = ReadQueryBuilder.Build(units,
            [new SoqlQuery("account", "External_Id__c", ids), new SoqlQuery("Profile__c", "External_Id__c", ["P1"]), new SoqlQuery("Profile__c", "External_Id__c", [])],
            Schemas);

        queries.Select(q => (q.Query.ReferenceId, q.SObject)).Should().Equal(("q0", "Account"), ("q1", "Account"), ("q2", "Profile__c"));
        queries[0].Query.Soql.Should().StartWith("SELECT Name,External_Id__c,Id FROM Account WHERE External_Id__c IN ('A0',");
        queries[1].Query.Soql.Should().Contain("'A149'").And.NotContain("'A99'");
        queries[2].Query.Soql.Should().Be("SELECT Account__c,External_Id__c,Id FROM Profile__c WHERE External_Id__c IN ('P1')");
    }

    [Fact]
    public void More_queries_than_a_composite_request_takes_is_refused_before_any_call()
    {
        var ids = Enumerable.Range(0, 501).Select(i => $"A{i}").ToList();

        var build = () => ReadQueryBuilder.Build(Normalized(new UpsertUnit([AccountRecord("A1", "Acme")])),
            [new SoqlQuery("Account", "External_Id__c", ids)], Schemas);

        build.Should().Throw<InvalidOperationException>().WithMessage("*6 SOQL queries*Sync:BatchSize*");
    }

    // ---------- CompositeWrite.Pack ----------

    [Fact]
    public void Units_that_need_the_same_collections_share_one_request_parents_and_creates_first()
    {
        var (Requests, TooLarge) = CompositeWrite.Pack(
        [
            new PlannedUnit("NEW", [Write("Account", create: true), Write("Profile__c", create: true, depth: 1)]),
            new PlannedUnit("OLD", [Write("Account", create: false), Write("Profile__c", create: false, depth: 1)]),
            new PlannedUnit("NEW2", [Write("Account", create: true)])
        ]);

        TooLarge.Should().BeEmpty();
        var request = Requests.Should().ContainSingle().Subject;
        request.Units.Select(u => u.Key).Should().Equal("NEW", "OLD", "NEW2");
        request.BuildRequests().Select(r => (r.ReferenceId, r.Create, r.Records.Count)).Should().Equal(
            ("Account_create", true, 2), ("Account_update", false, 1), ("Profile__c_create", true, 1), ("Profile__c_update", false, 1));
    }

    [Fact]
    public void A_unit_that_does_not_fit_the_requests_so_far_starts_another_and_stays_whole()
    {
        var (Requests, TooLarge) = CompositeWrite.Pack(
        [
            new PlannedUnit("A", [Write("Account", true), Write("Account", false), Write("Profile__c", true, 1), Write("Profile__c", false, 1)]),
            new PlannedUnit("B", [Write("Account_Relationship__c", true), Write("Account_Relationship__c", false)]),
            new PlannedUnit("C", [Write("Account", true)])
        ]);

        Requests.Select(r => r.Units.Select(u => u.Key)).Should().BeEquivalentTo<string[]>([["A", "C"], ["B"]],
            "B would be a 5th and 6th collection; C still fits the first request");
    }

    [Fact]
    public void A_collection_takes_200_records()
    {
        var units = Enumerable.Range(0, SalesforceRestClient.MaxCollectionRecords + 1)
            .Select(i => new PlannedUnit($"A{i}", [Write("Account", create: true)])).ToList();

        var (Requests, TooLarge) = CompositeWrite.Pack(units);

        Requests.Select(r => r.Units.Count).Should().Equal(SalesforceRestClient.MaxCollectionRecords, 1);
    }

    [Fact]
    public void A_unit_that_needs_more_collections_than_a_request_takes_fails_alone()
    {
        var tooMany = Enumerable.Range(0, SalesforceRestClient.MaxCompositeCollections + 1).Select(depth => Write("Account", true, depth)).ToList();

        var (Requests, TooLarge) = CompositeWrite.Pack([new PlannedUnit("BIG", tooMany), new PlannedUnit("OK", [Write("Account", true)])]);

        var failed = TooLarge.Should().ContainSingle().Subject;
        (failed.Key, failed.Outcome, failed.PrimaryError!.Code).Should().Be(("BIG", SyncOutcome.PermanentFailure, "LIMIT_EXCEEDED"));
        Requests.Should().ContainSingle().Which.Units.Should().ContainSingle().Which.Key.Should().Be("OK");
    }

    // ---------- Helpers ----------

    private static SObjectRecord AccountRecord(string key, string name)
    {
        return new("acct", "Account", "External_Id__c", key, new Dictionary<string, object?> { ["Name"] = name });
    }

    private static List<NormalizedUnit> Normalized(UpsertUnit unit)
    {
        var normalized = RecordNormalizer.Normalize(unit.Records[0].ExternalId, unit, Schemas, out var problems);
        problems.Should().BeEmpty();
        return [normalized];
    }

    private static PlannedWrite Write(string sobject, bool create, int depth = 0)
    {
        var record = new NormalizedRecord(
            new SObjectRecord("r", sobject, "External_Id__c", "X", new Dictionary<string, object?>()),
            new SObjectSchema(sobject, []), new Dictionary<SalesforceField, object?>(), []);
        return new PlannedWrite(record, create ? null : "001000000000001AAA", new Dictionary<string, object?>(), [],
            new HashSet<string>(), depth);
    }
}