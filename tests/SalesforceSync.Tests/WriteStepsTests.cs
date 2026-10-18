using Microsoft.Extensions.Options;
using SalesforceSync.Infrastructure.Salesforce;
using SalesforceSync.Sync;
using SalesforceSync.Sync.Write;

namespace SalesforceSync.Tests;

/// <summary>
/// The pure steps of <see cref="SalesforceBatchSyncService"/>'s Salesforce step on their own: normalizing against the schema, building the
/// read and packing writes into Composite requests. No org, no HTTP.
/// </summary>
public sealed class WriteStepsTests
{
    private static readonly SObjectSchema Account = new("Account",
    [
        new SalesforceField("Id", "id", 18, 0, 0, false, false, false),
        new SalesforceField("ExternalId__c", "string", 255, 0, 0, true, true, true),
        new SalesforceField("Name", "string", 5, 0, 0, true, true, false)
    ]);

    /// <summary>A foreign key: AccountId__c holds the Account's external id.</summary>
    private static readonly SObjectSchema Profile = new("Profile__c",
    [
        new SalesforceField("Id", "id", 18, 0, 0, false, false, false),
        new SalesforceField("ExternalId__c", "string", 255, 0, 0, true, true, true),
        new SalesforceField("AccountId__c", "string", 255, 0, 0, true, true, true)
    ]);

    /// <summary>A lookup: AccountId holds the Account's Salesforce Id.</summary>
    private static readonly SObjectSchema Contact = new("Contact",
    [
        new SalesforceField("Id", "id", 18, 0, 0, false, false, false),
        new SalesforceField("ExternalId__c", "string", 255, 0, 0, true, true, true),
        new SalesforceField("AccountId", "reference", 18, 0, 0, true, true, true)
    ]);

    private static readonly Dictionary<string, SObjectSchema> Schemas = new(StringComparer.OrdinalIgnoreCase)
    {
        ["Account"] = Account,
        ["Profile__c"] = Profile,
        ["Contact"] = Contact
    };

    // ---------- RecordNormalizer ----------

    [Fact]
    public void A_unit_Salesforce_would_accept_has_no_problems()
    {
        var unit = new SyncUnit([AccountRecord("A1", "Acme")]);

        var normalized = RecordNormalizer.Normalize("A1", unit, Schemas, out var problems);

        problems.Should().BeEmpty();
        normalized.Key.Should().Be("A1");
        normalized.Records[0].Record.Should().BeSameAs(unit.Records[0]);
        normalized.Records[0].Fields.Should().ContainSingle().Which.Key.Name.Should().Be("Name");
    }

    [Fact]
    public void Every_problem_of_a_unit_is_reported_with_its_records_reference_id()
    {
        var unit = new SyncUnit(
        [
            new SObjectRecord("acct", "Account", "ExternalId__c", "A1",
                new Dictionary<string, object?> { ["Name"] = "too long", ["Nope__c"] = 1 }),
            new SObjectRecord("prof", "Profile__c", "ExternalId__c", "P1", new Dictionary<string, object?>(),
                [new SObjectLookup("ExternalId__c", "Account", "ExternalId__c", "A1")]),
            new SObjectRecord("contact", "Contact", "ExternalId__c", "C1", new Dictionary<string, object?>(),
                ForeignKeys: [new SObjectLookup("AccountId", "Account", "ExternalId__c", "A1")])
        ]);

        RecordNormalizer.Normalize("A1", unit, Schemas, out var problems);

        problems.Select(p => (p.ReferenceId, p.Code)).Should().BeEquivalentTo(
        [
            ("acct", "STRING_TOO_LONG"), ("acct", "INVALID_FIELD"), ("prof", "INVALID_FIELD"), ("contact", "INVALID_FIELD")
        ]);
        problems.Should().Contain(p => p.Message!.Contains("Profile__c.ExternalId__c is not a lookup"))
            .And.Contain(p => p.Message!.Contains("Contact.AccountId is a lookup, not a foreign key"));
    }

    [Fact]
    public void A_foreign_key_is_normalized_and_compared_like_any_text_field()
    {
        var unit = new SyncUnit([new SObjectRecord("prof", "Profile__c", "ExternalId__c", "P1",
            new Dictionary<string, object?> { ["AccountId__c"] = "A1" }, ForeignKeys: [new SObjectLookup("AccountId__c", "Account", "ExternalId__c", "A1")])]);

        var normalized = RecordNormalizer.Normalize("P1", unit, Schemas, out var problems);

        problems.Should().BeEmpty();
        normalized.Records[0].Fields.Should().ContainSingle().Which.Should().Be(
            new KeyValuePair<SalesforceField, object?>(Profile.Fields["AccountId__c"], "A1"));
        normalized.Records[0].Lookups.Should().BeEmpty("never resolved to an Id");
    }

    // ---------- Record types ----------

    private static readonly Dictionary<string, SObjectSchema> WithRecordTypes = new(StringComparer.OrdinalIgnoreCase)
    {
        ["Account"] = new("Account",
        [
            new SalesforceField("Id", "id", 18, 0, 0, false, false, false),
            new SalesforceField("ExternalId__c", "string", 255, 0, 0, true, true, true),
            new SalesforceField("Name", "string", 5, 0, 0, true, true, false),
            new SalesforceField("RecordTypeId", "reference", 18, 0, 0, true, true, true)
        ],
        [
            new SalesforceRecordType("Master", "012000000000000AAA", true, false),
            new SalesforceRecordType("Business", "012000000000001", true, true),
            new SalesforceRecordType("Retired", "012000000000002AAA", false, true)
        ])
    };

    [Fact]
    public void The_record_type_becomes_RecordTypeId_a_field_like_any_other()
    {
        var unit = new SyncUnit([AccountRecord("A1", "Acme") with { RecordType = "business" }]);

        var normalized = RecordNormalizer.Normalize("A1", unit, WithRecordTypes, out var problems);

        problems.Should().BeEmpty();
        normalized.Records[0].Fields.Should().Contain(
            new KeyValuePair<SalesforceField, object?>(WithRecordTypes["Account"].Fields["RecordTypeId"], "012000000000001AAA"),
            "developer names match without regard to case; the Id is normalized to 18 characters for the diff");
    }

    [Fact]
    public void A_record_without_a_record_type_leaves_RecordTypeId_alone()
    {
        var normalized = RecordNormalizer.Normalize("A1", new SyncUnit([AccountRecord("A1", "Acme")]), WithRecordTypes, out var problems);

        problems.Should().BeEmpty();
        normalized.Records[0].Fields.Keys.Select(f => f.Name).Should().Equal("Name");
    }

    [Theory]
    [InlineData("Nope", "doesn't exist")]
    [InlineData("Retired", "is not active")]
    [InlineData("Master", "is not available to the running user")]
    public void A_record_type_the_org_does_not_offer_is_a_problem(string developerName, string why)
    {
        var unit = new SyncUnit([AccountRecord("A1", "Acme") with { RecordType = developerName }]);

        RecordNormalizer.Normalize("A1", unit, WithRecordTypes, out var problems);

        var problem = problems.Should().ContainSingle().Subject;
        (problem.ReferenceId, problem.Code).Should().Be(("acct", SalesforceErrors.InvalidRecordType));
        problem.Message.Should().Be($"Record type '{developerName}' of Account {why}");
    }

    [Fact]
    public void A_record_type_problem_alone_is_transient_with_another_problem_permanent()
    {
        var errors = new SalesforceErrors(Options.Create(new SalesforceOptions()));
        (string, SyncUnit)[] units =
        [
            ("A1", new SyncUnit([AccountRecord("A1", "Acme") with { RecordType = "Nope" }])),
            ("A2", new SyncUnit([AccountRecord("A2", "too long") with { RecordType = "Nope" }]))
        ];

        var (normalized, rejected) = RecordNormalizer.Normalize(units, WithRecordTypes, errors);

        normalized.Should().BeEmpty();
        rejected.Select(r => (r.Key, r.Outcome, r.PrimaryError!.Code)).Should().Equal(
            ("A1", SyncOutcome.TransientFailure, SalesforceErrors.InvalidRecordType),
            ("A2", SyncOutcome.PermanentFailure, "STRING_TOO_LONG"));
    }

    // ---------- ReadQueryBuilder ----------

    [Fact]
    public void A_query_selects_what_the_diff_and_the_lookups_need_and_is_chunked_by_100_values()
    {
        var units = Normalized(new SyncUnit(
        [
            AccountRecord("A1", "Acme"),
            new SObjectRecord("contact", "Contact", "ExternalId__c", "C1", new Dictionary<string, object?>(),
                [new SObjectLookup("AccountId", "Account", "ExternalId__c", "A1")]),
            new SObjectRecord("prof", "Profile__c", "ExternalId__c", "P1", new Dictionary<string, object?> { ["AccountId__c"] = "A1" },
                ForeignKeys: [new SObjectLookup("AccountId__c", "Account", "ExternalId__c", "A1")])
        ]));
        var ids = Enumerable.Range(0, 150).Select(i => $"A{i}").ToList();

        var queries = ReadQueryBuilder.Build(units,
            [new SoqlQuery("account", "ExternalId__c", ids), new SoqlQuery("Contact", "ExternalId__c", ["C1"]), new SoqlQuery("Profile__c", "ExternalId__c", ["P1"]),
                new SoqlQuery("Profile__c", "ExternalId__c", [])],
            Schemas, maxValuesPerQuery: 100, SalesforceStep.DataPath);

        queries.Select(q => (q.Query.ReferenceId, q.SObject)).Should().Equal(("q0", "Account"), ("q1", "Account"), ("q2", "Contact"), ("q3", "Profile__c"));
        queries[0].Query.Soql.Should().StartWith("SELECT Name,ExternalId__c,Id FROM Account WHERE ExternalId__c IN ('A0',");
        queries[1].Query.Soql.Should().Contain("'A149'").And.NotContain("'A99'");
        queries[2].Query.Soql.Should().Be("SELECT AccountId,ExternalId__c,Id FROM Contact WHERE ExternalId__c IN ('C1')");
        queries[3].Query.Soql.Should().Be("SELECT AccountId__c,ExternalId__c,Id FROM Profile__c WHERE ExternalId__c IN ('P1')",
            "a foreign key is selected as the field it is");
    }

    [Fact]
    public void A_hundred_short_ids_are_one_query()
    {
        var ids = Enumerable.Range(0, 100).Select(i => $"A{i}").ToList();

        var queries = BuildAccountRead(ids, maxValuesPerQuery: 100);

        queries.Should().ContainSingle();
    }

    [Fact]
    public void Queries_take_at_most_the_given_number_of_values()
    {
        var ids = Enumerable.Range(0, 7).Select(i => $"A{i}").ToList();

        var queries = BuildAccountRead(ids, maxValuesPerQuery: 3);

        queries.Select(q => ids.Count(id => q.Query.Soql.Contains($"'{id}'"))).Should().Equal(3, 3, 1);
    }

    [Fact]
    public void The_URL_limit_wins_over_a_large_number_of_values()
    {
        var ids = Enumerable.Range(0, SalesforceRestClient.MaxQueryValues).Select(i => $"{i:D4}{new string('a', 96)}").ToList();

        var queries = BuildAccountRead(ids, SalesforceRestClient.MaxQueryValues);

        queries.Should().HaveCountGreaterThan(1).And.OnlyContain(q => SalesforceRestClient.FitsUrlLimit(SalesforceRestClient.QueryUrl(SalesforceStep.DataPath, q.Query.Soql)));
        queries.Select(q => ids.Count(id => q.Query.Soql.Contains($"'{id}'"))).Should().OnlyContain(n => n < SalesforceRestClient.MaxQueryValues)
            .And.Subject.Sum().Should().Be(ids.Count);
    }

    [Theory]
    [InlineData('a')] // 1 byte escaped
    [InlineData('é')] // 2 bytes of UTF-8, 6 escaped
    public void Long_ids_are_split_by_URL_length_each_query_fitting_and_full(char c)
    {
        var ids = Enumerable.Range(0, 100).Select(i => $"{i:D3}{new string(c, 247)}").ToList();

        var queries = BuildAccountRead(ids, maxValuesPerQuery: 100);

        queries.Should().HaveCountGreaterThan(1).And.OnlyContain(q => SalesforceRestClient.FitsUrlLimit(SalesforceRestClient.QueryUrl(SalesforceStep.DataPath, q.Query.Soql)));
        var chunks = queries.Select(q => ids.Where(id => q.Query.Soql.Contains($"'{id}'")).ToList()).ToList();
        chunks.SelectMany(c => c).Should().Equal(ids, "every id is read once, in order");
        chunks.Zip(chunks.Skip(1)).Select(p => SalesforceRestClient.FitsUrlLimit(SalesforceRestClient.QueryUrl(SalesforceStep.DataPath, 
                SalesforceRestClient.Soql("Account", ["Name", "ExternalId__c", "Id"], "ExternalId__c", [.. p.First, p.Second[0]]))))
            .Should().AllBeEquivalentTo(false, "a query is closed only when the next id would not fit");
    }

    [Fact]
    public void A_value_whose_query_alone_does_not_fit_a_URL_fails_naming_the_read()
    {
        var build = () => BuildAccountRead(["A1", new string('x', SalesforceRestClient.MaxUrlBytes)], maxValuesPerQuery: 100);

        build.Should().Throw<InvalidOperationException>().WithMessage("The read of Account by ExternalId__c does not fit a URL*");
    }

    [Fact]
    public void A_URL_fits_up_to_the_budget_left_after_the_headers()
    {
        SalesforceRestClient.MaxUrlBytes.Should().Be(SalesforceRestClient.MaxUriAndHeadersBytes - SalesforceRestClient.HeaderAllowanceBytes);
        SalesforceRestClient.FitsUrlLimit(new string('a', SalesforceRestClient.MaxUrlBytes)).Should().BeTrue();
        SalesforceRestClient.FitsUrlLimit(new string('a', SalesforceRestClient.MaxUrlBytes + 1)).Should().BeFalse();
        SalesforceRestClient.FitsUrlLimit(new string('a', SalesforceRestClient.MaxUrlBytes - 1) + "é").Should().BeFalse("bytes count, not characters");
    }

    // ---------- CompositeWrite.Pack ----------

    [Fact]
    public void Units_that_need_the_same_collections_share_one_request_parents_first()
    {
        var (requests, tooLarge) = CompositeWrite.Pack(
        [
            new PlannedUnit("NEW", [Write("Account", create: true), Write("Profile__c", create: true, depth: 1)]),
            new PlannedUnit("OLD", [Write("Account", create: false), Write("Profile__c", create: false, depth: 1)]),
            new PlannedUnit("NEW2", [Write("Account", create: true)])
        ], MaxRecordsPerWrite);

        tooLarge.Should().BeEmpty();
        var request = requests.Should().ContainSingle().Subject;
        request.Units.Select(u => u.Key).Should().Equal("NEW", "OLD", "NEW2");
        request.Upserts().Select(r => (r.ReferenceId, r.Records.Count)).Should().Equal([("Account", 3), ("Profile__c", 2)],
            "one upsert per level and object, creates and updates together");
    }

    [Fact]
    public void A_unit_that_does_not_fit_the_requests_so_far_starts_another_and_stays_whole()
    {
        var (requests, _) = CompositeWrite.Pack(
        [
            new PlannedUnit("A", [Write("Account", true), Write("Account", false, 1), Write("Profile__c", true, 1), Write("Profile__c", false, 2)]),
            new PlannedUnit("B", [Write("Account_Relationship__c", true), Write("Account_Relationship__c", false, 1)]),
            new PlannedUnit("C", [Write("Account", true)])
        ], MaxRecordsPerWrite);

        requests.Select(r => r.Units.Select(u => u.Key)).Should().BeEquivalentTo<string[]>([["A", "C"], ["B"]],
            "B would be a 5th and 6th collection; C still fits the first request");
    }

    [Fact]
    public void A_collection_takes_up_to_200_records()
    {
        var units = Enumerable.Range(0, SalesforceRestClient.MaxCollectionRecords + 1)
            .Select(i => new PlannedUnit($"A{i}", [Write("Account", create: true)])).ToList();

        var (requests, _) = CompositeWrite.Pack(units, SalesforceRestClient.MaxCollectionRecords);

        requests.Select(r => r.Units.Count).Should().Equal(SalesforceRestClient.MaxCollectionRecords, 1);
    }

    [Fact]
    public void Writes_are_split_at_the_given_number_of_records_per_collection()
    {
        var (requests, tooLarge) = CompositeWrite.Pack(
            [.. Enumerable.Range(0, 3).Select(i => new PlannedUnit($"A{i}", [Write("Account", create: true)]))], maxRecordsPerWrite: 2);

        tooLarge.Should().BeEmpty();
        requests.Select(r => r.Upserts().Single().Records.Count).Should().Equal(2, 1);
    }

    [Fact]
    public void A_unit_with_more_records_of_an_object_than_a_collection_takes_fails_alone()
    {
        var (requests, tooLarge) = CompositeWrite.Pack(
            [new PlannedUnit("BIG", [Write("Account", true), Write("Account", false)]), new PlannedUnit("OK", [Write("Account", true)])],
            maxRecordsPerWrite: 1);

        tooLarge.Should().ContainSingle().Which.Key.Should().Be("BIG");
        requests.Should().ContainSingle().Which.Units.Should().ContainSingle().Which.Key.Should().Be("OK");
    }

    [Fact]
    public void A_unit_that_needs_more_collections_than_a_request_takes_fails_alone()
    {
        var tooMany = Enumerable.Range(0, SalesforceRestClient.MaxCompositeCollections + 1).Select(depth => Write("Account", true, depth)).ToList();

        var (requests, tooLarge) = CompositeWrite.Pack([new PlannedUnit("BIG", tooMany), new PlannedUnit("OK", [Write("Account", true)])],
            MaxRecordsPerWrite);

        var failed = tooLarge.Should().ContainSingle().Subject;
        (failed.Key, failed.Outcome, failed.PrimaryError!.Code).Should().Be(("BIG", SyncOutcome.PermanentFailure, "LIMIT_EXCEEDED"));
        requests.Should().ContainSingle().Which.Units.Should().ContainSingle().Which.Key.Should().Be("OK");
    }

    // ---------- Collections delete ----------

    [Fact]
    public async Task A_delete_of_as_many_Ids_as_a_collection_takes_fits_a_call_and_one_more_is_refused_before_sending()
    {
        var ids = Enumerable.Range(0, SalesforceRestClient.MaxCollectionRecords + 1).Select(i => $"003{i:D12}AAA").ToList();
        var full = new CollectionDelete("delete", ids[..SalesforceRestClient.MaxCollectionRecords]);

        SalesforceRestClient.FitsUrlLimit(SalesforceStep.DataPath + full.Path).Should().BeTrue("200 Ids of 18 characters are under 4 KB: {0} bytes", (SalesforceStep.DataPath + full.Path).Length);
        full.Path.Should().StartWith("/composite/sobjects?ids=003000000000000AAA,003000000000001AAA,")
            .And.EndWith("&allOrNone=false");

        var client = new SalesforceRestClient(SalesforceStep.Http(new ThrowingHandler()));
        var tooMany = () => client.WriteAsync([new CollectionDelete("delete", ids)], CancellationToken.None);
        await tooMany.Should().ThrowAsync<ArgumentOutOfRangeException>().WithMessage("delete: 201 records*");
    }

    /// <summary>A handler the test must never reach: the client refuses before sending.</summary>
    private sealed class ThrowingHandler : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            throw new InvalidOperationException("Nothing may be sent");
        }
    }

    // ---------- Helpers ----------

    private const int MaxRecordsPerWrite = 180;

    private static SObjectRecord AccountRecord(string key, string name)
    {
        return new("acct", "Account", "ExternalId__c", key, new Dictionary<string, object?> { ["Name"] = name });
    }

    /// <summary>The read of Accounts by external id, selecting Name, ExternalId__c and Id.</summary>
    private static List<ReadQuery> BuildAccountRead(IReadOnlyList<string> ids, int maxValuesPerQuery)
    {
        return ReadQueryBuilder.Build(Normalized(new SyncUnit([AccountRecord("A1", "Acme")])),
            [new SoqlQuery("Account", "ExternalId__c", ids)], Schemas, maxValuesPerQuery, SalesforceStep.DataPath);
    }

    private static List<NormalizedUnit> Normalized(SyncUnit unit)
    {
        var normalized = RecordNormalizer.Normalize(unit.Records[0].ExternalId, unit, Schemas, out var problems);
        problems.Should().BeEmpty();
        return [normalized];
    }

    private static PlannedWrite Write(string sobject, bool create, int depth = 0)
    {
        var record = new NormalizedRecord(
            new SObjectRecord("r", sobject, "ExternalId__c", "X", new Dictionary<string, object?>()),
            new SObjectSchema(sobject, [new SalesforceField("ExternalId__c", "string", 255, 0, 0, true, true, true)]),
            new Dictionary<SalesforceField, object?>(), []);
        return new PlannedWrite(record, create ? null : "001000000000001AAA", new Dictionary<string, object?>(), [], [],
            new HashSet<string>(), depth);
    }
}