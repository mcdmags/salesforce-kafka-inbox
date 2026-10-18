using Microsoft.Extensions.Options;
using SalesforceSync.Infrastructure.Salesforce;
using SalesforceSync.Sync;
using static SalesforceSync.Sync.Normalizer;
using static SalesforceSync.Sync.Reader;

namespace SalesforceSync.Tests;

/// <summary>
/// The pure steps of <see cref="SalesforceBatchSyncService"/>'s Salesforce step on their own: normalizing against the schema, building the
/// read and the limits of a call. No org, no HTTP.
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

    /// <summary>A lookup: AccountId holds the Account's Salesforce Id, so it can't be a foreign key.</summary>
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

    // ---------- Normalize ----------

    [Fact]
    public void A_unit_Salesforce_would_accept_has_no_problems()
    {
        var unit = new SyncUnit([AccountRecord("A1", "Acme")]);

        var normalized = Normalize("A1", unit, Schemas, out var problems);

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
                ForeignKeys: [new ForeignKey("AccountId__c", "Account", "Missing__c", "A1")]),
            new SObjectRecord("contact", "Contact", "ExternalId__c", "C1", new Dictionary<string, object?>(),
                ForeignKeys: [new ForeignKey("AccountId", "Account", "ExternalId__c", "A1")])
        ]);

        Normalize("A1", unit, Schemas, out var problems);

        problems.Select(p => (p.ReferenceId, p.Code)).Should().BeEquivalentTo(
        [
            ("acct", "STRING_TOO_LONG"), ("acct", "INVALID_FIELD"), ("prof", "INVALID_FIELD"), ("contact", "INVALID_FIELD")
        ]);
        problems.Should().Contain(p => p.Message!.Contains("Account.Missing__c doesn't exist"))
            .And.Contain(p => p.Message!.Contains("Contact.AccountId is a lookup, not a foreign key"));
    }

    [Fact]
    public void A_foreign_key_is_normalized_and_compared_like_any_text_field()
    {
        var unit = new SyncUnit([new SObjectRecord("prof", "Profile__c", "ExternalId__c", "P1",
            new Dictionary<string, object?> { ["AccountId__c"] = "A1" }, ForeignKeys: [new ForeignKey("AccountId__c", "Account", "ExternalId__c", "A1")])]);

        var normalized = Normalize("P1", unit, Schemas, out var problems);

        problems.Should().BeEmpty();
        normalized.Records[0].Fields.Should().ContainSingle().Which.Should().Be(
            new KeyValuePair<SalesforceField, object?>(Profile.Fields["AccountId__c"], "A1"));
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

        var normalized = Normalize("A1", unit, WithRecordTypes, out var problems);

        problems.Should().BeEmpty();
        normalized.Records[0].Fields.Should().Contain(
            new KeyValuePair<SalesforceField, object?>(WithRecordTypes["Account"].Fields["RecordTypeId"], "012000000000001AAA"),
            "developer names match without regard to case; the Id is normalized to 18 characters for the diff");
    }

    [Fact]
    public void A_record_without_a_record_type_leaves_RecordTypeId_alone()
    {
        var normalized = Normalize("A1", new SyncUnit([AccountRecord("A1", "Acme")]), WithRecordTypes, out var problems);

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

        Normalize("A1", unit, WithRecordTypes, out var problems);

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

        var (normalized, rejected) = new Normalizer(errors).Normalize(units, WithRecordTypes);

        normalized.Should().BeEmpty();
        rejected.Select(r => (r.Key, r.Outcome, r.PrimaryError!.Code)).Should().Equal(
            ("A1", SyncOutcome.TransientFailure, SalesforceErrors.InvalidRecordType),
            ("A2", SyncOutcome.PermanentFailure, "STRING_TOO_LONG"));
    }

    // ---------- BuildReadQueries ----------

    [Fact]
    public void A_query_selects_what_the_diff_needs_and_is_chunked_by_100_values()
    {
        var units = Normalized(new SyncUnit(
        [
            AccountRecord("A1", "Acme"),
            new SObjectRecord("contact", "Contact", "ExternalId__c", "C1", new Dictionary<string, object?>()),
            new SObjectRecord("prof", "Profile__c", "ExternalId__c", "P1", new Dictionary<string, object?> { ["AccountId__c"] = "A1" },
                ForeignKeys: [new ForeignKey("AccountId__c", "Account", "ExternalId__c", "A1")])
        ]));
        var ids = Enumerable.Range(0, 150).Select(i => $"A{i}").ToList();

        var queries = Reader().BuildReadQueries(units,
            [new SoqlQuery("account", "ExternalId__c", ids), new SoqlQuery("Contact", "ExternalId__c", ["C1"]), new SoqlQuery("Profile__c", "ExternalId__c", ["P1"]),
                new SoqlQuery("Profile__c", "ExternalId__c", [])],
            Schemas, maxValuesPerQuery: 100);

        queries.Select(q => q.SObject).Should().Equal("Account", "Account", "Contact", "Profile__c");
        queries[0].Soql.Should().StartWith("SELECT Name,ExternalId__c,Id FROM Account WHERE ExternalId__c IN ('A0',");
        queries[1].Soql.Should().Contain("'A149'").And.NotContain("'A99'");
        queries[2].Soql.Should().Be("SELECT ExternalId__c,Id FROM Contact WHERE ExternalId__c IN ('C1')");
        queries[3].Soql.Should().Be("SELECT AccountId__c,ExternalId__c,Id FROM Profile__c WHERE ExternalId__c IN ('P1')",
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

        queries.Select(q => ids.Count(id => q.Soql.Contains($"'{id}'"))).Should().Equal(3, 3, 1);
    }

    [Fact]
    public void The_URL_limit_wins_over_a_large_number_of_values()
    {
        var ids = Enumerable.Range(0, SalesforceRestClient.MaxQueryValues).Select(i => $"{i:D4}{new string('a', 96)}").ToList();

        var queries = BuildAccountRead(ids, SalesforceRestClient.MaxQueryValues);

        queries.Should().HaveCountGreaterThan(1).And.OnlyContain(q => SalesforceRestClient.FitsUrlLimit(SalesforceRestClient.QueryUrl(SalesforceStep.DataPath, q.Soql)));
        queries.Select(q => ids.Count(id => q.Soql.Contains($"'{id}'"))).Should().OnlyContain(n => n < SalesforceRestClient.MaxQueryValues)
            .And.Subject.Sum().Should().Be(ids.Count);
    }

    [Theory]
    [InlineData('a')] // 1 byte escaped
    [InlineData('é')] // 2 bytes of UTF-8, 6 escaped
    public void Long_ids_are_split_by_URL_length_each_query_fitting_and_full(char c)
    {
        var ids = Enumerable.Range(0, 100).Select(i => $"{i:D3}{new string(c, 247)}").ToList();

        var queries = BuildAccountRead(ids, maxValuesPerQuery: 100);

        queries.Should().HaveCountGreaterThan(1).And.OnlyContain(q => SalesforceRestClient.FitsUrlLimit(SalesforceRestClient.QueryUrl(SalesforceStep.DataPath, q.Soql)));
        var chunks = queries.Select(q => ids.Where(id => q.Soql.Contains($"'{id}'")).ToList()).ToList();
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

    // ---------- The client refuses before sending ----------

    [Fact]
    public async Task A_delete_of_as_many_Ids_as_a_call_takes_fits_a_URL_and_one_more_is_refused_before_sending()
    {
        var ids = Enumerable.Range(0, SalesforceRestClient.MaxCollectionRecords + 1).Select(i => $"003{i:D12}AAA").ToList();
        var url = $"{SalesforceStep.DataPath}/composite/sobjects?ids={string.Join(",", ids[..SalesforceRestClient.MaxCollectionRecords])}&allOrNone=false";

        SalesforceRestClient.FitsUrlLimit(url).Should().BeTrue("200 Ids of 18 characters are under 4 KB: {0} bytes", url.Length);

        var client = SalesforceStep.Client(SalesforceStep.Http(new ThrowingHandler()));
        var tooMany = () => client.DeleteAsync(ids, CancellationToken.None);
        await tooMany.Should().ThrowAsync<ArgumentOutOfRangeException>().WithMessage("201 records*");
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

    private static Reader Reader()
    {
        return new Reader(SalesforceStep.Client(SalesforceStep.Http(new ThrowingHandler())), null!);
    }

    private static SObjectRecord AccountRecord(string key, string name)
    {
        return new("acct", "Account", "ExternalId__c", key, new Dictionary<string, object?> { ["Name"] = name });
    }

    /// <summary>The read of Accounts by external id, selecting Name, ExternalId__c and Id.</summary>
    private static List<ReadQuery> BuildAccountRead(IReadOnlyList<string> ids, int maxValuesPerQuery)
    {
        return Reader().BuildReadQueries(Normalized(new SyncUnit([AccountRecord("A1", "Acme")])),
            [new SoqlQuery("Account", "ExternalId__c", ids)], Schemas, maxValuesPerQuery);
    }

    private static List<NormalizedUnit> Normalized(SyncUnit unit)
    {
        var normalized = Normalize(unit.Records[0].ExternalId, unit, Schemas, out var problems);
        problems.Should().BeEmpty();
        return [normalized];
    }
}
