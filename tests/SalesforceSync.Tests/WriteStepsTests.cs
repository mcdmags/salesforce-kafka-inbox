using Microsoft.Extensions.Options;
using SalesforceSync.FakeSalesforce;
using SalesforceSync.Infrastructure.Salesforce;
using SalesforceSync.Sync;
using static SalesforceSync.Sync.Normalizer;
using static SalesforceSync.Sync.Reader;

namespace SalesforceSync.Tests;

/// <summary>
/// The pure steps of <see cref="SalesforceBatchSyncService"/>'s Salesforce step on their own: normalizing, building the
/// read and the limits of a call. No HTTP except to the in-memory org for record types.
/// </summary>
public sealed class WriteStepsTests
{
    private readonly FakeSalesforceStore _org = new(new FakeSalesforceOptions { EnforceDailyLimit = false });

    // ---------- Normalize ----------

    [Fact]
    public async Task Values_are_put_in_canonical_form_and_the_unit_is_kept_as_it_is()
    {
        var unit = new SyncUnit([new SObjectRecord("acct", "Account", "ExternalId__c", "A1", new Dictionary<string, object?>
        {
            ["Name"] = "Acme", ["Description"] = "", ["NumberOfEmployees"] = 250, ["Last_Contacted__c"] = new DateTimeOffset(2026, 10, 13, 3, 2, 3, 456, TimeSpan.FromHours(2))
        })]);

        var (normalized, rejected) = await Normalizer().NormalizeAsync([("A1", unit)], CancellationToken.None);

        rejected.Should().BeEmpty();
        normalized.Single().Key.Should().Be("A1");
        var record = normalized.Single().Records.Single();
        record.Record.Should().BeSameAs(unit.Records[0]);
        record.Fields.Should().BeEquivalentTo(new Dictionary<string, object?>
        {
            ["Name"] = "Acme",
            ["Description"] = null,
            ["NumberOfEmployees"] = 250m,
            ["Last_Contacted__c"] = new DateTimeOffset(2026, 10, 13, 1, 2, 3, TimeSpan.Zero)
        });
    }

    // ---------- Record types ----------

    [Fact]
    public async Task The_record_type_becomes_RecordTypeId_a_field_like_any_other()
    {
        var unit = new SyncUnit([AccountRecord("A1", "Acme") with { RecordType = "business" }]);

        var (normalized, rejected) = await Normalizer().NormalizeAsync([("A1", unit)], CancellationToken.None);

        rejected.Should().BeEmpty();
        normalized.Single().Records.Single().Fields.Should().Contain(
            new KeyValuePair<string, object?>("RecordTypeId", FakeSchema.RecordTypeIdOf("Account", "Business")),
            "developer names match without regard to case");
    }

    [Fact]
    public async Task A_record_without_a_record_type_leaves_RecordTypeId_alone()
    {
        var (normalized, rejected) = await Normalizer().NormalizeAsync([("A1", new SyncUnit([AccountRecord("A1", "Acme")]))], CancellationToken.None);

        rejected.Should().BeEmpty();
        normalized.Single().Records.Single().Fields.Keys.Should().Equal("Name");
        _org.GetStats().Queries.Should().Be(0, "no record type to look up");
    }

    [Theory]
    [InlineData("Nope")]
    [InlineData("Master")] // every object has it, but it is not a RecordType row
    public async Task A_record_type_the_org_does_not_offer_rejects_the_unit_as_transient(string developerName)
    {
        var unit = new SyncUnit([AccountRecord("A1", "Acme") with { RecordType = developerName }]);

        var (normalized, rejected) = await Normalizer().NormalizeAsync([("A1", unit), ("A2", new SyncUnit([AccountRecord("A2", "Other")]))], CancellationToken.None);

        normalized.Select(n => n.Key).Should().Equal(["A2"], "the rest of the batch carries on");
        var result = rejected.Should().ContainSingle().Subject;
        (result.Key, result.Outcome).Should().Be(("A1", SyncOutcome.TransientFailure));
        var problem = result.Errors.Should().ContainSingle().Subject;
        (problem.ReferenceId, problem.Code).Should().Be(("acct", SalesforceErrors.InvalidRecordType));
        problem.Message.Should().Be($"Record type '{developerName}' of Account doesn't exist or is not active");
    }

    [Fact]
    public async Task Record_types_are_read_once_per_object_and_again_once_invalidated()
    {
        var normalizer = Normalizer();
        static SyncUnit Business(string key)
        {
            return new SyncUnit([AccountRecord(key, "Acme") with { RecordType = "Business" }]);
        }

        await normalizer.NormalizeAsync([("A1", Business("A1")), ("A2", Business("A2"))], CancellationToken.None);
        await normalizer.NormalizeAsync([("A3", Business("A3"))], CancellationToken.None);
        _org.GetStats().Queries.Should().Be(1, "cached");

        normalizer.InvalidateRecordTypes([("A1", Business("A1"))],
            [new SyncResult("A1", SyncOutcome.TransientFailure, [new SalesforceError("acct", SalesforceErrors.InvalidRecordType, "refused")])]);
        await normalizer.NormalizeAsync([("A4", Business("A4"))], CancellationToken.None);

        _org.GetStats().Queries.Should().Be(2, "read again");
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
            [new SoqlQuery("Account", "ExternalId__c", ids), new SoqlQuery("Contact", "ExternalId__c", ["C1"]), new SoqlQuery("Profile__c", "ExternalId__c", ["P1"]),
                new SoqlQuery("Profile__c", "ExternalId__c", [])],
            maxValuesPerQuery: 100);

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
        return new Reader(SalesforceStep.Client(SalesforceStep.Http(new ThrowingHandler())));
    }

    private static SObjectRecord AccountRecord(string key, string name)
    {
        return new("acct", "Account", "ExternalId__c", key, new Dictionary<string, object?> { ["Name"] = name });
    }

    /// <summary>The read of Accounts by external id, selecting Name, ExternalId__c and Id.</summary>
    private static List<ReadQuery> BuildAccountRead(IReadOnlyList<string> ids, int maxValuesPerQuery)
    {
        return Reader().BuildReadQueries(Normalized(new SyncUnit([AccountRecord("A1", "Acme")])),
            [new SoqlQuery("Account", "ExternalId__c", ids)], maxValuesPerQuery);
    }

    private static List<NormalizedUnit> Normalized(SyncUnit unit)
    {
        return [new NormalizedUnit(unit.Records[0].ExternalId,
            [.. unit.Records.Select(r => new NormalizedRecord(r, r.Fields.ToDictionary(f => f.Key, f => FieldValues.Normalize(f.Value))))])];
    }

    private Normalizer Normalizer()
    {
        var http = SalesforceStep.Http(new FakeSalesforceHandler(_org));
        return new Normalizer(SalesforceStep.RecordTypes(http, Options.Create(new SalesforceOptions())));
    }
}