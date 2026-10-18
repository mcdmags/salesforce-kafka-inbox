using System.Net;
using System.Text;
using SalesforceSync.Infrastructure.Salesforce;

namespace SalesforceSync.Tests;

/// <summary>The three calls the sync makes (<see cref="SalesforceRestClient"/>): what each sends and what it makes of the answer.</summary>
public sealed class ClientCallsTests
{
    private static readonly string Data = SalesforceStep.DataPath;

    [Fact]
    public async Task A_select_is_one_GET_of_the_escaped_query_and_returns_its_rows()
    {
        var (client, sent) = Client(HttpStatusCode.OK, /*lang=json,strict*/ """{"totalSize":1,"done":true,"records":[{"Id":"001X","Name":"Acme"}]}""");

        var rows = await client.SelectAsync("SELECT Id FROM Account WHERE Name = 'A b'", CancellationToken.None);

        (sent.Single().Method, sent.Single().Url).Should().Be(("GET", $"{Data}/query?q=SELECT%20Id%20FROM%20Account%20WHERE%20Name%20%3D%20%27A%20b%27"));
        rows.Single().GetProperty("Name").GetString().Should().Be("Acme");
    }

    [Fact]
    public async Task An_upsert_is_one_PATCH_of_the_records_by_external_id_that_never_stops_at_the_first_failure()
    {
        var (client, sent) = Client(HttpStatusCode.OK, /*lang=json,strict*/ """[{"id":"001X","success":true,"created":true,"errors":[]},{"success":false,"errors":[{"statusCode":"DUPLICATE_VALUE","message":"dup","fields":["ExternalId__c"]}]}]""");
        SObjectBody[] records = [new("Account") { ["ExternalId__c"] = "A1" }, new("Account") { ["ExternalId__c"] = "A2" }];

        var results = await client.UpsertAsync("Account", "ExternalId__c", records, CancellationToken.None);

        (sent.Single().Method, sent.Single().Url).Should().Be(("PATCH", $"{Data}/composite/sobjects/Account/ExternalId__c"));
        sent.Single().Body.Should().Be(/*lang=json,strict*/ """{"allOrNone":false,"records":[{"attributes":{"type":"Account"},"ExternalId__c":"A1"},{"attributes":{"type":"Account"},"ExternalId__c":"A2"}]}""");
        (results[0].Success, results[0].Id, results[0].Created).Should().Be((true, "001X", true));
        (results[1].Success, results[1].Errors.Single().Code).Should().Be((false, "DUPLICATE_VALUE"));
        results[1].Errors.Single().Fields.Should().Equal("ExternalId__c");
    }

    [Fact]
    public async Task A_delete_is_one_DELETE_with_the_Ids_in_the_URL()
    {
        var (client, sent) = Client(HttpStatusCode.OK, /*lang=json,strict*/ """[{"id":"001X","success":true,"errors":[]},{"id":"001Y","success":true,"errors":[]}]""");

        var results = await client.DeleteAsync(["001X", "001Y"], CancellationToken.None);

        (sent.Single().Method, sent.Single().Url, sent.Single().Body).Should().Be(("DELETE", $"{Data}/composite/sobjects?ids=001X,001Y&allOrNone=false", ""));
        results.Should().HaveCount(2).And.OnlyContain(r => r.Success);
    }

    [Fact]
    public async Task No_records_more_than_200_and_an_over_long_URL_are_refused_before_anything_is_sent()
    {
        var (client, sent) = Client(HttpStatusCode.OK, "[]");
        var tooMany = Enumerable.Range(0, 201).Select(i => new SObjectBody("Account") { ["ExternalId__c"] = $"A{i}" }).ToList();

        var none = () => client.UpsertAsync("Account", "ExternalId__c", [], CancellationToken.None);
        var over = () => client.UpsertAsync("Account", "ExternalId__c", tooMany, CancellationToken.None);
        var noIds = () => client.DeleteAsync([], CancellationToken.None);
        var longUrl = () => client.SelectAsync("SELECT " + new string('x', 20_000), CancellationToken.None);

        await none.Should().ThrowAsync<ArgumentOutOfRangeException>();
        await over.Should().ThrowAsync<ArgumentOutOfRangeException>();
        await noIds.Should().ThrowAsync<ArgumentOutOfRangeException>();
        await longUrl.Should().ThrowAsync<ArgumentException>();
        sent.Should().BeEmpty("a refused call is not a call");
    }

    [Fact]
    public async Task A_failed_call_throws_the_response_exception_with_Salesforces_error_code()
    {
        var (client, _) = Client(HttpStatusCode.BadRequest, /*lang=json,strict*/ """[{"errorCode":"INVALID_FIELD","message":"No such column"}]""");

        var select = () => client.SelectAsync("SELECT Nope FROM Account", CancellationToken.None);

        var thrown = (await select.Should().ThrowAsync<SalesforceResponseException>()).Which;
        (thrown.StatusCode, thrown.ErrorCode).Should().Be((HttpStatusCode.BadRequest, "INVALID_FIELD"));
    }

    private static (SalesforceRestClient Client, List<(string Method, string Url, string Body)> Sent) Client(HttpStatusCode status, string body)
    {
        var sent = new List<(string, string, string)>();
        return (SalesforceStep.Client(SalesforceStep.Http(new Answering(status, body, sent))), sent);
    }

    private sealed class Answering(HttpStatusCode status, string body, List<(string Method, string Url, string Body)> sent) : HttpMessageHandler
    {
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            sent.Add((request.Method.Method, request.RequestUri!.PathAndQuery, request.Content is null ? "" : await request.Content.ReadAsStringAsync(ct)));
            return new HttpResponseMessage(status) { Content = new StringContent(body, Encoding.UTF8, "application/json"), RequestMessage = request };
        }
    }
}
