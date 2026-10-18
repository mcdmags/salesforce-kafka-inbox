using System.Net;
using System.Text;
using System.Text.Json.Nodes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using SalesforceSync.FakeSalesforce;
using SalesforceSync.Infrastructure.Salesforce;

namespace SalesforceSync.Tests;

/// <summary>
/// The mock org against what the REST API Developer Guide (and, for per-record codes, the SOAP API Developer Guide)
/// documents, over HTTP as a client sees it: sObject Collections deletes and upserts, query pages and
/// OAuth sessions. The engine's own use of them is covered by <see cref="SalesforceBatchSyncServiceTests"/>.
/// </summary>
public sealed class FakeSalesforceTests
{
    private const string Data = "/services/data/v62.0";

    private readonly FakeSalesforceStore _org = new(new FakeSalesforceOptions { EnforceDailyLimit = false });
    private readonly HttpClient _http;

    public FakeSalesforceTests()
    {
        _http = SalesforceStep.Http(new FakeSalesforceHandler(_org));
    }

    // ---------- sObject Collections delete ----------

    [Fact]
    public async Task A_delete_answers_200_with_a_result_per_Id_in_order_and_a_malformed_Id_fails_alone_without_an_id()
    {
        var (a1, a2) = (await UpsertAccountsAsync("A1"), await UpsertAccountsAsync("A2"));

        var (status, results) = await DeleteAsync($"{a1},001BAD,{a2}");

        status.Should().Be(HttpStatusCode.OK);
        results.Select(r => ((string?)r!["id"], (bool)r["success"]!)).Should().Equal((a1, true), (null, false), (a2, true));
        results[1]!.AsObject().ContainsKey("id").Should().BeFalse();
        Code(results[1]).Should().Be("MALFORMED_ID");
        _org.FindByExternalId("Account", "A1").Should().BeNull();
    }

    [Fact]
    public async Task A_delete_tells_an_Id_in_the_Recycle_Bin_from_one_that_never_existed_and_from_one_with_a_wrong_suffix()
    {
        var a1 = await UpsertAccountsAsync("A1");
        await DeleteAsync(a1);
        var unknown = FakeRecords.Id18("001FAKE99999999");

        var (_, results) = await DeleteAsync($"{a1},{unknown},{unknown[..15]}ZZZ");

        results.Select(Code).Should().Equal("ENTITY_IS_DELETED", "INVALID_CROSS_REFERENCE_KEY", "MALFORMED_ID");
        ((string?)results[0]!["id"]).Should().Be(a1, "a failed delete names the Id it was for");
    }

    [Fact]
    public async Task A_delete_by_the_15_character_Id_deletes_the_record_and_answers_its_18_character_Id()
    {
        var a1 = await UpsertAccountsAsync("A1");

        var (_, results) = await DeleteAsync(a1[..15]);

        ((string?)results.Single()!["id"], (bool)results.Single()!["success"]!).Should().Be((a1, true));
    }

    [Fact]
    public async Task A_delete_with_allOrNone_true_rolls_back_every_record_when_one_fails_and_still_answers_200()
    {
        var a1 = await UpsertAccountsAsync("A1");

        var (status, results) = await DeleteAsync($"{a1},001BAD", allOrNone: true);

        status.Should().Be(HttpStatusCode.OK);
        results.Select(Code).Should().Equal("ALL_OR_NONE_OPERATION_ROLLED_BACK", "MALFORMED_ID");
        _org.FindByExternalId("Account", "A1").Should().NotBeNull("rolled back");
        _org.GetStats().Deletes.Should().Be(0);
    }

    [Fact]
    public async Task A_delete_of_more_than_200_Ids_fails_as_a_whole()
    {
        var ids = string.Join(",", Enumerable.Range(0, 201).Select(i => FakeRecords.Id18($"001FAKE{i:D8}")));

        using var res = await _http.DeleteAsync($"{Data}/composite/sobjects?ids={ids}&allOrNone=false");

        res.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        ((string?)JsonNode.Parse(await res.Content.ReadAsStringAsync())![0]!["errorCode"]).Should().Be("EXCEEDED_ID_LIMIT");
    }

    [Fact]
    public async Task A_deleted_record_is_not_returned_by_a_query_and_upserting_its_external_id_again_creates_a_new_record()
    {
        var a1 = await UpsertAccountsAsync("A1");
        await DeleteAsync(a1);

        var found = await SalesforceStep.Client(_http).SelectAsync("SELECT Id FROM Account WHERE ExternalId__c IN ('A1')", CancellationToken.None);
        var recreated = (await UpsertAsync("Account", Account("A1"))).Single()!;

        found.Should().BeEmpty("Query leaves out the Recycle Bin");
        ((bool)recreated["created"]!).Should().BeTrue("an upsert matches no record in the Recycle Bin");
        ((string?)recreated["id"]).Should().NotBe(a1);
    }

    [Fact]
    public async Task Deleting_an_Account_deletes_the_Contacts_its_standard_lookup_links_but_not_those_a_Text_key_names()
    {
        var a1 = await UpsertAccountsAsync("A1");
        await UpsertAsync("Contact",
            new JsonObject { ["attributes"] = Type("Contact"), ["ExternalId__c"] = "LOOKUP", ["LastName"] = "L", ["AccountId"] = a1 },
            new JsonObject { ["attributes"] = Type("Contact"), ["ExternalId__c"] = "TEXT", ["LastName"] = "T", ["AccountId__c"] = "A1" });

        await DeleteAsync(a1);

        _org.FindByExternalId("Contact", "LOOKUP").Should().BeNull("Contact.AccountId cascades");
        _org.FindByExternalId("Contact", "TEXT").Should().NotBeNull("a Text field is no relationship");
    }

    [Fact]
    public async Task An_Account_that_Text_keys_name_is_deleted_unless_the_org_is_set_to_refuse_it()
    {
        var a1 = await UpsertAccountsAsync("A1");
        await UpsertAsync("Contact", new JsonObject { ["attributes"] = Type("Contact"), ["ExternalId__c"] = "C1", ["LastName"] = "C", ["AccountId__c"] = "A1" });
        _org.Options.RefuseDeletingReferencedRecords = true;

        var (_, refused) = await DeleteAsync(a1);
        _org.Options.RefuseDeletingReferencedRecords = false;
        var (_, deleted) = await DeleteAsync(a1);

        Code(refused.Single()).Should().Be("DELETE_FAILED");
        ((string?)refused.Single()!["errors"]![0]!["message"]).Should().Contain("is associated with the following records: Contact C1");
        ((bool)deleted.Single()!["success"]!).Should().BeTrue();
    }

    // ---------- sObject Collections upsert ----------

    [Fact]
    public async Task Records_of_one_upsert_that_share_an_external_id_all_fail_without_an_id_and_the_others_are_written()
    {
        var results = await UpsertAsync("Account", Account("DUP"), Account("A1"), Account("DUP"));

        results.Select(r => (bool)r!["success"]!).Should().Equal(false, true, false);
        Code(results[0]).Should().Be("DUPLICATE_VALUE");
        results[0]!.AsObject().ContainsKey("id").Should().BeFalse();
        _org.FindByExternalId("Account", "DUP").Should().BeNull();
    }

    [Fact]
    public async Task A_failed_update_has_no_id_even_though_the_record_exists()
    {
        await UpsertAccountsAsync("A1");
        _org.InjectFault("Account", "A1", "UNABLE_TO_LOCK_ROW", "locked", 1);

        var result = (await UpsertAsync("Account", Account("A1"))).Single()!;

        ((bool)result["success"]!, result.AsObject().ContainsKey("id")).Should().Be((false, false));
    }

    [Fact]
    public async Task An_upsert_with_allOrNone_true_writes_nothing_when_a_record_fails()
    {
        var bad = Account("BAD");
        bad["NoSuchField__c"] = "x";

        var results = await UpsertAsync("Account", allOrNone: true, Account("A1"), bad);

        results.Select(Code).Should().Equal("ALL_OR_NONE_OPERATION_ROLLED_BACK", "INVALID_FIELD");
        ((bool)results[0]!["created"]!).Should().BeFalse();
        _org.FindByExternalId("Account", "A1").Should().BeNull("rolled back");
        (await UpsertAccountsAsync("A1")).Should().NotBe((string?)results[0]!["id"], "an Id is never handed out twice");
    }

    [Fact]
    public async Task An_upsert_of_more_than_200_records_fails_as_a_whole()
    {
        using var res = await SendAsync(HttpMethod.Patch, $"{Data}/composite/sobjects/Account/ExternalId__c",
            new JsonObject { ["records"] = new JsonArray([.. Enumerable.Range(0, 201).Select(i => (JsonNode)Account($"A{i}"))]) });

        res.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        ((string?)JsonNode.Parse(await res.Content.ReadAsStringAsync())![0]!["errorCode"]).Should().Be("EXCEEDED_ID_LIMIT");
    }

    // ---------- Query ----------

    [Fact]
    public async Task A_query_answers_2000_rows_per_page_and_the_client_follows_nextRecordsUrl_to_the_rest()
    {
        await UpsertAccountsAsync("A1");
        for (var batch = 0; batch < 11; batch++)
        {
            await UpsertAsync("Contact", [.. Enumerable.Range(batch * 200, batch < 10 ? 200 : 1).Select(i => new JsonObject
            {
                ["attributes"] = Type("Contact"), ["ExternalId__c"] = $"C{i}", ["LastName"] = "Doe", ["AccountId__c"] = "A1"
            })]);
        }

        const string soql = "SELECT Id FROM Contact WHERE AccountId__c IN ('A1')";
        var first = JsonNode.Parse(await _http.GetStringAsync(SalesforceRestClient.QueryUrl(Data, soql)))!;
        var all = await SalesforceStep.Client(_http).SelectAsync(soql, CancellationToken.None);

        ((int)first["totalSize"]!, (bool)first["done"]!, first["records"]!.AsArray().Count).Should().Be((2_001, false, 2_000));
        ((string?)first["nextRecordsUrl"]).Should().StartWith($"{Data}/query/");
        ((string?)first["records"]![0]!["attributes"]!["url"]).Should().StartWith($"{Data}/sobjects/Contact/003");
        all.Should().HaveCount(2_001);
    }

    // ---------- OAuth sessions ----------

    [Fact]
    public async Task A_call_without_the_current_access_token_is_401_INVALID_SESSION_ID_and_costs_no_API_call()
    {
        using var anonymous = new HttpClient(new FakeSalesforceHandler(_org)) { BaseAddress = _http.BaseAddress };

        using var res = await anonymous.GetAsync($"{Data}/sobjects/Account/describe");

        res.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
        ((string?)JsonNode.Parse(await res.Content.ReadAsStringAsync())![0]!["errorCode"]).Should().Be("INVALID_SESSION_ID");
        _org.GetStats().ApiCalls.Should().Be(0);
    }

    [Fact]
    public async Task After_the_session_expires_the_client_gets_a_new_token_and_its_call_goes_through()
    {
        var options = Options.Create(new SalesforceOptions { TokenUrl = $"{_http.BaseAddress}services/oauth2/token" });
        var services = new ServiceCollection();
        services.AddHttpClient(SalesforceTokenProvider.HttpClientName).ConfigurePrimaryHttpMessageHandler(() => new FakeSalesforceHandler(_org));
        await using var provider = services.BuildServiceProvider();
        var tokens = new SalesforceTokenProvider(provider.GetRequiredService<IHttpClientFactory>(), options);
        var client = SalesforceStep.Client(new HttpClient(new SalesforceAuthHandler(tokens) { InnerHandler = new FakeSalesforceHandler(_org) })
        {
            BaseAddress = _http.BaseAddress
        });
        await client.DescribeAsync("Account", null, CancellationToken.None);

        _org.ExpireAccessToken();
        var schema = await client.DescribeAsync("Account", null, CancellationToken.None);

        schema!.Name.Should().Be("Account", "the 401 made the client fetch the org's new token and send again");
    }

    // ---------- Helpers ----------

    private static JsonObject Type(string sobject)
    {
        return new JsonObject { ["type"] = sobject };
    }

    private static JsonObject Account(string key)
    {
        return new JsonObject { ["attributes"] = Type("Account"), ["ExternalId__c"] = key, ["Name"] = $"Account {key}" };
    }

    private static string? Code(JsonNode? result)
    {
        return (string?)result!["errors"]![0]!["statusCode"];
    }

    /// <summary>Creates (or updates) an Account and returns its Id.</summary>
    private async Task<string> UpsertAccountsAsync(string key)
    {
        return (string)(await UpsertAsync("Account", Account(key))).Single()!["id"]!;
    }

    private Task<JsonArray> UpsertAsync(string sobject, params JsonObject[] records)
    {
        return UpsertAsync(sobject, false, records);
    }

    private async Task<JsonArray> UpsertAsync(string sobject, bool allOrNone, params JsonObject[] records)
    {
        using var res = await SendAsync(HttpMethod.Patch, $"{Data}/composite/sobjects/{sobject}/ExternalId__c",
            new JsonObject { ["allOrNone"] = allOrNone, ["records"] = new JsonArray([.. records]) });
        res.StatusCode.Should().Be(HttpStatusCode.OK);
        return JsonNode.Parse(await res.Content.ReadAsStringAsync())!.AsArray();
    }

    private async Task<(HttpStatusCode Status, JsonArray Results)> DeleteAsync(string ids, bool allOrNone = false)
    {
        using var res = await _http.DeleteAsync($"{Data}/composite/sobjects?ids={ids}&allOrNone={(allOrNone ? "true" : "false")}");
        return (res.StatusCode, JsonNode.Parse(await res.Content.ReadAsStringAsync())!.AsArray());
    }

    private Task<HttpResponseMessage> SendAsync(HttpMethod method, string url, JsonNode body)
    {
        return _http.SendAsync(new HttpRequestMessage(method, url)
        {
            Content = new StringContent(body.ToJsonString(), Encoding.UTF8, "application/json")
        });
    }
}