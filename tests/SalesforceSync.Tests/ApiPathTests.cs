using System.Net;
using Microsoft.Extensions.Options;
using SalesforceSync.Infrastructure.Salesforce;

namespace SalesforceSync.Tests;

/// <summary><c>Salesforce:BaseApiPath</c> and <c>Salesforce:ApiVersion</c> decide the path of every REST call.</summary>
public class ApiPathTests
{
    [Fact]
    public void Defaults_are_the_services_data_path_of_the_current_version()
    {
        new SalesforceOptions().DataPath.Should().Be("/services/data/v62.0");
    }

    [Theory]
    [InlineData("/services/data", "v70.0", "/services/data/v70.0")]
    [InlineData("services/data/", "v70.0", "/services/data/v70.0")]
    [InlineData("/proxy/sf", "v61.0", "/proxy/sf/v61.0")]
    public void Data_path_is_the_base_path_and_the_version(string basePath, string version, string expected)
    {
        new SalesforceOptions { BaseApiPath = basePath, ApiVersion = version }.DataPath.Should().Be(expected);
    }

    [Fact]
    public async Task Calls_go_to_the_configured_path_and_version()
    {
        var paths = new List<string>();
        var http = new HttpClient(new Recorder(paths)) { BaseAddress = new Uri("https://fake.my.salesforce.com") };
        var client = new SalesforceRestClient(http, Options.Create(new SalesforceOptions { BaseApiPath = "/proxy/sf", ApiVersion = "v70.0" }));

        await client.DescribeAsync("Account", null, CancellationToken.None);
        await client.WriteAsync([new CollectionDelete("d", ["001000000000000AAA"])], CancellationToken.None);

        paths.Should().Equal("/proxy/sf/v70.0/sobjects/Account/describe", "/proxy/sf/v70.0/composite/sobjects");
    }

    private sealed class Recorder(List<string> paths) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            paths.Add(request.RequestUri!.AbsolutePath);
            var body = request.Method == HttpMethod.Get ? """{"name":"Account","fields":[],"recordTypeInfos":[]}""" : "[]";
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(body) });
        }
    }
}
