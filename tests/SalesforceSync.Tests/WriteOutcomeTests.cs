using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using Microsoft.Extensions.Options;
using SalesforceSync.FakeSalesforce;
using SalesforceSync.Infrastructure.RateLimiting;
using SalesforceSync.Infrastructure.Salesforce;
using SalesforceSync.ToSalesforce;

namespace SalesforceSync.Tests;

/// <summary>
/// Failed write = "not applied" only when the failure proves it; else outcome unknown, entities held.
/// Failed read is never unknown: nothing written.
/// </summary>
public sealed class WriteOutcomeTests
{
    private const string SalesforceBody = /*lang=json,strict*/ """[{"errorCode":"SERVER_UNAVAILABLE","message":"Servers are temporarily unable to respond"}]""";

    public static TheoryData<string, Exception, bool> Failures => new()
    {
        // Proven not applied
        { "400 from Salesforce", new SalesforceResponseException(HttpStatusCode.BadRequest, /*lang=json,strict*/ """[{"errorCode":"JSON_PARSER_ERROR","message":"x"}]"""), false },
        { "401 without a body", new SalesforceResponseException(HttpStatusCode.Unauthorized, ""), false },
        { "403 REQUEST_LIMIT_EXCEEDED, or no rate-limit token (never sent)", new RateLimitedException(TimeSpan.FromSeconds(30)), false },
        { "503 with Salesforce's error body", new SalesforceResponseException(HttpStatusCode.ServiceUnavailable, SalesforceBody), false },
        { "500 with Salesforce's error body", new SalesforceResponseException(HttpStatusCode.InternalServerError, SalesforceBody), false },
        { "connection refused", new HttpRequestException(HttpRequestError.ConnectionError, "refused", new SocketException((int)SocketError.ConnectionRefused)), false },
        { "DNS failure", new HttpRequestException(HttpRequestError.NameResolutionError, "no such host"), false },
        { "TLS handshake failed", new HttpRequestException(HttpRequestError.SecureConnectionError, "handshake"), false },
        { "too many collections (not sent)", new ArgumentOutOfRangeException("collections"), false },

        // Unknown
        { "lease cutoff or shutdown", new OperationCanceledException(), true },
        { "HttpClient timeout", new TaskCanceledException("The request was canceled due to the configured HttpClient.Timeout", new TimeoutException()), true },
        { "connection reset mid-request", new HttpRequestException(HttpRequestError.ResponseEnded, "ended"), true },
        { "error without detail", new HttpRequestException("An error occurred while sending the request", new IOException("reset")), true },
        { "socket error", new IOException("Connection reset by peer", new SocketException((int)SocketError.ConnectionReset)), true },
        { "502 from a gateway", new SalesforceResponseException(HttpStatusCode.BadGateway, "<html>Bad Gateway</html>"), true },
        { "504 from a gateway", new SalesforceResponseException(HttpStatusCode.GatewayTimeout, ""), true },
        { "503 without a Salesforce body", new SalesforceResponseException(HttpStatusCode.ServiceUnavailable, "<html>Service Unavailable</html>"), true },
        { "503 from a gateway, via the rate limiter", new RateLimitedException(TimeSpan.FromSeconds(30), mayHaveBeenProcessed: true), true },
        { "unreadable response", new JsonException("unexpected end of data"), true },
        { "empty response", new InvalidOperationException("Empty composite response"), true }
    };

    [Theory, MemberData(nameof(Failures))]
    public void A_failed_write_is_unknown_unless_the_failure_proves_it_was_not_applied(string what, Exception failure, bool unknown)
    {
        SalesforceCalls.IsUnknownOutcome(failure).Should().Be(unknown, what);
    }

    [Theory]
    [InlineData(/*lang=json,strict*/ """[{"errorCode":"X","message":"y"}]""", true)]
    [InlineData("<html>503 Service Unavailable</html>", false)]
    [InlineData("", false)]
    public void Only_a_body_with_Salesforce_errors_counts_as_Salesforces_own_answer(string body, bool own)
    {
        new SalesforceResponseException(HttpStatusCode.ServiceUnavailable, body).IsSalesforceError.Should().Be(own);
    }

    // ---------- Read vs write ----------

    [Fact]
    public async Task A_failed_read_is_not_an_unknown_outcome()
    {
        var (engine, org, failing) = Engine();
        failing.FailReads = true;

        var read = () => engine.ReadAsync(AccountSync.AccountObject, ["Name"], ["A1"], CancellationToken.None);

        (await read.Should().ThrowAsync<HttpRequestException>()).Which.Should().NotBeOfType<UnknownOutcomeException>();
        org.FindByExternalId("Account", "A1").Should().BeNull();
    }

    [Fact]
    public async Task A_write_that_fails_after_it_was_sent_is_an_unknown_outcome()
    {
        var (engine, org, _) = Engine();
        org.LoseResponsesFor("Account", "A1", 1);

        var upsert = () => Upsert(engine, "A1");

        (await upsert.Should().ThrowAsync<UnknownOutcomeException>()).WithInnerException<HttpRequestException>();
        org.FindByExternalId("Account", "A1").Should().NotBeNull("the org applied the write; only the answer was lost");
    }

    [Fact]
    public async Task A_write_Salesforce_rejected_is_not_an_unknown_outcome()
    {
        var (engine, _, failing) = Engine();
        failing.FailWrites = true;
        failing.Status = HttpStatusCode.BadRequest;

        var upsert = () => Upsert(engine, "A1");

        await upsert.Should().ThrowAsync<SalesforceResponseException>();
    }

    [Fact]
    public async Task A_cancelled_write_is_an_unknown_outcome()
    {
        var (engine, _, failing) = Engine();
        failing.FailWrites = true;
        failing.Cancel = true;

        var upsert = () => Upsert(engine, "A1");

        (await upsert.Should().ThrowAsync<UnknownOutcomeException>()).WithInnerException<OperationCanceledException>();
    }

    private static Task Upsert(SalesforceCalls calls, string key)
    {
        return calls.UpsertAsync(AccountSync.AccountObject, [new DesiredRecord(key, key, new Dictionary<string, object?> { ["Name"] = "Acme" })], [], CancellationToken.None);
    }

    private static (SalesforceCalls, FakeSalesforceStore, FailingHandler) Engine()
    {
        var org = new FakeSalesforceStore(new FakeSalesforceOptions { EnforceDailyLimit = false });
        var options = Options.Create(new SalesforceOptions { CompressRequestsLargerThan = 0 });
        var failing = new FailingHandler { InnerHandler = new FakeSalesforceHandler(org) };
        var http = SalesforceStep.Http(failing);
        return (SalesforceStep.Calls(http, options), org, failing);
    }

    /// <summary>
    /// Fails the read (query) or write (Collections upsert or delete) before it reaches the org.
    /// </summary>
    private sealed class FailingHandler : DelegatingHandler
    {
        public bool FailReads { get; set; }
        public bool FailWrites { get; set; }
        public HttpStatusCode Status { get; set; } = HttpStatusCode.InternalServerError;
        public bool Cancel { get; set; }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            var path = request.RequestUri!.AbsolutePath;
            var isWrite = path.Contains("/composite/sobjects", StringComparison.Ordinal);
            return (isWrite ? !FailWrites : !FailReads) || (!isWrite && !path.EndsWith("/query", StringComparison.Ordinal))
                ? await base.SendAsync(request, ct)
                : Cancel
                ? throw new TaskCanceledException("timed out")
                : new HttpResponseMessage(Status)
                {
                    RequestMessage = request,
                    Content = new StringContent(/*lang=json,strict*/ """[{"errorCode":"JSON_PARSER_ERROR","message":"injected"}]""")
                };
        }
    }
}