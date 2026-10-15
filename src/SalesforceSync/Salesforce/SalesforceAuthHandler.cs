using System.Net;
using System.Net.Http.Headers;

namespace SalesforceSync.Salesforce;

/// <summary>Adds the bearer token and retries once with a fresh token on 401.</summary>
public sealed class SalesforceAuthHandler(SalesforceTokenProvider tokens) : DelegatingHandler
{
    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
    {
        var token = await tokens.GetTokenAsync(null, ct);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        var res = await base.SendAsync(request, ct);
        if (res.StatusCode != HttpStatusCode.Unauthorized) return res;

        res.Dispose();
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", await tokens.GetTokenAsync(token, ct));
        return await base.SendAsync(request, ct);
    }
}
