using System.Net;
using System.Net.Http.Headers;

namespace SalesforceSync.Salesforce;

/// <summary>Adds the bearer token and retries once with a fresh token on 401.</summary>
public sealed class SalesforceAuthHandler(SalesforceTokenProvider tokens) : DelegatingHandler
{
    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
    {
        var token = await TokenAsync(null, ct);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        var res = await base.SendAsync(request, ct);
        if (res.StatusCode != HttpStatusCode.Unauthorized) return res;

        res.Dispose();
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", await TokenAsync(token, ct));
        return await base.SendAsync(request, ct);
    }

    /// <summary>
    /// A failure to get a token is not a failed API request: either nothing has been sent yet, or what was sent came
    /// back 401 (not applied). Marked so a write is not held as "outcome unknown" because of it.
    /// </summary>
    private async Task<string> TokenAsync(string? rejected, CancellationToken ct)
    {
        try
        {
            return await tokens.GetTokenAsync(rejected, ct);
        }
        catch (Exception ex)
        {
            WriteOutcome.MarkNotSent(ex);
            throw;
        }
    }
}
