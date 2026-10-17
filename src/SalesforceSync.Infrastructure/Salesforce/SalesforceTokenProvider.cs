using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Options;

namespace SalesforceSync.Infrastructure.Salesforce;

/// <summary>OAuth 2.0 client credentials flow, cached and shared by all requests in the process.</summary>
public sealed class SalesforceTokenProvider(IHttpClientFactory httpFactory, IOptions<SalesforceOptions> options)
{
    public const string HttpClientName = "salesforce-token";

    private sealed record CachedToken(string Value, DateTimeOffset ExpiresAt);

    private readonly SemaphoreSlim _lock = new(1, 1);
    private volatile CachedToken? _token; // replaced as a whole, so lock-free readers never pair a token with another's expiry

    /// <param name="rejected">
    /// A token Salesforce just answered 401 to. It is replaced only if it is still the cached one, so when many
    /// requests fail at once, one of them fetches a new token and the rest reuse it.
    /// </param>
    /// <param name="ct">Cancels the wait and the token request.</param>
    public async Task<string> GetTokenAsync(string? rejected, CancellationToken ct)
    {
        if (Usable(_token, rejected) is { } cached)
        {
            return cached;
        }

        await _lock.WaitAsync(ct);
        try
        {
            if (Usable(_token, rejected) is { } refreshed)
            {
                return refreshed; // another request just refreshed it
            }

            var o = options.Value;
            using var client = httpFactory.CreateClient(HttpClientName);
            using var form = new FormUrlEncodedContent(new Dictionary<string, string>
            {
                ["grant_type"] = "client_credentials",
                ["client_id"] = o.ClientId,
                ["client_secret"] = o.ClientSecret
            });
            using var res = await client.PostAsync(o.TokenUrl, form, ct);

            if (!res.IsSuccessStatusCode)
            {
                throw new HttpRequestException(
                    $"Salesforce token request failed {(int)res.StatusCode}: {await res.Content.ReadAsStringAsync(ct)}");
            }

            var body = await res.Content.ReadFromJsonAsync<TokenResponse>(cancellationToken: ct)
                       ?? throw new InvalidOperationException("Empty token response");

            _token = new CachedToken(body.AccessToken, DateTimeOffset.UtcNow + o.TokenLifetime);
            return body.AccessToken;
        }
        finally
        {
            _lock.Release();
        }
    }

    private static string? Usable(CachedToken? token, string? rejected)
    {
        return token is not null && token.Value != rejected && DateTimeOffset.UtcNow < token.ExpiresAt ? token.Value : null;
    }

    private sealed record TokenResponse([property: JsonPropertyName("access_token")] string AccessToken);
}

/// <summary>Adds the bearer token and retries once with a fresh token on 401.</summary>
public sealed class SalesforceAuthHandler(SalesforceTokenProvider tokens) : DelegatingHandler
{
    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
    {
        var token = await TokenAsync(null, ct);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        var res = await base.SendAsync(request, ct);
        if (res.StatusCode != HttpStatusCode.Unauthorized)
        {
            return res;
        }

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