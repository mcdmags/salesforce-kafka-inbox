using System.Net.Http.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Options;

namespace SalesforceSync.Salesforce;

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
    public async Task<string> GetTokenAsync(string? rejected, CancellationToken ct)
    {
        if (Usable(_token, rejected) is { } cached) return cached;

        await _lock.WaitAsync(ct);
        try
        {
            if (Usable(_token, rejected) is { } refreshed) return refreshed; // another request just refreshed it

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
                throw new HttpRequestException(
                    $"Salesforce token request failed {(int)res.StatusCode}: {await res.Content.ReadAsStringAsync(ct)}");

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
        => token is not null && token.Value != rejected && DateTimeOffset.UtcNow < token.ExpiresAt ? token.Value : null;

    private sealed record TokenResponse([property: JsonPropertyName("access_token")] string AccessToken);
}
