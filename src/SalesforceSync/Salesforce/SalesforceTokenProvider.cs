using System.Net.Http.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Options;

namespace SalesforceSync.Salesforce;

/// <summary>OAuth 2.0 client credentials flow, cached and shared by all requests in the process.</summary>
public sealed class SalesforceTokenProvider(IHttpClientFactory httpFactory, IOptions<SalesforceOptions> options)
{
    public const string HttpClientName = "salesforce-token";

    private readonly SemaphoreSlim _lock = new(1, 1);
    private string? _token;
    private DateTimeOffset _expiresAt;

    public async Task<string> GetTokenAsync(bool forceRefresh, CancellationToken ct)
    {
        if (!forceRefresh && _token is not null && DateTimeOffset.UtcNow < _expiresAt) return _token;

        await _lock.WaitAsync(ct);
        try
        {
            if (!forceRefresh && _token is not null && DateTimeOffset.UtcNow < _expiresAt) return _token;

            var o = options.Value;
            using var client = httpFactory.CreateClient(HttpClientName);
            using var res = await client.PostAsync(o.TokenUrl, new FormUrlEncodedContent(new Dictionary<string, string>
            {
                ["grant_type"] = "client_credentials",
                ["client_id"] = o.ClientId,
                ["client_secret"] = o.ClientSecret
            }), ct);

            if (!res.IsSuccessStatusCode)
                throw new HttpRequestException(
                    $"Salesforce token request failed {(int)res.StatusCode}: {await res.Content.ReadAsStringAsync(ct)}");

            var body = await res.Content.ReadFromJsonAsync<TokenResponse>(cancellationToken: ct)
                       ?? throw new InvalidOperationException("Empty token response");

            _token = body.AccessToken;
            _expiresAt = DateTimeOffset.UtcNow + o.TokenLifetime;
            return _token;
        }
        finally
        {
            _lock.Release();
        }
    }

    private sealed record TokenResponse([property: JsonPropertyName("access_token")] string AccessToken);
}
