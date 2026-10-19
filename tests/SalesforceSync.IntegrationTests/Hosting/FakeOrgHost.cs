using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using SalesforceSync.FakeSalesforce.Server;

namespace SalesforceSync.IntegrationTests.Hosting;

/// <summary>
/// The mock Salesforce org (the FakeSalesforce.Server app, unchanged) hosted in the test process on Kestrel at a
/// random loopback port. Workers reach it over real HTTP through their OAuth + REST client, as they reach Salesforce.
/// </summary>
public sealed class FakeOrgHost : IAsyncDisposable
{
    private readonly WebApplication _app;

    private FakeOrgHost(WebApplication app, Uri url)
    {
        _app = app;
        Url = url;
    }

    /// <summary>Base URL, e.g. http://127.0.0.1:54321/.</summary>
    public Uri Url { get; }

    /// <param name="settings">Configuration overrides (<c>FakeSalesforce:Latency</c>, ...), applied last.</param>
    /// <param name="ct">Cancels the start.</param>
    public static async Task<FakeOrgHost> StartAsync(IReadOnlyDictionary<string, string?> settings, CancellationToken ct)
    {
        var app = FakeSalesforceServer.Build([], builder =>
        {
            builder.WebHost.UseUrls("http://127.0.0.1:0");
            builder.Configuration.AddInMemoryCollection(settings);
            builder.Logging.ClearProviders().AddSimpleConsole().SetMinimumLevel(LogLevel.Warning);
        });
        await app.StartAsync(ct);

        // Port 0: Kestrel picks a free port and reports the bound address once started
        return new FakeOrgHost(app, new Uri(app.Urls.Single()));
    }

    public async ValueTask DisposeAsync()
    {
        await _app.StopAsync();
        await _app.DisposeAsync();
    }
}