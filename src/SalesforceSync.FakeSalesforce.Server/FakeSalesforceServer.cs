using System.Text.Json.Nodes;
using Microsoft.Extensions.Options;

namespace SalesforceSync.FakeSalesforce.Server;

/// <summary>
/// The in-memory org as an HTTP service, so the worker talks to it over the network (real OAuth + REST client path)
/// and tests can inspect what reached "Salesforce". Program.cs runs it standalone (Aspire); the integration tests host
/// the same app in-process on a random Kestrel port.
/// </summary>
public static class FakeSalesforceServer
{
    /// <param name="args">Command-line arguments (configuration, <c>--urls</c>).</param>
    /// <param name="configure">Adjusts the builder (configuration, logging, URLs) before the app is built.</param>
    public static WebApplication Build(string[] args, Action<WebApplicationBuilder>? configure = null)
    {
        var builder = WebApplication.CreateBuilder(args);

        builder.Services.Configure<FakeSalesforceOptions>(builder.Configuration.GetSection(FakeSalesforceOptions.Section));
        builder.Services.AddSingleton(sp => new FakeSalesforceStore(sp.GetRequiredService<IOptions<FakeSalesforceOptions>>().Value));
        builder.Services.AddSingleton(sp => new HttpMessageInvoker(new FakeSalesforceHandler(sp.GetRequiredService<FakeSalesforceStore>())));
        builder.Services.AddSingleton<ClientLog>();
        builder.Services.AddHostedService<StatsLogger>();
        // Like Salesforce: gzip request bodies are accepted, responses are compressed when the client accepts it
        builder.Services.AddRequestDecompression();
        builder.Services.AddResponseCompression(o => o.MimeTypes = ["application/json"]);

        configure?.Invoke(builder);

        var app = builder.Build();
        Map(app);
        return app;
    }

    private static void Map(WebApplication app)
    {
        app.MapGet("/health", () => Results.Ok("healthy"));

        // Record how Salesforce API requests arrive, before they are decompressed
        app.Use(async (ctx, next) =>
        {
            if (ctx.Request.Path.StartsWithSegments("/services/data"))
            {
                ctx.RequestServices.GetRequiredService<ClientLog>().RecordRequest(
                    gzipBody: ctx.Request.Headers.ContentEncoding.ToString().Contains("gzip", StringComparison.OrdinalIgnoreCase),
                    acceptsGzip: ctx.Request.Headers.AcceptEncoding.ToString().Contains("gzip", StringComparison.OrdinalIgnoreCase));
            }

            await next();
        });
        app.UseRequestDecompression();
        app.UseResponseCompression();

        // Inspection API for tests and humans (not part of Salesforce)
        var fake = app.MapGroup("/__fake");
        fake.MapGet("/stats", (FakeSalesforceStore store) => store.GetStats());
        fake.MapGet("/records/{sobject}", (string sobject, FakeSalesforceStore store) => store.All(sobject));
        fake.MapGet("/records/{sobject}/{externalId}", (string sobject, string externalId, FakeSalesforceStore store) =>
            store.FindByExternalId(sobject, externalId) is { } record ? Results.Ok(record) : Results.NotFound());
        fake.MapPatch("/records/{sobject}/{externalId}", (string sobject, string externalId, JsonObject fields, FakeSalesforceStore store) =>
            store.Edit(sobject, externalId, fields) ? Results.NoContent() : Results.NotFound());
        fake.MapGet("/history/{sobject}/{externalId}", (string sobject, string externalId, FakeSalesforceStore store) =>
            store.History(sobject, externalId) is { } writes ? Results.Ok(writes) : Results.NotFound());
        fake.MapGet("/clients", (ClientLog clients) => clients.CompositeRequests);
        // The composite requests that contained writes, as received (to check what the client really sent)
        fake.MapGet("/requests", (FakeSalesforceStore store) => store.WriteRequests);
        fake.MapGet("/transport", (ClientLog clients) => clients.Transport);
        // Make create/update of one record fail with a Salesforce error code the next N times
        fake.MapPost("/faults", (FaultRequest f, FakeSalesforceStore store) =>
        {
            store.InjectFault(f.SObject, f.ExternalId, f.ErrorCode, f.Message ?? f.ErrorCode, f.Times ?? int.MaxValue);
            return Results.NoContent();
        });
        // Apply the next N writes of one record, but drop the connection instead of answering
        fake.MapPost("/lost-responses", (LostResponseRequest l, FakeSalesforceStore store) =>
        {
            store.LoseResponsesFor(l.SObject, l.ExternalId, l.Times ?? 1);
            return Results.NoContent();
        });
        fake.MapPost("/reset", (FakeSalesforceStore store, ClientLog clients) =>
        {
            store.Reset();
            clients.Reset();
            return Results.NoContent();
        });

        // Everything else is Salesforce REST (/services/oauth2/token, /services/data/vXX.X/...)
        app.Map("/{**path}", SalesforceProxy.HandleAsync);
    }

    internal sealed record LostResponseRequest(string SObject, string ExternalId, int? Times);

    internal sealed record FaultRequest(string SObject, string ExternalId, string ErrorCode, string? Message, int? Times);
}