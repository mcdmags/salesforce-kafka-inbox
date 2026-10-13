using System.Text.Json.Nodes;
using Microsoft.Extensions.Options;
using SalesforceSync.FakeSalesforce;
using SalesforceSync.FakeSalesforce.Server;

// The in-memory org as a standalone HTTP service, so the worker talks to it over the network
// (real OAuth + REST client path) and tests can inspect what reached "Salesforce".
var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<FakeSalesforceOptions>(builder.Configuration.GetSection(FakeSalesforceOptions.Section));
builder.Services.AddSingleton(sp => new FakeSalesforceStore(sp.GetRequiredService<IOptions<FakeSalesforceOptions>>().Value));
builder.Services.AddSingleton(sp => new HttpMessageInvoker(new FakeSalesforceHandler(sp.GetRequiredService<FakeSalesforceStore>())));
builder.Services.AddSingleton<ClientLog>();
builder.Services.AddHostedService<StatsLogger>();
// Like Salesforce: gzip request bodies are accepted, responses are compressed when the client accepts it
builder.Services.AddRequestDecompression();
builder.Services.AddResponseCompression(o => o.MimeTypes = ["application/json"]);

var app = builder.Build();

app.MapGet("/health", () => Results.Ok("healthy"));

// Record how Salesforce API requests arrive, before they are decompressed
app.Use(async (ctx, next) =>
{
    if (ctx.Request.Path.StartsWithSegments("/services/data"))
        ctx.RequestServices.GetRequiredService<ClientLog>().RecordRequest(
            gzipBody: ctx.Request.Headers.ContentEncoding.ToString().Contains("gzip", StringComparison.OrdinalIgnoreCase),
            acceptsGzip: ctx.Request.Headers.AcceptEncoding.ToString().Contains("gzip", StringComparison.OrdinalIgnoreCase));
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
fake.MapGet("/clients", (ClientLog clients) => clients.GraphRequests);
fake.MapGet("/transport", (ClientLog clients) => clients.Transport);
// Make create/update of one record fail with a Salesforce error code the next N times
fake.MapPost("/faults", (FaultRequest f, FakeSalesforceStore store) =>
{
    store.InjectFault(f.SObject, f.ExternalId, f.ErrorCode, f.Message ?? f.ErrorCode, f.Times ?? int.MaxValue);
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

app.Run();

internal sealed record FaultRequest(string SObject, string ExternalId, string ErrorCode, string? Message, int? Times);
