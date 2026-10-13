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

var app = builder.Build();

app.MapGet("/health", () => Results.Ok("healthy"));

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
fake.MapPost("/reset", (FakeSalesforceStore store, ClientLog clients) =>
{
    store.Reset();
    clients.Reset();
    return Results.NoContent();
});

// Everything else is Salesforce REST (/services/oauth2/token, /services/data/vXX.X/...)
app.Map("/{**path}", SalesforceProxy.HandleAsync);

app.Run();
