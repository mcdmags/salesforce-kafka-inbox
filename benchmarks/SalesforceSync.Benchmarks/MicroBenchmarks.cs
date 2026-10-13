using System.Text.Json;
using System.Text.Json.Nodes;
using BenchmarkDotNet.Attributes;
using SalesforceSync.Inbox;
using SalesforceSync.RateLimiting;
using SalesforceSync.Salesforce;
using SalesforceSync.Sync;

namespace SalesforceSync.Benchmarks;

/// <summary>CPU-only hot paths: header parsing, event coalescing, type-aware normalize + compare.</summary>
[MemoryDiagnoser]
public class MicroBenchmarks
{
    private const string LimitHeader = "api-usage=10018/100000; api-bursts=1/750; per-app-api-usage=17/250(appName=sync)";

    private List<InboxEvent> _tenEvents = default!;
    private JsonElement _number;
    private JsonElement _datetime;
    private readonly SalesforceField _currency = new("AnnualRevenue", "currency", 0, 18, 2, true, true, true);
    private readonly SalesforceField _stamp = new("Last_Contacted__c", "datetime", 0, 0, 0, true, true, true);

    [GlobalSetup]
    public void Setup()
    {
        _tenEvents = Enumerable.Range(1, 10).Select(v => new InboxEvent(
            Id: v,
            Key: "ACC-000001",
            Version: v,
            Payload: new JsonObject
            {
                ["profileExternalId"] = "PRF-000001",
                ["version"] = v,
                ["account"] = new JsonObject { ["Name"] = "Acme", ["Description"] = $"v{v}" },
                ["profile"] = new JsonObject { ["Score__c"] = v }
            }.ToJsonString(),
            RetryCount: 1)).ToList();

        _number = JsonDocument.Parse("87.50").RootElement.Clone();
        _datetime = JsonDocument.Parse("\"2026-10-12T01:02:03.000+0000\"").RootElement.Clone();
    }

    [Benchmark]
    public SalesforceLimitInfo? ParseLimitHeader() => SalesforceLimitInfo.Parse(LimitHeader);

    [Benchmark]
    public JsonObject CoalesceTenEvents() => SalesforceBatchUpsertService.Coalesce(_tenEvents);

    [Benchmark]
    public bool DiffNumber()
        => FieldValues.TryNormalize(_currency, "87.499", out var v, out _) && FieldValues.AreEqual(_currency, v, _number);

    [Benchmark]
    public bool DiffDateTime()
        => FieldValues.TryNormalize(_stamp, "2026-10-12T01:02:03Z", out var v, out _) && FieldValues.AreEqual(_stamp, v, _datetime);
}
