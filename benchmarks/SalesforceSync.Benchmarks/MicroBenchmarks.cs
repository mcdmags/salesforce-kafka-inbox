using System.Text.Json;
using System.Text.Json.Nodes;
using BenchmarkDotNet.Attributes;
using SalesforceSync.Inbox;
using SalesforceSync.RateLimiting;
using SalesforceSync.Salesforce;

namespace SalesforceSync.Benchmarks;

/// <summary>CPU-only hot paths: header parsing, event coalescing, diffing.</summary>
[MemoryDiagnoser]
public class MicroBenchmarks
{
    private const string LimitHeader = "api-usage=10018/100000; api-bursts=1/750; per-app-api-usage=17/250(appName=sync)";

    private List<InboxEvent> _tenEvents = default!;
    private JsonElement _number;
    private JsonElement _datetime;

    [GlobalSetup]
    public void Setup()
    {
        _tenEvents = Enumerable.Range(1, 10).Select(v => new InboxEvent(
            Id: v,
            AccountExtId: "ACC-000001",
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
    public WorkUnit CoalesceTenEvents() => InboxWorker.Coalesce(_tenEvents);

    [Benchmark]
    public bool DiffNumber() => AccountProfileSync.ValuesEqual(87.5m, _number);

    [Benchmark]
    public bool DiffDateTime() => AccountProfileSync.ValuesEqual(new DateTimeOffset(2026, 10, 12, 1, 2, 3, TimeSpan.Zero), _datetime);
}
