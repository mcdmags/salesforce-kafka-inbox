using System.Text.Json;
using BenchmarkDotNet.Attributes;
using SalesforceSync.Data.Inbox;
using SalesforceSync.Infrastructure.RateLimiting;
using SalesforceSync.Infrastructure.Salesforce;
using SalesforceSync.LoadGenerator;
using SalesforceSync.Sync;

namespace SalesforceSync.Benchmarks;

/// <summary>
/// CPU-only hot paths: header parsing, event reading, type-aware normalize + compare.
/// </summary>
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
        // Full records as source sends them, built on the event template: a claim's rows, one per entity
        var factory = new AggregateFactory(AggregateTemplate.Current, seed: 1);
        _tenEvents = [.. Enumerable.Range(1, 10).Select(v =>
        {
            var key = $"ACC-{v:D6}";
            AccountPayload account = factory.Account(key, key, v, [], []);
            var meta = new EventMeta { Time = $"2026-10-14T01:02:03.{v:D3}Z", Type = v == 1 ? "inserted" : "updated" };
            return new InboxEvent(
                EntityId: key,
                Payload: JsonSerializer.Serialize(new AccountEvent(meta, account), EventJson.Options),
                EventTime: new DateTimeOffset(2026, 10, 14, 1, 2, 3, v, TimeSpan.Zero),
                LeaseEnd: DateTimeOffset.MaxValue);
        })];

        _number = JsonDocument.Parse("87.50").RootElement.Clone();
        _datetime = JsonDocument.Parse("\"2026-10-12T01:02:03.000+0000\"").RootElement.Clone();
    }

    [Benchmark]
    public SalesforceLimitInfo? ParseLimitHeader()
    {
        return SalesforceLimitInfo.Parse(LimitHeader);
    }

    /// <summary>Consumer per message: typed read + validation (entity id, event time).</summary>
    [Benchmark]
    public bool ReadAndValidateEvent()
    {
        return AccountEvent.Reader.TryRead(_tenEvents[0].Payload, out _, out _);
    }

    [Benchmark]
    public bool DiffNumber()
    {
        return FieldValues.AreEqual(FieldValues.Normalize(87.5m), _number);
    }

    [Benchmark]
    public bool DiffDateTime()
    {
        return FieldValues.AreEqual(FieldValues.Normalize(new DateTimeOffset(2026, 10, 12, 1, 2, 3, TimeSpan.Zero)), _datetime);
    }
}