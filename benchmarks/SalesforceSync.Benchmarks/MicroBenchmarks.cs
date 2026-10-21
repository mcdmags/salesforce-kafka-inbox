using System.Text.Json;
using BenchmarkDotNet.Attributes;
using SalesforceSync.Data.Inbox;
using SalesforceSync.Infrastructure.RateLimiting;
using SalesforceSync.Infrastructure.Salesforce;
using SalesforceSync.LoadGenerator;
using SalesforceSync.Sync;
using SalesforceSync.ToSalesforce;

namespace SalesforceSync.Benchmarks;

/// <summary>
/// CPU-only hot paths: header parsing, event reading, a record's fields, type-aware normalize + compare.
/// </summary>
[MemoryDiagnoser]
public class MicroBenchmarks
{
    private const string LimitHeader = "api-usage=10018/100000; api-bursts=1/750; per-app-api-usage=17/250(appName=sync)";

    private List<InboxEvent> _tenEvents = default!;
    private AccountRecord _accountRecord = default!;
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
            var meta = EventMeta.Of(v == 1 ? EventType.Inserted : EventType.Updated, $"2026-10-14T01:02:03.{v:D3}Z");
            return new InboxEvent(
                EntityId: key,
                Payload: JsonSerializer.Serialize(new AccountEvent(meta, account), EventJson.Options),
                EventTime: new DateTimeOffset(2026, 10, 14, 1, 2, 3, v, TimeSpan.Zero),
                LeaseEnd: DateTimeOffset.MaxValue);
        })];

        // Every field filled, as the account sync fills it from an event
        var a = factory.Account("ACC-000099", "ACC-000099", 1, [], []);
        _accountRecord = new AccountRecord
        {
            Name = a.Name,
            Rating = a.Rating,
            Industry = a.Industry,
            Description = a.Description,
            AnnualRevenue = a.AnnualRevenue,
            NumberOfEmployees = a.NumberOfEmployees,
            Phone = a.Phone,
            Website = a.Website,
            Active = a.IsActive,
            Tags = a.Tags is null ? null : string.Join(';', a.Tags),
            CustomerSince = a.CustomerSince,
            LastContacted = a.LastContactedAt,
            RecordTypeId = "012000000000000AAA"
        };

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

    /// <summary>
    /// Send step per record: a record class as the fields dictionary (<see cref="SalesforceFields.Of{T}"/>: property list
    /// read once per class, compiled getters). A hand-written dictionary of the same 13 fields took about the same.
    /// </summary>
    [Benchmark]
    public Dictionary<string, object?> AccountFields()
    {
        return SalesforceFields.Of(_accountRecord);
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