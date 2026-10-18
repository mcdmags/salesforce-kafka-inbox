using System.Text.Json;
using System.Text.Json.Nodes;
using BenchmarkDotNet.Attributes;
using SalesforceSync.Data.Inbox;
using SalesforceSync.Infrastructure.RateLimiting;
using SalesforceSync.Infrastructure.Salesforce;
using SalesforceSync.Sync;

namespace SalesforceSync.Benchmarks;

/// <summary>
/// CPU-only hot paths: header parsing, reading events, the typed mapping (event JSON -> payload -> sObject -> record),
/// type-aware normalize + compare.
/// </summary>
[MemoryDiagnoser]
public class MicroBenchmarks
{
    private const string LimitHeader = "api-usage=10018/100000; api-bursts=1/750; per-app-api-usage=17/250(appName=sync)";

    private static readonly SyncStrategy<AccountEvent> Accounts = AccountSync.Strategy;

    private List<InboxEvent> _tenEvents = default!;
    private AccountPayload _payload = default!;
    private AccountSObject _sobject = default!;
    private JsonElement _number;
    private JsonElement _datetime;

    [GlobalSetup]
    public void Setup()
    {
        // Full records, as the source sends them: a claim's rows, one per entity
        _tenEvents = [.. Enumerable.Range(1, 10).Select(v => new InboxEvent(
            EntityId: $"ACC-{v:D6}",
            Payload: new JsonObject
            {
                ["meta"] = new JsonObject { ["time"] = $"2026-10-14T01:02:03.{v:D3}Z", ["type"] = v == 1 ? "inserted" : "updated" },
                ["account"] = new JsonObject
                {
                    ["externalId"] = $"ACC-{v:D6}",
                    ["name"] = "Acme",
                    ["rating"] = "Hot",
                    ["industry"] = "Technology",
                    ["description"] = $"v{v}",
                    ["annualRevenue"] = 1250000.5,
                    ["numberOfEmployees"] = 250,
                    ["phone"] = "+1 555 0100",
                    ["website"] = "https://example.com",
                    ["isActive"] = true,
                    ["tags"] = new JsonArray("B2B", "Retail"),
                    ["customerSince"] = "2020-01-15",
                    ["lastContactedAt"] = "2026-10-12T01:02:03Z",
                    ["profile"] = new JsonObject
                    {
                        ["name"] = "Acme profile",
                        ["tier"] = "Gold",
                        ["score"] = v,
                        ["discountPercent"] = 12.5,
                        ["notes"] = null,
                        ["optIn"] = true,
                        ["startDate"] = "2020-02-01"
                    }
                }
            }.ToJsonString(),
            EventTime: new DateTimeOffset(2026, 10, 14, 1, 2, 3, v, TimeSpan.Zero)))];

        _payload = JsonSerializer.Deserialize<AccountEvent>(_tenEvents[0].Payload, SalesforceBatchSyncService.EventJson)!.Account!;
        _sobject = AccountSync.ToAccount(_payload);

        _number = JsonDocument.Parse("87.50").RootElement.Clone();
        _datetime = JsonDocument.Parse("\"2026-10-12T01:02:03.000+0000\"").RootElement.Clone();
    }

    [Benchmark]
    public SalesforceLimitInfo? ParseLimitHeader()
    {
        return SalesforceLimitInfo.Parse(LimitHeader);
    }

    /// <summary>What the engine does with the rows of a claim of ten entities: read each one's event and map it.</summary>
    [Benchmark]
    public int PrepareTenEntities()
    {
        return PreparedBatch.Prepare(Accounts, _tenEvents).Work.Count;
    }

    /// <summary>What the consumer does per message: typed read + validation (entity id, event time).</summary>
    [Benchmark]
    public bool ReadAndValidateEvent()
    {
        return Accounts.TryRead(_tenEvents[0].Payload, out _, out _);
    }

    /// <summary>What the engine does per entity: raw event JSON -> typed event -> mapping -> Account + Profile records.</summary>
    [Benchmark]
    public SyncUnit MapAccountEvent()
    {
        return Accounts.Map("ACC-000001", JsonSerializer.Deserialize<AccountEvent>(_tenEvents[0].Payload, SalesforceBatchSyncService.EventJson)!);
    }

    /// <summary>The hand-written mapping alone: payload -> sObject.</summary>
    [Benchmark]
    public AccountSObject MapPayloadToSObject()
    {
        return AccountSync.ToAccount(_payload);
    }

    /// <summary>sObject -> record through the cached attribute metadata (no reflection per call).</summary>
    [Benchmark]
    public SObjectRecord MapSObjectToRecord()
    {
        return SObjectRecord.From("acct", _sobject);
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