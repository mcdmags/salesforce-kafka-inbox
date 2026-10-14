using System.Text.Json;
using System.Text.Json.Nodes;
using BenchmarkDotNet.Attributes;
using SalesforceSync.Inbox.Payloads;
using SalesforceSync.RateLimiting;
using SalesforceSync.Salesforce;
using SalesforceSync.Salesforce.SObjects;
using SalesforceSync.Sync;
using SalesforceSync.Sync.Mapping;

namespace SalesforceSync.Benchmarks;

/// <summary>
/// CPU-only hot paths: header parsing, event coalescing, the typed mapping (event JSON -> payload -> sObject ->
/// record), type-aware normalize + compare.
/// </summary>
[MemoryDiagnoser]
public class MicroBenchmarks
{
    private const string LimitHeader = "api-usage=10018/100000; api-bursts=1/750; per-app-api-usage=17/250(appName=sync)";

    private static readonly UpsertStrategy<AccountEvent> Accounts = AccountSyncService.CreateStrategy(new AccountMapper(), new ProfileMapper());

    private readonly AccountMapper _accountMapper = new();
    private List<InboxEvent> _tenEvents = default!;
    private AccountPayload _payload = default!;
    private AccountSObject _sobject = default!;
    private JsonElement _number;
    private JsonElement _datetime;
    private readonly SalesforceField _currency = new("AnnualRevenue", "currency", 0, 18, 2, true, true, true);
    private readonly SalesforceField _stamp = new("Last_Contacted__c", "datetime", 0, 0, 0, true, true, true);

    [GlobalSetup]
    public void Setup()
    {
        // Full records, as the source sends them; ids are deliberately not in version order
        _tenEvents = Enumerable.Range(1, 10).Select(v => new InboxEvent(
            Id: 11 - v,
            Key: "ACC-000001",
            Version: v,
            Payload: new JsonObject
            {
                ["meta"] = new JsonObject { ["time"] = $"2026-10-14T01:02:03.{v:D3}Z" },
                ["account"] = new JsonObject
                {
                    ["externalId"] = "ACC-000001",
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
            RetryCount: 1)).ToList();

        _payload = JsonSerializer.Deserialize<AccountEvent>(_tenEvents[0].Payload, SalesforceBatchUpsertService.EventJson)!.Account!;
        _sobject = _accountMapper.Map(_payload);

        _number = JsonDocument.Parse("87.50").RootElement.Clone();
        _datetime = JsonDocument.Parse("\"2026-10-12T01:02:03.000+0000\"").RootElement.Clone();
    }

    [Benchmark]
    public SalesforceLimitInfo? ParseLimitHeader() => SalesforceLimitInfo.Parse(LimitHeader);

    /// <summary>Ten pending events of one entity -> the one to write (the newest).</summary>
    [Benchmark]
    public InboxEvent CoalesceTenEvents() => SalesforceBatchUpsertService.Coalesce(_tenEvents);

    /// <summary>What the engine does per entity: raw event JSON -> typed event -> mappers -> Account + Profile records.</summary>
    [Benchmark]
    public UpsertUnit MapAccountEvent()
        => Accounts.Map("ACC-000001", JsonSerializer.Deserialize<AccountEvent>(_tenEvents[0].Payload, SalesforceBatchUpsertService.EventJson)!);

    /// <summary>The hand-written mapper alone: payload -> sObject.</summary>
    [Benchmark]
    public AccountSObject MapPayloadToSObject() => _accountMapper.Map(_payload);

    /// <summary>sObject -> record through the cached attribute metadata (no reflection per call).</summary>
    [Benchmark]
    public SObjectRecord MapSObjectToRecord() => SObjectRecord.From("acct", _sobject);

    [Benchmark]
    public bool DiffNumber()
        => FieldValues.TryNormalize(_currency, "87.499", out var v, out _) && FieldValues.AreEqual(_currency, v, _number);

    [Benchmark]
    public bool DiffDateTime()
        => FieldValues.TryNormalize(_stamp, "2026-10-12T01:02:03Z", out var v, out _) && FieldValues.AreEqual(_stamp, v, _datetime);
}
