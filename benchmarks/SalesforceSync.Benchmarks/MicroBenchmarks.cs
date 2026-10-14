using System.Text.Json;
using System.Text.Json.Nodes;
using BenchmarkDotNet.Attributes;
using SalesforceSync.Inbox.Payloads;
using SalesforceSync.Salesforce;
using SalesforceSync.Salesforce.SObjects;
using SalesforceSync.Sync;
using SalesforceSync.Sync.Mapping;

namespace SalesforceSync.Benchmarks;

/// <summary>
/// CPU-only hot paths: reading and coalescing events, the typed mapping (event JSON -> payload -> sObject ->
/// record), type-aware normalize + compare.
/// </summary>
[MemoryDiagnoser]
public class MicroBenchmarks
{

    private static readonly UpsertStrategy<AccountEvent> Accounts = AccountSyncService.CreateStrategy(new AccountMapper(), new ProfileMapper());

    private static readonly Dictionary<string, long> NothingSynced = [];

    private readonly AccountMapper _accountMapper = new();
    private List<InboxEvent> _tenEvents = default!;
    private List<(InboxEvent Row, ReadEvent<AccountEvent> Read)> _tenRead = default!;
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
        _tenEvents = [.. Enumerable.Range(1, 10).Select(v => new InboxEvent(
            Id: 11 - v,
            EntityId: "ACC-000001",
            Payload: new JsonObject
            {
                ["meta"] = new JsonObject { ["time"] = $"2026-10-14T01:02:03.{v:D3}Z", ["type"] = v == 1 ? "inserted" : "updated" },
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
            RetryCount: 1))];

        _tenRead = [.. _tenEvents.Select(row => (row, Accounts.TryRead(row.Payload, out var read, out _) ? read : null!))];
        _payload = JsonSerializer.Deserialize<AccountEvent>(_tenEvents[0].Payload, SalesforceBatchUpsertService.EventJson)!.Account!;
        _sobject = _accountMapper.Map(_payload);

        _number = JsonDocument.Parse("87.50").RootElement.Clone();
        _datetime = JsonDocument.Parse("\"2026-10-12T01:02:03.000+0000\"").RootElement.Clone();
    }

    /// <summary>Ten claimed events of one entity, already read -> the one to write (the newest by event time).</summary>
    [Benchmark]
    public long CoalesceTenEvents()
    {
        return BatchPreparer.Coalesce(_tenRead).Row.Id;
    }

    /// <summary>What the engine does with a claimed entity: read its ten events, keep the newest, map it.</summary>
    [Benchmark]
    public int PrepareTenEvents()
    {
        return BatchPreparer.Prepare(Accounts, _tenEvents, NothingSynced).Work.Count;
    }

    /// <summary>What the consumer does per message: typed read + validation (entity id, event time).</summary>
    [Benchmark]
    public bool ReadAndValidateEvent()
    {
        return Accounts.TryRead(_tenEvents[0].Payload, out _, out _);
    }

    /// <summary>What the engine does per entity: raw event JSON -> typed event -> mappers -> Account + Profile records.</summary>
    [Benchmark]
    public UpsertUnit MapAccountEvent()
    {
        return Accounts.Map("ACC-000001", JsonSerializer.Deserialize<AccountEvent>(_tenEvents[0].Payload, SalesforceBatchUpsertService.EventJson)!);
    }

    /// <summary>The hand-written mapper alone: payload -> sObject.</summary>
    [Benchmark]
    public AccountSObject MapPayloadToSObject()
    {
        return _accountMapper.Map(_payload);
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
        return FieldValues.TryNormalize(_currency, "87.499", out var v, out _) && FieldValues.AreEqual(_currency, v, _number);
    }

    [Benchmark]
    public bool DiffDateTime()
    {
        return FieldValues.TryNormalize(_stamp, "2026-10-12T01:02:03Z", out var v, out _) && FieldValues.AreEqual(_stamp, v, _datetime);
    }
}