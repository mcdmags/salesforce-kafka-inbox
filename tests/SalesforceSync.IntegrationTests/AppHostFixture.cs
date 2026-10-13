using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Nodes;
using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Dapper;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace SalesforceSync.IntegrationTests;

/// <summary>
/// Starts the whole Aspire app once for all tests: Postgres + Kafka containers, the sync worker and the
/// mock Salesforce service. Tests produce to Kafka and assert on the mock org and the Postgres inbox.
/// Subclasses can reshape the app (replicas, settings) in <see cref="Configure"/>.
/// </summary>
public class AppHostFixture : IAsyncLifetime
{
    public const string Topic = "account-changes";
    private static readonly TimeSpan StartupTimeout = TimeSpan.FromMinutes(5);

    private DistributedApplication? _app;
    private IProducer<string, string>? _producer;
    private NpgsqlDataSource? _db;

    public HttpClient Salesforce { get; private set; } = null!;

    public NpgsqlDataSource Db => _db ?? throw new InvalidOperationException("Fixture not started");

    /// <summary>Adjust the app model before it starts.</summary>
    protected virtual void Configure(IDistributedApplicationTestingBuilder builder)
    {
    }

    /// <summary>Runs once the worker has created the inbox schema, before any test.</summary>
    protected virtual Task OnStartedAsync() => Task.CompletedTask;

    public async Task InitializeAsync()
    {
        using var cts = new CancellationTokenSource(StartupTimeout);
        var ct = cts.Token;

        var builder = await DistributedApplicationTestingBuilder.CreateAsync<Projects.SalesforceSync_AppHost>(
            ["--AppHost:TestMode=true"], ct);
        Configure(builder);
        _app = await builder.BuildAsync(ct);
        await _app.StartAsync(ct);

        var notifications = _app.Services.GetRequiredService<ResourceNotificationService>();
        await notifications.WaitForResourceHealthyAsync("fake-salesforce", ct);
        await notifications.WaitForResourceAsync("salesforce-sync", KnownResourceStates.Running, ct);

        Salesforce = _app.CreateHttpClient("fake-salesforce");

        var kafka = await _app.GetConnectionStringAsync("kafka", ct)
                    ?? throw new InvalidOperationException("No Kafka connection string");
        await EnsureTopicAsync(kafka);
        _producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = kafka,
            Acks = Acks.All,
            EnableIdempotence = true
        }).Build();

        var inbox = await _app.GetConnectionStringAsync("inboxdb", ct)
                    ?? throw new InvalidOperationException("No inboxdb connection string");
        _db = NpgsqlDataSource.Create(inbox);

        // "Running" only means the worker process started; wait until it has applied the inbox schema.
        await Eventually.ValueAsync(async () =>
        {
            await using var conn = await _db.OpenConnectionAsync(ct);
            return await conn.ExecuteScalarAsync<bool>("select to_regclass('sf_inbox') is not null");
        }, ready => ready, "the worker to create sf_inbox", StartupTimeout);

        await OnStartedAsync();
    }

    public async Task DisposeAsync()
    {
        _producer?.Dispose();
        if (_db is not null) await _db.DisposeAsync();
        if (_app is not null) await _app.DisposeAsync();
    }

    /// <summary>Publishes an account change event, keyed by account like the real upstream.</summary>
    public async Task PublishAsync(string accountExtId, long version, JsonObject account, JsonObject profile)
        => await _producer!.ProduceAsync(Topic, Event(accountExtId, version, account, profile));

    /// <summary>Fire-and-forget produce for bulk loads; call <see cref="Flush"/> afterwards.</summary>
    public void Publish(string accountExtId, long version, JsonObject account, JsonObject profile)
        => _producer!.Produce(Topic, Event(accountExtId, version, account, profile), report =>
        {
            if (report.Error.IsError) throw new InvalidOperationException($"Produce failed: {report.Error.Reason}");
        });

    public void Flush()
    {
        if (_producer!.Flush(TimeSpan.FromSeconds(30)) > 0)
            throw new TimeoutException("Kafka producer did not flush all messages");
    }

    private static Message<string, string> Event(string accountExtId, long version, JsonObject account, JsonObject profile) => new()
    {
        Key = accountExtId,
        Value = new JsonObject
        {
            ["accountExternalId"] = accountExtId,
            ["profileExternalId"] = ProfileIdFor(accountExtId),
            ["version"] = version,
            ["account"] = account,
            ["profile"] = profile
        }.ToJsonString()
    };

    public static string NewAccountId() => $"E2E-{Guid.NewGuid():N}";

    public static string ProfileIdFor(string accountExtId) => accountExtId.Replace("E2E-", "PRF-");

    /// <summary>Record from the mock org by external id, or null if it does not exist (yet).</summary>
    public async Task<JsonObject?> GetRecordAsync(string sobject, string externalId)
    {
        using var res = await Salesforce.GetAsync($"/__fake/records/{sobject}/{Uri.EscapeDataString(externalId)}");
        if (res.StatusCode == System.Net.HttpStatusCode.NotFound) return null;
        res.EnsureSuccessStatusCode();
        return await res.Content.ReadFromJsonAsync<JsonObject>();
    }

    /// <summary>Field values of every write the mock org applied to the record, oldest first (the create first).</summary>
    public async Task<List<JsonObject>> GetHistoryAsync(string sobject, string externalId)
        => await Salesforce.GetFromJsonAsync<List<JsonObject>>(
               $"/__fake/history/{sobject}/{Uri.EscapeDataString(externalId)}")
           ?? throw new InvalidOperationException("Empty history response");

    public async Task<JsonElement> GetStatsAsync() => await Salesforce.GetFromJsonAsync<JsonElement>("/__fake/stats");

    /// <summary>(version, status) of every inbox row for the account, oldest first.</summary>
    public async Task<IReadOnlyList<(long Version, string Status)>> GetInboxAsync(string accountExtId)
    {
        await using var conn = await _db!.OpenConnectionAsync();
        var rows = await conn.QueryAsync<(long, string)>(
            "select version, status from sf_inbox where account_ext_id = @accountExtId order by version",
            new { accountExtId });
        return rows.ToList();
    }

    public async Task ExecuteSqlAsync(string sql)
    {
        await using var conn = await _db!.OpenConnectionAsync();
        await conn.ExecuteAsync(sql);
    }

    /// <summary>Polls until every expected inbox row exists and none is pending, then returns them.</summary>
    public Task<IReadOnlyList<(long Version, string Status)>> WaitForInboxSettledAsync(string accountExtId, int expectedRows)
        => Eventually.ValueAsync(
            () => GetInboxAsync(accountExtId),
            rows => rows.Count == expectedRows && rows.All(r => r.Status != "pending"),
            $"inbox rows for {accountExtId} to settle");

    private static async Task EnsureTopicAsync(string bootstrap)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build();
        try
        {
            await admin.CreateTopicsAsync([new TopicSpecification { Name = Topic, NumPartitions = 6, ReplicationFactor = 1 }]);
        }
        catch (CreateTopicsException ex) when (ex.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
        {
            // the worker created it first
        }
    }
}

[CollectionDefinition(Name)]
public sealed class AppHostCollection : ICollectionFixture<AppHostFixture>
{
    public const string Name = "AppHost";
}
