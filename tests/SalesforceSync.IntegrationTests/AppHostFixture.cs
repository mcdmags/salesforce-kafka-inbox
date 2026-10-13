using System.Globalization;
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
    public const string RelationshipTopic = "account-relationship-changes";
    public const string Accounts = "account";
    public const string Relationships = "accountRelationship";

    /// <summary>
    /// Tests think in versions (v1, v2, ...); an event's order is its meta.time, so version N is published as this
    /// time + N ms, and read back from sf_event_version the same way.
    /// </summary>
    public static readonly DateTimeOffset BaseTime = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    /// <summary>The tests' own Postgres sessions, so chaos tests can drop everyone else's connections.</summary>
    public const string TestsApplicationName = "integration-tests";
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

    protected static IResourceBuilder<ProjectResource> Project(IDistributedApplicationTestingBuilder builder, string name)
        => builder.CreateResourceBuilder(builder.Resources.OfType<ProjectResource>().Single(r => r.Name == name));

    /// <summary>Runs once the worker has created the inbox schema, before any test.</summary>
    protected virtual Task OnStartedAsync() => Task.CompletedTask;

    public async Task InitializeAsync()
    {
        using var cts = new CancellationTokenSource(StartupTimeout);
        var ct = cts.Token;

        var builder = await DistributedApplicationTestingBuilder.CreateAsync<Projects.SalesforceSync_AppHost>(
            ["--AppHost:TestMode=true"], ct);
        // Fast retries, so tests that wait for a transient failure to clear don't wait 30 s
        Project(builder, "salesforce-sync").WithEnvironment("Sync__RetryBaseDelay", "00:00:01");
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
        _db = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(inbox) { ApplicationName = TestsApplicationName }.ConnectionString);

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

    /// <summary>Publishes a raw message value as it is (for events the consumer must reject or deduplicate).</summary>
    public async Task PublishRawAsync(string key, string value)
        => await _producer!.ProduceAsync(Topic, new Message<string, string> { Key = key, Value = value });

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
        Value = AccountEvent(accountExtId, version, account, profile).ToJsonString()
    };

    /// <summary>
    /// The envelope of an account event: <c>{ "meta": { "time" }, "account": { "externalId", ...account, "profile":
    /// { "externalId", ...profile } } }</c>. The caller's objects are copied, so they can be published again.
    /// </summary>
    public static JsonObject AccountEvent(string accountExtId, long version, JsonObject account, JsonObject profile)
    {
        var profileBody = Body(ProfileIdFor(accountExtId), profile);
        var body = Body(accountExtId, account);
        body["profile"] = profileBody;
        return new JsonObject { ["meta"] = Meta(version), ["account"] = body };
    }

    /// <summary>Publishes an account-to-account relationship event, keyed by relationship id.</summary>
    public async Task PublishRelationshipAsync(string relationshipKey, long version, string accountExtId,
        string relatedAccountExtId, JsonObject fields)
    {
        var body = Body(relationshipKey, fields);
        body["accountExternalId"] = accountExtId;
        body["relatedAccountExternalId"] = relatedAccountExtId;
        await _producer!.ProduceAsync(RelationshipTopic, new Message<string, string>
        {
            Key = relationshipKey,
            Value = new JsonObject { ["meta"] = Meta(version), ["accountRelationship"] = body }.ToJsonString()
        });
    }

    public static JsonObject Meta(long version) => new()
    {
        ["time"] = BaseTime.AddMilliseconds(version).ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'", CultureInfo.InvariantCulture)
    };

    private static JsonObject Body(string externalId, JsonObject fields)
    {
        var body = new JsonObject { ["externalId"] = externalId };
        foreach (var (name, value) in fields) body[name] = value?.DeepClone();
        return body;
    }

    public static string NewAccountId() => $"E2E-{Guid.NewGuid():N}";

    public static string NewRelationshipId() => $"REL-{Guid.NewGuid():N}";

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

    /// <summary>(version, status) of every inbox row for the entity, oldest first (version N = meta.time BaseTime + N ms).</summary>
    public async Task<IReadOnlyList<(long Version, string Status)>> GetInboxAsync(string key, string entityType = Accounts)
        => (await GetInboxRowsAsync(key, entityType)).Select(r => (r.Version, r.Status)).ToList();

    public async Task<IReadOnlyList<(long Version, string Status, int RetryCount, string? ErrorCode)>> GetInboxRowsAsync(
        string key, string entityType = Accounts)
    {
        await using var conn = await _db!.OpenConnectionAsync();
        var rows = await conn.QueryAsync<(long, string, int, string?)>(
            """
            select (sf_event_version(payload) - @baseMicros) / 1000, status, retry_count, last_error_code from sf_inbox
            where sf_event_type(payload) = @entityType and sf_event_key(payload) = @key
            order by sf_event_version(payload), id
            """,
            new { entityType, key, baseMicros = (BaseTime - DateTimeOffset.UnixEpoch).Ticks / TimeSpan.TicksPerMicrosecond });
        return rows.ToList();
    }

    public async Task ExecuteSqlAsync(string sql)
    {
        await using var conn = await _db!.OpenConnectionAsync();
        await conn.ExecuteAsync(sql);
    }

    /// <summary>Polls until every expected inbox row exists and none is pending, then returns them.</summary>
    public Task<IReadOnlyList<(long Version, string Status)>> WaitForInboxSettledAsync(string key, int expectedRows,
        string entityType = Accounts)
        => Eventually.ValueAsync(
            () => GetInboxAsync(key, entityType),
            rows => rows.Count == expectedRows && rows.All(r => r.Status != "pending"),
            $"inbox rows for {entityType} {key} to settle");

    private static async Task EnsureTopicAsync(string bootstrap)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build();
        try
        {
            await admin.CreateTopicsAsync(
            [
                new TopicSpecification { Name = Topic, NumPartitions = 6, ReplicationFactor = 1 },
                new TopicSpecification { Name = RelationshipTopic, NumPartitions = 6, ReplicationFactor = 1 }
            ]);
        }
        catch (CreateTopicsException ex) when (ex.Results.All(r => r.Error.Code is ErrorCode.TopicAlreadyExists or ErrorCode.NoError))
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
