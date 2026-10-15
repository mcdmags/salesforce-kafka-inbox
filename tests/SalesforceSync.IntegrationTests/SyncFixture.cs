using System.Globalization;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Nodes;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Npgsql;
using SalesforceSync.Data;
using SalesforceSync.Inbox;
using SalesforceSync.IntegrationTests.Hosting;
using SalesforceSync.Sync;
using Testcontainers.Kafka;
using Testcontainers.PostgreSql;
using Testcontainers.Redis;

namespace SalesforceSync.IntegrationTests;

/// <summary>
/// The whole sync, started once per xUnit collection: Postgres and Kafka (and Redis, if asked) in containers (Testcontainers), the mock
/// Salesforce org on Kestrel in this process, and <see cref="Pods"/> worker pods, each an in-process
/// <see cref="Microsoft.Extensions.Hosting.IHost"/> wired by the worker's own registrations. Tests produce to Kafka
/// and assert on the mock org and the Postgres inbox. Subclasses reshape the deployment (pods, per-pod settings, the
/// org's behavior) in their constructor through <see cref="WorkerSettings"/> and <see cref="FakeOrgSettings"/>.
/// Requires only Docker.
/// </summary>
public class SyncFixture : IAsyncLifetime
{
    public const string Topic = "account-changes";
    public const string RelationshipTopic = "account-relationship-changes";
    public const string ContactTopic = "contact-changes";
    public const string Accounts = "account";
    public const string Relationships = "accountRelationship";
    public const string Contacts = "contact";

    /// <summary>
    /// Tests think in versions (v1, v2, ...); an event's order is its meta.time, so version N is published as this
    /// time + N ms, and read back from the stored payload the same way.
    /// </summary>
    public static readonly DateTimeOffset BaseTime = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    /// <summary>The tests' own Postgres sessions, so chaos tests can drop everyone else's connections.</summary>
    public const string TestsApplicationName = "integration-tests";

    private static readonly TimeSpan StartupTimeout = TimeSpan.FromMinutes(5);

    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder("postgres:17")
        .WithDatabase("inboxdb")
        .WithUsername("postgres")
        .WithPassword("postgres")
        // Room for several pods' pools plus the tests' own connections
        .WithCommand("-c", "max_connections=300")
        .Build();

    private readonly KafkaContainer _kafka = new KafkaBuilder("apache/kafka:3.9.1").Build();

    /// <summary>Only with <see cref="SharedDescribeCache"/>.</summary>
    private RedisContainer? _redis;

    private readonly List<WorkerPod> _pods = [];
    private FakeOrgHost? _org;
    private IProducer<string, string>? _producer;
    private NpgsqlDataSource? _db;
    private IDbContextFactory<SyncDbContext>? _contexts;

    /// <param name="pods">Worker pods (replicas) sharing the inbox, the Kafka consumer group and the org.</param>
    protected SyncFixture(int pods)
    {
        Pods = pods;
    }

    public SyncFixture() : this(pods: 1)
    {
    }

    public int Pods { get; }

    /// <summary>
    /// The pods share their Describe cache through Redis (a container, <c>ConnectionStrings:redis</c>), as in production;
    /// otherwise each pod has its own in-memory one.
    /// </summary>
    protected bool SharedDescribeCache { get; init; }

    /// <summary>
    /// Configuration every pod gets on top of the worker's appsettings.json (like the environment variables of a
    /// Kubernetes deployment). Connection strings and the org's URL are added at startup.
    /// </summary>
    protected Dictionary<string, string?> WorkerSettings { get; } = new()
    {
        ["Salesforce:UseFake"] = "false", // the real OAuth + REST client, over HTTP to the mock org
        ["Salesforce:ClientId"] = "fake-client",
        ["Salesforce:ClientSecret"] = "fake-secret",
        ["Sync:Workers"] = "4",
        // Fast retries, so tests that wait for a transient failure to clear don't wait 30 s
        ["Sync:RetryBaseDelay"] = "00:00:01",
        // Seconds instead of the production 10 minutes, so the unknown-outcome test can watch a hold end
        ["Sync:UnknownOutcomeHold"] = "00:00:04",
        ["Logging:LogLevel:Default"] = "Warning"
    };

    /// <summary>The mock org's configuration (<c>FakeSalesforce:*</c>).</summary>
    protected Dictionary<string, string?> FakeOrgSettings { get; } = new()
    {
        ["FakeSalesforce:Latency"] = "00:00:00",
        // The worker's SalesforceLimits:DailyApiAllocation (appsettings.json)
        ["FakeSalesforce:DailyApiLimit"] = "115000",
        ["Logging:LogLevel:Default"] = "Warning"
    };

    public HttpClient Salesforce { get; private set; } = null!;

    public NpgsqlDataSource Db => _db ?? throw new InvalidOperationException("Fixture not started");

    /// <summary>The database's full connection string (the data source's own hides the password).</summary>
    public string ConnectionString { get; private set; } = "";

    public IDbContextFactory<SyncDbContext> Contexts => _contexts ??= SyncDbContext.CreateFactory(Db);

    /// <summary>The running pods, in start order.</summary>
    public IReadOnlyList<WorkerPod> RunningPods => _pods;

    /// <summary>Runs once the inbox schema exists, before any test.</summary>
    protected virtual Task OnStartedAsync()
    {
        return Task.CompletedTask;
    }

    public async Task InitializeAsync()
    {
        using var cts = new CancellationTokenSource(StartupTimeout);
        var ct = cts.Token;

        var org = FakeOrgHost.StartAsync(FakeOrgSettings, ct);
        _redis = SharedDescribeCache ? new RedisBuilder("redis:7.4-alpine").Build() : null;
        await Task.WhenAll(_postgres.StartAsync(ct), _kafka.StartAsync(ct), org, _redis?.StartAsync(ct) ?? Task.CompletedTask);
        _org = await org;
        Salesforce = new HttpClient { BaseAddress = _org.Url };

        var kafka = BootstrapServers(_kafka.GetBootstrapAddress());
        await EnsureTopicsAsync(kafka);
        _producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = kafka,
            Acks = Acks.All,
            EnableIdempotence = true
        }).Build();

        var inbox = _postgres.GetConnectionString();
        ConnectionString = inbox;
        _db = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(inbox) { ApplicationName = TestsApplicationName }.ConnectionString);

        var instanceUrl = _org.Url.GetLeftPart(UriPartial.Authority);
        for (var i = 0; i < Pods; i++)
        {
            var name = $"pod-{i}";
            var settings = new Dictionary<string, string?>(WorkerSettings)
            {
                ["ConnectionStrings:inboxdb"] = inbox,
                ["ConnectionStrings:kafka"] = kafka,
                ["Salesforce:InstanceUrl"] = instanceUrl,
                ["Salesforce:TokenUrl"] = $"{instanceUrl}/services/oauth2/token",
                // Pods share a process here, so name them apart (the mock org counts requests per User-Agent)
                ["Salesforce:UserAgentInstance"] = name,
                ["ConnectionStrings:redis"] = _redis?.GetConnectionString()
            };
            // Started one at a time: StartAsync returns once the schema is applied, so later pods find it in place
            _pods.Add(await WorkerPod.StartAsync(name, settings, ct));
        }

        await OnStartedAsync();
    }

    public async Task DisposeAsync()
    {
        // Pods first, so nothing is still talking to the containers when they go
        await Task.WhenAll(_pods.Select(p => p.DisposeAsync().AsTask()));
        _producer?.Dispose();
        Salesforce?.Dispose();
        if (_db is not null)
        {
            await _db.DisposeAsync();
        }

        if (_org is not null)
        {
            await _org.DisposeAsync();
        }

        await Task.WhenAll(_kafka.DisposeAsync().AsTask(), _postgres.DisposeAsync().AsTask(),
            _redis?.DisposeAsync().AsTask() ?? Task.CompletedTask);
    }

    /// <summary>Publishes an account change event, keyed by account like the real upstream.</summary>
    public async Task PublishAsync(string accountExtId, long version, JsonObject account, JsonObject profile)
    {
        await _producer!.ProduceAsync(Topic, Event(accountExtId, version, account, profile));
    }

    /// <summary>
    /// Publishes a raw message value as it is, under any Kafka key (or none) and on either topic. The sync must not
    /// depend on the key, so tests use this to scatter one entity's events over partitions.
    /// </summary>
    public async Task PublishRawAsync(string? key, string value, string topic = Topic)
    {
        await _producer!.ProduceAsync(topic, new Message<string, string> { Key = key!, Value = value });
    }

    /// <summary>Fire-and-forget produce for bulk loads; call <see cref="Flush"/> afterwards.</summary>
    public void Publish(string accountExtId, long version, JsonObject account, JsonObject profile)
    {
        _producer!.Produce(Topic, Event(accountExtId, version, account, profile), report =>
            {
                if (report.Error.IsError)
                {
                    throw new InvalidOperationException($"Produce failed: {report.Error.Reason}");
                }
            });
    }

    public void Flush()
    {
        if (_producer!.Flush(TimeSpan.FromSeconds(30)) > 0)
        {
            throw new TimeoutException("Kafka producer did not flush all messages");
        }
    }

    private static Message<string, string> Event(string accountExtId, long version, JsonObject account, JsonObject profile)
    {
        return new()
        {
            Key = accountExtId,
            Value = AccountEvent(accountExtId, version, account, profile).ToJsonString()
        };
    }

    /// <summary>
    /// The envelope of an account event: <c>{ "meta": { "time" }, "account": { "externalId", ...account, "profile":
    /// { "externalId", ...profile } } }</c>, with the payload's camelCase names (<c>name</c>, <c>rating</c>, <c>tier</c>,
    /// ...). An event is the full record: what isn't given is empty. The caller's objects are copied, so they can be
    /// published again.
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

    /// <summary>Publishes a contact event, keyed by contact id.</summary>
    public async Task PublishContactAsync(string contactKey, long version, string accountExtId, JsonObject fields)
    {
        var body = Body(contactKey, fields);
        body["accountExternalId"] = accountExtId;
        await _producer!.ProduceAsync(ContactTopic, new Message<string, string>
        {
            Key = contactKey,
            Value = new JsonObject { ["meta"] = Meta(version), ["contact"] = body }.ToJsonString()
        });
    }

    /// <summary>meta of version N: its time, and by default "inserted" for version 1 and "updated" for every later one.</summary>
    public static JsonObject Meta(long version, string? type = null)
    {
        return new()
        {
            ["time"] = BaseTime.AddMilliseconds(version).ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'", CultureInfo.InvariantCulture),
            ["type"] = type ?? (version == 1 ? "inserted" : "updated")
        };
    }

    private static JsonObject Body(string externalId, JsonObject fields)
    {
        var body = new JsonObject { ["externalId"] = externalId };
        foreach (var (name, value) in fields)
        {
            body[name] = value?.DeepClone();
        }

        return body;
    }

    /// <summary>A fresh account external id, so tests don't interfere through shared state.</summary>
    public static string NewAccountId()
    {
        return Any.String("E2E-");
    }

    public static string NewRelationshipId()
    {
        return Any.String("REL-");
    }

    public static string NewContactId()
    {
        return Any.String("CON-");
    }

    public static string ProfileIdFor(string accountExtId)
    {
        return accountExtId.Replace("E2E-", "PRF-");
    }

    /// <summary>Record from the mock org by external id, or null if it does not exist (yet).</summary>
    public async Task<JsonObject?> GetRecordAsync(string sobject, string externalId)
    {
        using var res = await Salesforce.GetAsync($"/__fake/records/{sobject}/{Uri.EscapeDataString(externalId)}");
        if (res.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return null;
        }

        res.EnsureSuccessStatusCode();
        return await res.Content.ReadFromJsonAsync<JsonObject>();
    }

    /// <summary>Field values of every write the mock org applied to the record, oldest first (the create first).</summary>
    public async Task<List<JsonObject>> GetHistoryAsync(string sobject, string externalId)
    {
        return await Salesforce.GetFromJsonAsync<List<JsonObject>>(
                   $"/__fake/history/{sobject}/{Uri.EscapeDataString(externalId)}")
               ?? throw new InvalidOperationException("Empty history response");
    }

    public async Task<JsonElement> GetStatsAsync()
    {
        return await Salesforce.GetFromJsonAsync<JsonElement>("/__fake/stats");
    }

    /// <summary>
    /// The composite write requests the mock org received (the most recent ones, oldest first) that mention any of
    /// <paramref name="texts"/>: a create carries the record's external id, an update only its Id.
    /// </summary>
    public async Task<List<JsonObject>> GetWriteRequestsAsync(params string[] texts)
    {
        return [.. (await Salesforce.GetFromJsonAsync<List<JsonObject>>("/__fake/requests") ?? []).Where(r => r.ToJsonString() is var json && texts.Any(t => json.Contains(t, StringComparison.Ordinal)))];
    }

    /// <summary>Makes the mock org reject the next <paramref name="times"/> writes of one record with a Salesforce error code.</summary>
    public async Task InjectFaultAsync(string sobject, string externalId, string errorCode, int times)
    {
        using var res = await Salesforce.PostAsJsonAsync("/__fake/faults",
            new JsonObject { ["sObject"] = sobject, ["externalId"] = externalId, ["errorCode"] = errorCode, ["times"] = times });
        res.EnsureSuccessStatusCode();
    }

    /// <summary>The inbox table of an entity type, as the EF model names it.</summary>
    public static string InboxTable(string entityType = Accounts)
    {
        // Only the model is read: nothing connects to this address
        using var model = new SyncDbContext(new DbContextOptionsBuilder<SyncDbContext>().UseNpgsql("Host=unused").Options);
        return entityType switch
        {
            Relationships => model.TableOf<AccountRelationshipInboxRow>(),
            Contacts => model.TableOf<ContactInboxRow>(),
            _ => model.TableOf<AccountInboxRow>()
        };
    }

    /// <summary>The account inbox through the worker's own repository, to play a worker by hand.</summary>
    public IRepository<AccountInboxRow> AccountInbox => new InboxRepository<AccountInboxRow>(Contexts, Options.Create(new SyncOptions()));

    /// <summary>(version, status) of every inbox row for the entity, by version then arrival (version N = meta.time BaseTime + N ms).</summary>
    public async Task<IReadOnlyList<(long Version, string Status)>> GetInboxAsync(string key, string entityType = Accounts)
    {
        return [.. (await GetInboxRowsAsync(key, entityType)).Select(r => (r.Version, r.Status))];
    }

    /// <summary>
    /// The entity's inbox rows. The table has no event columns, so the version is read here from the stored payload,
    /// like the worker does (in C#, not in SQL).
    /// </summary>
    public async Task<IReadOnlyList<InboxRow>> GetInboxRowsAsync(string key, string entityType = Accounts)
    {
        await using var conn = await Db.OpenConnectionAsync();
        var rows = await conn.QueryAsync<(long Id, string Payload, string Status, int RetryCount, string? ErrorCode, DateTime? LockedUntil, Guid? LockToken)>(
            $"""
            select id, payload::text, status, retry_count, last_error_code, locked_until, lock_token
            from {InboxTable(entityType)} where entity_id = @key order by id
            """, new { key });
        return [.. rows
            .Select(r => new InboxRow(r.Id, VersionOf(r.Payload), r.Status, r.RetryCount, r.ErrorCode, r.LockedUntil, r.LockToken))
            .OrderBy(r => r.Version).ThenBy(r => r.Id)];
    }

    private static long VersionOf(string payload)
    {
        var time = DateTimeOffset.Parse((string)JsonNode.Parse(payload)!["meta"]!["time"]!, CultureInfo.InvariantCulture);
        return (long)(time - BaseTime).TotalMilliseconds;
    }

    /// <summary>Stores events directly in the inbox, in one statement, so a single claim is sure to see them together.</summary>
    public async Task InsertIntoInboxAsync(string entityId, params JsonObject[] events)
    {
        await using var conn = await Db.OpenConnectionAsync();
        await conn.ExecuteAsync(
            $"insert into {InboxTable()} (entity_id, payload) select @entityId, p::jsonb from unnest(@payloads) with ordinality as u(p, n) order by n",
            new { entityId, payloads = events.Select(e => e.ToJsonString()).ToArray() });
    }

    public async Task ExecuteSqlAsync(string sql)
    {
        await using var conn = await Db.OpenConnectionAsync();
        await conn.ExecuteAsync(sql);
    }

    /// <summary>Polls until every expected inbox row exists and none is pending, then returns them (by version, then arrival).</summary>
    public Task<IReadOnlyList<(long Version, string Status)>> WaitForInboxSettledAsync(string key, int expectedRows,
        string entityType = Accounts)
    {
        return Eventually.ValueAsync(
                () => GetInboxAsync(key, entityType),
                rows => rows.Count == expectedRows && rows.All(r => r.Status != "pending"),
                $"inbox rows for {entityType} {key} to settle");
    }

    /// <summary>librdkafka wants host:port; Testcontainers reports a URI (PLAINTEXT://host:port/).</summary>
    private static string BootstrapServers(string address)
    {
        return Uri.TryCreate(address, UriKind.Absolute, out var uri) ? $"{uri.Host}:{uri.Port}" : address;
    }

    /// <summary>Creates every topic up front (6 partitions, like production), so pods never race to auto-create them.</summary>
    private static async Task EnsureTopicsAsync(string bootstrap)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build();
        try
        {
            await admin.CreateTopicsAsync(
            [
                new TopicSpecification { Name = Topic, NumPartitions = 6, ReplicationFactor = 1 },
                new TopicSpecification { Name = RelationshipTopic, NumPartitions = 6, ReplicationFactor = 1 },
                new TopicSpecification { Name = ContactTopic, NumPartitions = 6, ReplicationFactor = 1 }
            ]);
        }
        catch (CreateTopicsException ex) when (ex.Results.All(r => r.Error.Code is ErrorCode.TopicAlreadyExists or ErrorCode.NoError))
        {
            // already there
        }
    }
}

/// <summary>An inbox row as the tests see it; <paramref name="Version"/> comes from the payload's meta.time.</summary>
public sealed record InboxRow(long Id, long Version, string Status, int RetryCount, string? ErrorCode, DateTime? LockedUntil, Guid? LockToken);

[CollectionDefinition(Name)]
public sealed class SyncCollection : ICollectionFixture<SyncFixture>
{
    public const string Name = "Sync";
}