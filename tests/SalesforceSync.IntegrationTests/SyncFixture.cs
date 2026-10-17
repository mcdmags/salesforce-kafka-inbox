using System.Globalization;
using System.Net;
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
    /// <summary>The one topic: account aggregates (the Account with its relationships and contacts, before and after).</summary>
    public const string Topic = "account-changes";
    public const string Accounts = "account";
    public const string Relationships = "accountRelationship";
    public const string Contacts = "contact";

    /// <summary>
    /// Tests think in versions (v1, v2, ...); an event's order is its meta.time, so version N is published as this
    /// time + N ms, and read back from the stored payload the same way.
    /// </summary>
    public static readonly DateTimeOffset BaseTime = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    /// <summary>Sync:UnknownOutcomeHold of every pod: seconds instead of the production 10 minutes, so a test can watch a hold end.</summary>
    public static readonly TimeSpan UnknownOutcomeHold = TimeSpan.FromSeconds(4);

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
    private readonly Dictionary<string, (NpgsqlDataSource Db, IDbContextFactory<SyncDbContext> Contexts)> _migratedSchemas = [];

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
        ["Salesforce:ClientId"] = "fake-client",
        ["Salesforce:ClientSecret"] = "fake-secret",
        ["Sync:Workers"] = "4",
        // Fast retries, so tests that wait for a transient failure to clear don't wait 30 s
        ["Sync:RetryBaseDelay"] = "00:00:01",
        ["Sync:UnknownOutcomeHold"] = UnknownOutcomeHold.ToString(),
        ["Logging:LogLevel:Default"] = "Warning"
    };

    /// <summary>The mock org's configuration (<c>FakeSalesforce:*</c>).</summary>
    protected Dictionary<string, string?> FakeOrgSettings { get; } = new()
    {
        ["FakeSalesforce:Latency"] = "00:00:00",
        // Daily API allocation of an Enterprise org with 15 Salesforce licenses (100,000 + 15 x 1,000)
        ["FakeSalesforce:DailyApiLimit"] = "115000",
        // An org whose automation refuses to delete an Account that records still name (Salesforce itself wouldn't,
        // with Text foreign keys): DeleteTests covers the Account's delete waiting for its children (DEPENDENCY_NOT_SYNCED)
        ["FakeSalesforce:RefuseDeletingReferencedRecords"] = "true",
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
            var settings = new Dictionary<string, string?>(WorkerSettings)
            {
                ["ConnectionStrings:inboxdb"] = inbox,
                ["ConnectionStrings:kafka"] = kafka,
                ["Salesforce:InstanceUrl"] = instanceUrl,
                // Pods share a process here, so name them apart (the mock org counts requests per User-Agent)
                ["Salesforce:UserAgentInstance"] = $"pod-{i}",
                ["ConnectionStrings:redis"] = _redis?.GetConnectionString()
            };
            // Started one at a time: StartAsync returns once the schema is applied, so later pods find it in place
            _pods.Add(await WorkerPod.StartAsync(settings, ct));
        }

        await OnStartedAsync();
    }

    public async Task DisposeAsync()
    {
        // Pods first, so nothing is still talking to the containers when they go
        await Task.WhenAll(_pods.Select(p => p.DisposeAsync().AsTask()));
        _producer?.Dispose();
        Salesforce?.Dispose();
        foreach (var (db, _) in _migratedSchemas.Values)
        {
            await db.DisposeAsync();
        }

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

    /// <summary>
    /// Publishes an account change, keyed by account like the real upstream: an aggregate without a before, so the
    /// Account is stored whatever it was (<see cref="AsAggregate"/>).
    /// </summary>
    public async Task PublishAsync(string accountExtId, long version, JsonObject account, JsonObject profile)
    {
        await _producer!.ProduceAsync(Topic, Event(accountExtId, version, account, profile));
    }

    /// <summary>Publishes an account aggregate, keyed by account: version N's meta, and the two snapshots (null: none).</summary>
    public async Task PublishAggregateAsync(string accountExtId, long version, JsonObject? before, JsonObject? after, string? type = null)
    {
        var aggregate = new JsonObject { ["meta"] = Meta(version, type), ["before"] = before?.DeepClone(), ["after"] = after?.DeepClone() };
        await _producer!.ProduceAsync(Topic, new Message<string, string> { Key = accountExtId, Value = aggregate.ToJsonString() });
    }

    /// <summary>
    /// An aggregate's snapshot: <c>{ "externalId", ...account, "accountRelationships": [...], "contacts": [...] }</c>.
    /// The children name their Account through the aggregate; a relationship names its other Account by
    /// <c>relatedAccountExternalId</c>. The caller's objects are copied.
    /// </summary>
    public static JsonObject Snapshot(string accountExtId, JsonObject account, JsonObject[]? relationships = null, JsonObject[]? contacts = null)
    {
        var snapshot = Body(accountExtId, account);
        snapshot["accountRelationships"] = new JsonArray([.. (relationships ?? []).Select(r => r.DeepClone())]);
        snapshot["contacts"] = new JsonArray([.. (contacts ?? []).Select(c => c.DeepClone())]);
        return snapshot;
    }

    /// <summary>A child of a snapshot: its external id and fields.</summary>
    public static JsonObject Child(string externalId, JsonObject fields)
    {
        return Body(externalId, fields);
    }

    /// <summary>
    /// A per-entity account event (<see cref="AccountEvent"/>) as the aggregate that carries it: its meta, no before, and
    /// its account as after. Every part of after is stored.
    /// </summary>
    public static JsonObject AsAggregate(JsonObject accountEvent)
    {
        return new JsonObject
        {
            ["meta"] = accountEvent["meta"]?.DeepClone(),
            ["before"] = null,
            ["after"] = accountEvent["account"]?.DeepClone()
        };
    }

    /// <summary>
    /// Publishes a raw message value as it is, under any Kafka key (or none). The sync must not
    /// depend on the key, so tests use this to scatter one entity's events over partitions.
    /// </summary>
    public async Task PublishRawAsync(string? key, string value)
    {
        await _producer!.ProduceAsync(Topic, new Message<string, string> { Key = key!, Value = value });
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
            Value = AsAggregate(AccountEvent(accountExtId, version, account, profile)).ToJsonString()
        };
    }

    /// <summary>
    /// The envelope of an account event as its inbox stores it: <c>{ "meta": { "time" }, "account": { "externalId", ...account, "profile":
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

    /// <summary>
    /// Publishes a change of one account-to-account relationship: an aggregate of its first Account that only adds it (the
    /// Account itself is the same in before and after, so only the relationship is stored, at version N's time).
    /// </summary>
    public async Task PublishRelationshipAsync(string relationshipKey, long version, string accountExtId,
        string relatedAccountExtId, JsonObject fields)
    {
        var relationship = Child(relationshipKey, fields);
        relationship["relatedAccountExternalId"] = relatedAccountExtId;
        await PublishAggregateAsync(accountExtId, version, Snapshot(accountExtId, []), Snapshot(accountExtId, [], relationships: [relationship]));
    }

    /// <summary>Publishes a change of one contact: an aggregate of its Account that only adds it (see <see cref="PublishRelationshipAsync"/>).</summary>
    public async Task PublishContactAsync(string contactKey, long version, string accountExtId, JsonObject fields)
    {
        await PublishAggregateAsync(accountExtId, version, Snapshot(accountExtId, []),
            Snapshot(accountExtId, [], contacts: [Child(contactKey, fields)]));
    }

    /// <summary>
    /// Waits until the consumer has handled what was published under <paramref name="key"/> so far: a marker account's
    /// event published under the same key (one partition, consumed in order) is stored and synced.
    /// </summary>
    public async Task WaitUntilConsumedAsync(string key)
    {
        var marker = NewAccountId();
        await PublishRawAsync(key, AsAggregate(AccountEvent(marker, 1, new JsonObject { ["name"] = "marker" }, [])).ToJsonString());
        await WaitForInboxSettledAsync(marker, version: 1);
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

    /// <summary>An event's meta.time, read as the consumer reads it: the event_time it is stored with.</summary>
    public static DateTimeOffset TimeOf(JsonObject e)
    {
        return DateTimeOffset.Parse((string)e["meta"]!["time"]!, CultureInfo.InvariantCulture);
    }

    /// <summary>Version N's meta.time in microseconds since the epoch, as its event_time holds it.</summary>
    public static long Micros(long version)
    {
        return (BaseTime.AddMilliseconds(version) - DateTimeOffset.UnixEpoch).Ticks / TimeSpan.TicksPerMicrosecond;
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
        if (res.StatusCode == HttpStatusCode.NotFound)
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
    /// The write requests the mock org received (the most recent ones, oldest first: upserts, and Composite requests of
    /// upserts) that mention any of <paramref name="texts"/>: every record carries its external id.
    /// </summary>
    public async Task<List<JsonObject>> GetWriteRequestsAsync(params string[] texts)
    {
        var requests = await Salesforce.GetFromJsonAsync<List<JsonObject>>("/__fake/requests") ?? [];
        return [.. requests.Where(r => Mentions(r.ToJsonString()))];

        bool Mentions(string json)
        {
            return texts.Any(t => json.Contains(t, StringComparison.Ordinal));
        }
    }

    /// <summary>The versions written to an Account, oldest first: the "vN" Descriptions in its write history.</summary>
    public async Task<List<long>> GetWrittenVersionsAsync(string accountExtId)
    {
        var history = await GetHistoryAsync("Account", accountExtId);
        return [.. history.Where(w => w.ContainsKey("Description"))
            .Select(w => long.Parse(((string)w["Description"]!)[1..], CultureInfo.InvariantCulture))];
    }

    /// <summary>Makes the mock org reject the next <paramref name="times"/> writes of one record with a Salesforce error code.</summary>
    public async Task InjectFaultAsync(string sobject, string externalId, string errorCode, int times)
    {
        using var res = await Salesforce.PostAsJsonAsync("/__fake/faults",
            new JsonObject { ["sObject"] = sobject, ["externalId"] = externalId, ["errorCode"] = errorCode, ["times"] = times });
        res.EnsureSuccessStatusCode();
    }

    /// <summary>
    /// Makes the mock org apply the next <paramref name="times"/> writes of one record and then drop the connection,
    /// so the worker never hears whether they landed.
    /// </summary>
    public async Task LoseResponsesAsync(string sobject, string externalId, int times = 1)
    {
        using var res = await Salesforce.PostAsJsonAsync("/__fake/lost-responses",
            new JsonObject { ["sObject"] = sobject, ["externalId"] = externalId, ["times"] = times });
        res.EnsureSuccessStatusCode();
    }

    private static readonly IReadOnlyDictionary<string, string> InboxTables = ReadInboxTables();

    /// <summary>The inbox table of an entity type, as the EF model names it.</summary>
    public static string InboxTable(string entityType = Accounts)
    {
        return InboxTables[entityType];
    }

    private static Dictionary<string, string> ReadInboxTables()
    {
        using var model = SyncDbContext.ForModel();
        return new()
        {
            [Accounts] = model.TableOf<AccountInboxRow>(),
            [Relationships] = model.TableOf<AccountRelationshipInboxRow>(),
            [Contacts] = model.TableOf<ContactInboxRow>()
        };
    }

    /// <summary>The account inbox through the worker's own repository, to play a worker by hand.</summary>
    public IRepository<AccountInboxRow> AccountInbox => new InboxRepository<AccountInboxRow>(Contexts, Options.Create(new SyncOptions()));

    /// <summary>(version, status) of the entity's inbox row, or null if it has none (version N = meta.time BaseTime + N ms).</summary>
    public async Task<(long Version, string Status)?> GetInboxAsync(string key, string entityType = Accounts)
    {
        return await GetInboxRowAsync(key, entityType) is { } row ? (row.Version, row.Status) : null;
    }

    /// <summary>
    /// The entity's inbox row (it has one at most: a newer event overwrites it), or null. The version is read here from
    /// the stored payload, like the worker does (in C#, not in SQL).
    /// </summary>
    public async Task<InboxRowState?> GetInboxRowAsync(string key, string entityType = Accounts)
    {
        await using var conn = await Db.OpenConnectionAsync();
        var rows = await conn.QueryAsync<StoredRow>(
            $"SELECT id, payload::text, status, attempt_count, last_error_code, next_attempt_at, lock_token FROM {InboxTable(entityType)} WHERE entity_id = @key",
            new { key });
        return rows.Select(r => new InboxRowState(r.Id, VersionOf(r.Payload), r.Status, r.AttemptCount, r.ErrorCode, r.NextAttemptAt, r.LockToken))
            .SingleOrDefault();
    }

    /// <summary>An inbox row as read, its payload as stored.</summary>
    private sealed record StoredRow(long Id, string Payload, string Status, int AttemptCount, string? ErrorCode, DateTime? NextAttemptAt,
        Guid? LockToken);

    /// <summary>The version of a stored payload: its meta.time, BaseTime + N ms.</summary>
    public static long VersionOf(string payload)
    {
        return (long)(TimeOf(JsonNode.Parse(payload)!.AsObject()) - BaseTime).TotalMilliseconds;
    }

    /// <summary>
    /// Stores account events directly, in this order, in one transaction (as the consumer would one after the other):
    /// the entity's row ends with the newest of them, and no claim sees the ones in between.
    /// </summary>
    public async Task InsertIntoInboxAsync(string entityId, params JsonObject[] events)
    {
        await using var conn = await Db.OpenConnectionAsync();
        await using var tx = await conn.BeginTransactionAsync();
        var table = InboxTable();
        foreach (var e in events)
        {
            await conn.StoreAsync(table, entityId, TimeOf(e), e.ToJsonString(), tx);
        }
        await tx.CommitAsync();
    }

    public async Task ExecuteSqlAsync(string sql)
    {
        await using var conn = await Db.OpenConnectionAsync();
        await conn.ExecuteAsync(sql);
    }

    /// <summary>
    /// A data source whose connections work in <paramref name="schema"/>, created empty (dropped first, if a run left it
    /// behind), so the test's tables are apart from the ones the pods work. The test drops it at the end.
    /// </summary>
    public async Task<NpgsqlDataSource> CreateSchemaAsync(string schema)
    {
        await ExecuteSqlAsync($"DROP SCHEMA IF EXISTS {schema} CASCADE; CREATE SCHEMA {schema}");
        return NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(ConnectionString)
        {
            SearchPath = schema,
            ApplicationName = TestsApplicationName
        }.ConnectionString);
    }

    /// <summary>
    /// <paramref name="schema"/>, migrated, with an empty account inbox, and contexts on it: for tests that drive the
    /// worker's SQL by hand on rows the pods never see. Created on first use and only emptied after that: a data source
    /// per test would cost EF a service provider each, and more than twenty fail the run.
    /// </summary>
    public async Task<(NpgsqlDataSource Db, IDbContextFactory<SyncDbContext> Contexts)> MigratedSchemaAsync(string schema)
    {
        if (!_migratedSchemas.TryGetValue(schema, out var migrated))
        {
            var db = await CreateSchemaAsync(schema);
            var contexts = SyncDbContext.CreateFactory(db);
            await SyncSchema.MigrateAsync(contexts);
            _migratedSchemas[schema] = migrated = (db, contexts);
        }

        await using var conn = await migrated.Db.OpenConnectionAsync();
        await conn.ExecuteAsync($"TRUNCATE {InboxTable()}");
        return migrated;
    }

    /// <summary>Every kind of outcome a worker records for a claim (see <see cref="RecordOutcomeAsync"/>).</summary>
    public static TheoryData<string> Outcomes => ["done", "failed", "dead", "released", "failed all", "held"];

    /// <summary>Records one of <see cref="Outcomes"/> for a claim of one row, through the worker's repository.</summary>
    public static Task RecordOutcomeAsync(IRepository<AccountInboxRow> inbox, InboxClaim claim, string outcome)
    {
        var row = claim.RowIds.Single();
        var failure = new FailedEvent(row, "failed", "SOME_CODE");
        var none = CancellationToken.None;
        return outcome switch
        {
            "done" => inbox.RecordAsync(claim, new BatchOutcomes([row], [], []), none),
            "failed" => inbox.RecordAsync(claim, new BatchOutcomes([], [], [failure]), none),
            "dead" => inbox.RecordAsync(claim, new BatchOutcomes([], [failure], []), none),
            "released" => inbox.RecordAsync(claim, new BatchOutcomes([], [], []), none),
            "failed all" => inbox.FailAllAsync(claim, "failed", "SOME_CODE", none),
            "held" => inbox.HoldAsync(claim, "lost", none),
            _ => throw new ArgumentOutOfRangeException(nameof(outcome))
        };
    }

    /// <summary>Polls until the entity's inbox row holds <paramref name="version"/> and is not pending, then returns it.</summary>
    public async Task<(long Version, string Status)> WaitForInboxSettledAsync(string key, long version, string entityType = Accounts)
    {
        var row = await Eventually.ValueAsync(
                () => GetInboxAsync(key, entityType),
                row => row is { } r && r.Version == version && r.Status != "pending",
                $"the inbox row of {entityType} {key} to settle at v{version}");
        return row!.Value;
    }

    /// <summary>librdkafka wants host:port; Testcontainers reports a URI (PLAINTEXT://host:port/).</summary>
    private static string BootstrapServers(string address)
    {
        return Uri.TryCreate(address, UriKind.Absolute, out var uri) ? $"{uri.Host}:{uri.Port}" : address;
    }

    /// <summary>Creates the topic up front (6 partitions, like production), so pods never race to auto-create it.</summary>
    private static async Task EnsureTopicsAsync(string bootstrap)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build();
        try
        {
            await admin.CreateTopicsAsync([new TopicSpecification { Name = Topic, NumPartitions = 6, ReplicationFactor = 1 }]);
        }
        catch (CreateTopicsException ex) when (ex.Results.All(r => r.Error.Code is ErrorCode.TopicAlreadyExists or ErrorCode.NoError))
        {
            // already there
        }
    }
}

/// <summary>An inbox row as the tests see it; <paramref name="Version"/> comes from the payload's meta.time.</summary>
public sealed record InboxRowState(long Id, long Version, string Status, int AttemptCount, string? ErrorCode, DateTime? NextAttemptAt,
    Guid? LockToken);

[CollectionDefinition(Name)]
public sealed class SyncCollection : ICollectionFixture<SyncFixture>
{
    public const string Name = "Sync";
}