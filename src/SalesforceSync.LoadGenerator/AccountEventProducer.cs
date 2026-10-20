using System.Diagnostics;
using System.Globalization;
using System.Text.Json.Nodes;
using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace SalesforceSync.LoadGenerator;

/// <summary>
/// Publishes account aggregate events (keyed by account external id, like the real upstream, though the sync orders by
/// meta.time and never reads the key): the Account with its Profile, relationships and contacts, before and after each
/// change, including rapid bursts to the same account to exercise ordering and coalescing. Each event changes the
/// Account and, now and then, adds, changes or removes one of its relationships or contacts (a removed child is a
/// delete for the sync); a child can name an Account that isn't synced yet. Events use the real envelope:
/// <c>{ "meta": { "time", "type" }, "before": { ... } | null, "after": { ... } }</c>, with the source system's camelCase
/// field names. Every snapshot carries the full records.
/// </summary>
public sealed class AccountEventProducer(
    IProducer<string, string> producer,
    IOptions<LoadGeneratorOptions> options,
    ILogger<AccountEventProducer> log) : BackgroundService
{
    private static readonly string[] Ratings = ["Hot", "Warm", "Cold"];
    private static readonly string[] Tiers = ["Bronze", "Silver", "Gold", "Platinum"];
    private static readonly string[] RelationshipTypes = ["Parent", "Subsidiary", "Partner", "Reseller"];

    // Read once at startup: a template that doesn't conform throws here, before anything is produced
    private readonly AggregateTemplate? _template = options.Value.Template is { Length: > 0 } path
        ? options.Value.UseAutoFixture
            ? throw new InvalidOperationException("LoadGenerator: set Template or UseAutoFixture, not both")
            : AggregateTemplate.Load(path)
        : null;

    private readonly AutoFixtureShape? _autoFixture = options.Value.UseAutoFixture ? new AutoFixtureShape() : null;

    // Account and Profile external ids are GUIDs, as in the source system. From their own seeded sequence, so every run
    // produces the same accounts (a rerun updates them instead of creating new ones)
    private readonly string[] _accountIds = Guids(options.Value.Accounts, seed: 1);
    private readonly string[] _profileIds = Guids(options.Value.Accounts, seed: 2);

    private long _lastTimeTicks;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var o = options.Value;
        var versions = new long[o.Accounts];
        var snapshots = new JsonObject?[o.Accounts]; // each account's last "after", the next event's "before"
        var random = new Random(42);
        var interval = TimeSpan.FromSeconds(1.0 / Math.Max(1, o.EventsPerSecond));
        var sent = 0L;
        var sw = Stopwatch.StartNew();

        log.LogInformation("Producing to {Topic}: {Accounts} accounts at {Rate}/s (total {Total}), shape from {Template}",
            o.Topic, o.Accounts, o.EventsPerSecond, o.TotalEvents == 0 ? "unbounded" : o.TotalEvents,
            o.UseAutoFixture ? "AutoFixture" : o.Template is { Length: > 0 } ? o.Template : "the built-in example");

        while (!stoppingToken.IsCancellationRequested && (o.TotalEvents == 0 || sent < o.TotalEvents))
        {
            var account = random.Next(o.Accounts);
            var burst = random.NextDouble() < o.BurstProbability ? random.Next(2, 6) : 1;

            for (var i = 0; i < burst && (o.TotalEvents == 0 || sent < o.TotalEvents); i++)
            {
                var key = _accountIds[account];
                var before = snapshots[account];
                var after = Next(account, ++versions[account], before, o, random);
                snapshots[account] = after;

                var aggregate = new JsonObject
                {
                    ["meta"] = Meta(before is null ? "inserted" : "updated"),
                    ["before"] = before?.DeepClone(),
                    ["after"] = after.DeepClone()
                };
                Produce(o.Topic, key, aggregate.ToJsonString());
                sent++;
            }

            // Simple pacing: keep the average rate at EventsPerSecond
            var due = (interval * sent) - sw.Elapsed;
            if (due > TimeSpan.Zero)
            {
                await Task.Delay(due, stoppingToken);
            }

            if (sent % 5_000 == 0)
            {
                log.LogInformation("Produced {Sent} events", sent);
            }
        }

        producer.Flush(TimeSpan.FromSeconds(10));
        log.LogInformation("Done: produced {Sent} events in {Elapsed}", sent, sw.Elapsed);
    }

    private void Produce(string topic, string key, string value)
    {
        producer.Produce(topic, new Message<string, string> { Key = key, Value = value }, report =>
        {
            if (report.Error.IsError)
            {
                log.LogWarning("Produce failed: {Reason}", report.Error.Reason);
            }
        });
    }

    /// <summary>
    /// The account's next snapshot: the Account changed, its children as they were, maybe one of them touched. Records
    /// come from the template (copies of its examples, ids and version markers set), from AutoFixture (random values of
    /// the event classes), or else are the built-in ones.
    /// </summary>
    private JsonObject Next(int account, long version, JsonObject? before, LoadGeneratorOptions o, Random random)
    {
        var key = _accountIds[account];
        var relationships = before?["accountRelationships"]?.DeepClone().AsArray() ?? [];
        var contacts = before?["contacts"]?.DeepClone().AsArray() ?? [];

        if (o.Accounts > 1 && random.NextDouble() < o.RelationshipProbability)
        {
            // Owned by this account's aggregate: its key names this account first
            var other = (account + 1 + random.Next(o.Accounts - 1)) % o.Accounts;
            var related = _accountIds[other];
            Touch(relationships, $"REL-{account:D6}-{other:D6}", o, random, id => _template?.Relationship(id, related, version)
                ?? _autoFixture?.Relationship(id, related)
                ?? BuiltInRelationship(id, related, random));
        }

        if (random.NextDouble() < o.ContactProbability)
        {
            Touch(contacts, $"CON-{account:D6}-{random.Next(3)}", o, random, id => _template?.Contact(id, version)
                ?? _autoFixture?.Contact(id)
                ?? BuiltInContact(id, random));
        }

        if (_template is not null)
        {
            return _template.Account(key, version, relationships, contacts);
        }
        if (_autoFixture is not null)
        {
            return _autoFixture.Account(key, _profileIds[account], relationships, contacts);
        }

        var after = BuildAccount(key, _profileIds[account], version, random);
        after["accountRelationships"] = relationships;
        after["contacts"] = contacts;
        return after;
    }

    /// <summary>Adds the child, or, if the list has it, removes it (<see cref="LoadGeneratorOptions.ChildRemovalProbability"/>) or replaces it.</summary>
    private static void Touch(JsonArray children, string id, LoadGeneratorOptions o, Random random, Func<string, JsonObject> build)
    {
        var existing = children.FirstOrDefault(c => (string?)c?["externalId"] == id);
        if (existing is not null)
        {
            children.Remove(existing);
            if (random.NextDouble() < o.ChildRemovalProbability)
            {
                return;
            }
        }

        children.Add(build(id));
    }

    private static JsonObject BuiltInRelationship(string id, string related, Random random)
    {
        return new JsonObject
        {
            ["externalId"] = id,
            ["relatedAccountExternalId"] = related,
            ["type"] = RelationshipTypes[random.Next(RelationshipTypes.Length)],
            ["isActive"] = random.NextDouble() < 0.9
        };
    }

    private static JsonObject BuiltInContact(string id, Random random)
    {
        return new JsonObject
        {
            ["externalId"] = id,
            ["firstName"] = "Contact",
            ["lastName"] = id,
            ["email"] = $"{id.ToLowerInvariant()}@example.com",
            ["phone"] = $"+1 555 {random.Next(10_000):D4}"
        };
    }

    private static JsonObject BuildAccount(string accountKey, string profileKey, long version, Random random)
    {
        // Full records, like the source system sends them: every event carries every field, and most of them
        // don't change from one event to the next (so the diff has something to skip).
        return new JsonObject
        {
            ["externalId"] = accountKey,
            ["name"] = $"Account {accountKey}",
            ["rating"] = Ratings[random.Next(Ratings.Length)],
            ["industry"] = "Technology",
            ["description"] = $"Updated to v{version}",
            ["annualRevenue"] = 1_000_000 + (random.Next(0, 100) * 10_000),
            ["numberOfEmployees"] = 250,
            ["phone"] = "+1 555 0100",
            ["website"] = "https://example.com",
            ["isActive"] = true,
            ["tags"] = new JsonArray("B2B", "Retail"),
            ["customerSince"] = "2020-01-15",
            ["lastContactedAt"] = null, // empty in the source: cleared in Salesforce if set there
            ["profile"] = new JsonObject
            {
                ["externalId"] = profileKey,
                ["name"] = $"Profile {accountKey}",
                ["tier"] = Tiers[random.Next(Tiers.Length)],
                ["score"] = random.Next(0, 101),
                ["discountPercent"] = 12.5,
                ["notes"] = null,
                ["optIn"] = random.NextDouble() < 0.5,
                ["startDate"] = "2020-02-01"
            }
        };
    }

    private static string[] Guids(int count, int seed)
    {
        var random = new Random(seed);
        var bytes = new byte[16];
        var ids = new string[count];
        for (var i = 0; i < count; i++)
        {
            random.NextBytes(bytes);
            ids[i] = new Guid(bytes).ToString();
        }

        return ids;
    }

    /// <summary>
    /// meta.time: now, but always later than the previous event's (to the microsecond Postgres keeps), so events
    /// produced within the same clock tick still have a distinct order.
    /// </summary>
    private JsonObject Meta(string type)
    {
        var ticks = Math.Max(DateTimeOffset.UtcNow.Ticks, _lastTimeTicks + TimeSpan.TicksPerMicrosecond);
        ticks -= ticks % TimeSpan.TicksPerMicrosecond;
        _lastTimeTicks = ticks;
        return new JsonObject
        {
            ["time"] = new DateTimeOffset(ticks, TimeSpan.Zero).ToString("yyyy-MM-dd'T'HH:mm:ss.ffffff'Z'", CultureInfo.InvariantCulture),
            // An account's first event is its insert, every later one an update
            ["type"] = type
        };
    }
}