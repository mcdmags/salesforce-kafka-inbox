using System.Diagnostics;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Options;
using SalesforceSync.Sync;

namespace SalesforceSync.LoadGenerator;

/// <summary>
/// Publishes account aggregate events (keyed by account id, like the real upstream, though the sync orders by
/// meta.time and never reads the key): the account with its profile, relationships and contacts, before and after each
/// change, including rapid bursts to the same account to exercise ordering and coalescing. Each event changes the
/// account and, now and then, adds, changes or removes one of its relationships or contacts (a removed child is a
/// delete for the sync); a child can name an account that isn't synced yet. Every record is built on your event
/// template (<see cref="AggregateFactory"/>), serialized from the sync's own event classes, full records every time.
/// </summary>
public sealed class AccountEventProducer(
    IProducer<string, string> producer,
    IOptions<LoadGeneratorOptions> options,
    ILogger<AccountEventProducer> log) : BackgroundService
{
    // Read once at startup: a missing or nonconforming template throws here, before anything is produced
    private readonly AggregateFactory _factory = new(AggregateTemplate.Load(options.Value.TemplatePath), seed: 42);

    // Account and Profile ids are GUIDs, as in the source system. From their own seeded sequence, so every run
    // produces the same accounts (a rerun updates them instead of creating new ones)
    private readonly string[] _accountIds = Guids(options.Value.Accounts, seed: 1);
    private readonly string[] _profileIds = Guids(options.Value.Accounts, seed: 2);

    private long _lastTimeTicks;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var o = options.Value;
        var versions = new long[o.Accounts];
        var snapshots = new AccountSnapshot?[o.Accounts]; // each account's last "after", the next event's "before"
        var childVersions = new Dictionary<string, long>(StringComparer.Ordinal);
        var random = new Random(42);
        var interval = TimeSpan.FromSeconds(1.0 / Math.Max(1, o.EventsPerSecond));
        var sent = 0L;
        var sw = Stopwatch.StartNew();

        log.LogInformation("Producing to {Topic}: {Accounts} accounts at {Rate}/s (total {Total}), built on the template {Template}",
            o.Topic, o.Accounts, o.EventsPerSecond, o.TotalEvents == 0 ? "unbounded" : o.TotalEvents, o.TemplatePath);

        while (!stoppingToken.IsCancellationRequested && (o.TotalEvents == 0 || sent < o.TotalEvents))
        {
            var account = random.Next(o.Accounts);
            var burst = random.NextDouble() < o.BurstProbability ? random.Next(2, 6) : 1;

            for (var i = 0; i < burst && (o.TotalEvents == 0 || sent < o.TotalEvents); i++)
            {
                var before = snapshots[account];
                var after = Next(account, ++versions[account], before, childVersions, o, random);
                snapshots[account] = after;

                var aggregate = new AccountAggregateEvent(Meta(before is null ? EventType.Inserted : EventType.Updated), before, after);
                Produce(o.Topic, _accountIds[account], JsonSerializer.Serialize(aggregate, EventJson.Options));
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

    /// <summary>The account's next snapshot: the account changed, its children as they were, maybe one of them touched.</summary>
    private AccountSnapshot Next(int account, long version, AccountSnapshot? before, Dictionary<string, long> childVersions,
        LoadGeneratorOptions o, Random random)
    {
        var relationships = new List<AccountRelationshipPayload?>(before is null ? [] : AggregateShape.Relationships.Get(before) ?? []);
        var contacts = new List<ContactPayload?>(before is null ? [] : AggregateShape.Contacts.Get(before) ?? []);

        if (o.Accounts > 1 && random.NextDouble() < o.RelationshipProbability)
        {
            // Owned by this account's aggregate: its key names this account first
            var other = (account + 1 + random.Next(o.Accounts - 1)) % o.Accounts;
            var id = $"REL-{account:D6}-{other:D6}";
            Touch(relationships, AggregateShape.RelationshipId, id, o, random,
                () => _factory.Relationship(id, _accountIds[other], NextVersion(childVersions, id)));
        }

        if (random.NextDouble() < o.ContactProbability)
        {
            var id = $"CON-{account:D6}-{random.Next(3)}";
            Touch(contacts, AggregateShape.ContactId, id, o, random, () => _factory.Contact(id, NextVersion(childVersions, id)));
        }

        return _factory.Account(_accountIds[account], _profileIds[account], version, relationships, contacts);
    }

    /// <summary>Adds the child, or, if the list has it, removes it (<see cref="LoadGeneratorOptions.ChildRemovalProbability"/>) or replaces it.</summary>
    private static void Touch<T>(List<T?> children, Field<T, string?> idOf, string id, LoadGeneratorOptions o, Random random, Func<T> build)
        where T : class
    {
        var existing = children.FindIndex(c => c is not null && idOf.Get(c) == id);
        if (existing >= 0)
        {
            children.RemoveAt(existing);
            if (random.NextDouble() < o.ChildRemovalProbability)
            {
                return;
            }
        }

        children.Add(build());
    }

    private static long NextVersion(Dictionary<string, long> versions, string id)
    {
        return versions[id] = versions.GetValueOrDefault(id) + 1;
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
    private EventMeta Meta(EventType kind)
    {
        var ticks = Math.Max(DateTimeOffset.UtcNow.Ticks, _lastTimeTicks + TimeSpan.TicksPerMicrosecond);
        ticks -= ticks % TimeSpan.TicksPerMicrosecond;
        _lastTimeTicks = ticks;
        // An account's first event is its insert, every later one an update
        return EventMeta.Of(kind, new DateTimeOffset(ticks, TimeSpan.Zero));
    }
}
