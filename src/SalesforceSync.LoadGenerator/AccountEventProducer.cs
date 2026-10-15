using System.Diagnostics;
using System.Globalization;
using System.Text.Json.Nodes;
using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace SalesforceSync.LoadGenerator;

/// <summary>
/// Publishes account change events keyed by account external id (so Kafka keeps per-account order),
/// including rapid bursts to the same account to exercise the ordering/coalescing logic, relationship events between
/// accounts (keyed by relationship id) and contact events of accounts (keyed by contact id), which can arrive before
/// their accounts are synced. Events use the real envelope:
/// <c>{ "meta": { "time": ... }, "account" | "accountRelationship" | "contact": { "externalId": ..., ... } }</c>,
/// with the source system's camelCase field names. Every event carries the full record.
/// </summary>
public sealed class AccountEventProducer(
    IProducer<string, string> producer,
    IOptions<LoadGeneratorOptions> options,
    ILogger<AccountEventProducer> log) : BackgroundService
{
    private static readonly string[] Ratings = ["Hot", "Warm", "Cold"];
    private static readonly string[] Tiers = ["Bronze", "Silver", "Gold", "Platinum"];
    private static readonly string[] RelationshipTypes = ["Parent", "Subsidiary", "Partner", "Reseller"];

    private long _lastTimeTicks;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var o = options.Value;
        var versions = new long[o.Accounts];
        var relationships = new HashSet<string>(StringComparer.Ordinal);
        var contacts = new HashSet<string>(StringComparer.Ordinal);
        var random = new Random(42);
        var interval = TimeSpan.FromSeconds(1.0 / Math.Max(1, o.EventsPerSecond));
        var sent = 0L;
        var sw = Stopwatch.StartNew();

        log.LogInformation("Producing to {Topic}: {Accounts} accounts at {Rate}/s (total {Total})",
            o.Topic, o.Accounts, o.EventsPerSecond, o.TotalEvents == 0 ? "unbounded" : o.TotalEvents);

        while (!stoppingToken.IsCancellationRequested && (o.TotalEvents == 0 || sent < o.TotalEvents))
        {
            var account = random.Next(o.Accounts);
            var burst = random.NextDouble() < o.BurstProbability ? random.Next(2, 6) : 1;

            for (var i = 0; i < burst && (o.TotalEvents == 0 || sent < o.TotalEvents); i++)
            {
                var version = ++versions[account];
                var key = $"ACC-{account:D6}";
                var value = BuildEvent(key, version, random).ToJsonString();

                Produce(o.Topic, key, value);
                sent++;

                if (o.Accounts > 1 && random.NextDouble() < o.RelationshipProbability)
                {
                    var other = (account + 1 + random.Next(o.Accounts - 1)) % o.Accounts;
                    var (a, b) = (Math.Min(account, other), Math.Max(account, other));
                    var relKey = $"REL-{a:D6}-{b:D6}";
                    var relationship = new JsonObject
                    {
                        ["meta"] = Meta(inserted: relationships.Add(relKey)),
                        ["accountRelationship"] = new JsonObject
                        {
                            ["externalId"] = relKey,
                            ["accountExternalId"] = $"ACC-{a:D6}",
                            ["relatedAccountExternalId"] = $"ACC-{b:D6}",
                            ["type"] = RelationshipTypes[random.Next(RelationshipTypes.Length)],
                            ["isActive"] = random.NextDouble() < 0.9
                        }
                    };
                    Produce(o.RelationshipTopic, relKey, relationship.ToJsonString());
                }

                if (random.NextDouble() < o.ContactProbability)
                {
                    var contactKey = $"CON-{account:D6}-{random.Next(3)}";
                    var contact = new JsonObject
                    {
                        ["meta"] = Meta(inserted: contacts.Add(contactKey)),
                        ["contact"] = new JsonObject
                        {
                            ["externalId"] = contactKey,
                            ["accountExternalId"] = key,
                            ["firstName"] = "Contact",
                            ["lastName"] = contactKey,
                            ["email"] = $"{contactKey.ToLowerInvariant()}@example.com",
                            ["phone"] = $"+1 555 {random.Next(10_000):D4}"
                        }
                    };
                    Produce(o.ContactTopic, contactKey, contact.ToJsonString());
                }
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

    private JsonObject BuildEvent(string accountKey, long version, Random random)
    {
        // Full records, like the source system sends them: every event carries every field, and most of them
        // don't change from one event to the next (so the diff has something to skip).
        var account = new JsonObject
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
                ["externalId"] = accountKey.Replace("ACC-", "PRF-"),
                ["name"] = $"Profile {accountKey}",
                ["tier"] = Tiers[random.Next(Tiers.Length)],
                ["score"] = random.Next(0, 101),
                ["discountPercent"] = 12.5,
                ["notes"] = null,
                ["optIn"] = random.NextDouble() < 0.5,
                ["startDate"] = "2020-02-01"
            }
        };

        return new JsonObject { ["meta"] = Meta(inserted: version == 1), ["account"] = account };
    }

    /// <summary>
    /// meta.time: now, but always later than the previous event's (to the microsecond Postgres keeps), so events
    /// produced within the same clock tick still have a distinct order.
    /// </summary>
    private JsonObject Meta(bool inserted)
    {
        var ticks = Math.Max(DateTimeOffset.UtcNow.Ticks, _lastTimeTicks + TimeSpan.TicksPerMicrosecond);
        ticks -= ticks % TimeSpan.TicksPerMicrosecond;
        _lastTimeTicks = ticks;
        return new JsonObject
        {
            ["time"] = new DateTimeOffset(ticks, TimeSpan.Zero).ToString("yyyy-MM-dd'T'HH:mm:ss.ffffff'Z'", CultureInfo.InvariantCulture),
            // The first event of an entity is its insert, every later one an update
            ["type"] = inserted ? "inserted" : "updated"
        };
    }
}