using System.Diagnostics;
using System.Globalization;
using System.Text.Json.Nodes;
using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace SalesforceSync.LoadGenerator;

/// <summary>
/// Publishes account change events keyed by account external id (so Kafka keeps per-account order),
/// including rapid bursts to the same account to exercise the ordering/coalescing logic, and relationship
/// events between accounts (keyed by relationship id), which can arrive before their accounts are synced.
/// Events use the real envelope: <c>{ "meta": { "time": ... }, "account" | "accountRelationship": { "externalId": ..., ... } }</c>.
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

                producer.Produce(o.Topic, new Message<string, string> { Key = key, Value = value }, report =>
                {
                    if (report.Error.IsError) log.LogWarning("Produce failed: {Reason}", report.Error.Reason);
                });
                sent++;

                if (o.Accounts > 1 && random.NextDouble() < o.RelationshipProbability)
                {
                    var other = (account + 1 + random.Next(o.Accounts - 1)) % o.Accounts;
                    var (a, b) = (Math.Min(account, other), Math.Max(account, other));
                    var relKey = $"REL-{a:D6}-{b:D6}";
                    var relationship = new JsonObject
                    {
                        ["meta"] = Meta(),
                        ["accountRelationship"] = new JsonObject
                        {
                            ["externalId"] = relKey,
                            ["accountExternalId"] = $"ACC-{a:D6}",
                            ["relatedAccountExternalId"] = $"ACC-{b:D6}",
                            ["Type__c"] = RelationshipTypes[random.Next(RelationshipTypes.Length)],
                            ["Active__c"] = random.NextDouble() < 0.9
                        }
                    };
                    producer.Produce(o.RelationshipTopic,
                        new Message<string, string> { Key = relKey, Value = relationship.ToJsonString() }, report =>
                        {
                            if (report.Error.IsError) log.LogWarning("Produce failed: {Reason}", report.Error.Reason);
                        });
                }
            }

            // Simple pacing: keep the average rate at EventsPerSecond
            var due = interval * sent - sw.Elapsed;
            if (due > TimeSpan.Zero) await Task.Delay(due, stoppingToken);

            if (sent % 5_000 == 0) log.LogInformation("Produced {Sent} events", sent);
        }

        producer.Flush(TimeSpan.FromSeconds(10));
        log.LogInformation("Done: produced {Sent} events in {Elapsed}", sent, sw.Elapsed);
    }

    private JsonObject BuildEvent(string accountKey, long version, Random random)
    {
        // Partial updates: each event carries a subset of fields, like a real change feed.
        var account = new JsonObject { ["externalId"] = accountKey, ["Name"] = $"Account {accountKey}" };
        if (random.NextDouble() < 0.7) account["Rating"] = Ratings[random.Next(Ratings.Length)];
        if (random.NextDouble() < 0.5) account["Description"] = $"Updated to v{version}";

        var profile = new JsonObject { ["externalId"] = accountKey.Replace("ACC-", "PRF-") };
        if (random.NextDouble() < 0.8) profile["Tier__c"] = Tiers[random.Next(Tiers.Length)];
        if (random.NextDouble() < 0.6) profile["Score__c"] = random.Next(0, 101);
        account["profile"] = profile;

        return new JsonObject { ["meta"] = Meta(), ["account"] = account };
    }

    /// <summary>
    /// meta.time: now, but always later than the previous event's (to the microsecond Postgres keeps), so events
    /// produced within the same clock tick still have a distinct order.
    /// </summary>
    private JsonObject Meta()
    {
        var ticks = Math.Max(DateTimeOffset.UtcNow.Ticks, _lastTimeTicks + TimeSpan.TicksPerMicrosecond);
        ticks -= ticks % TimeSpan.TicksPerMicrosecond;
        _lastTimeTicks = ticks;
        return new JsonObject
        {
            ["time"] = new DateTimeOffset(ticks, TimeSpan.Zero).ToString("yyyy-MM-dd'T'HH:mm:ss.ffffff'Z'", CultureInfo.InvariantCulture)
        };
    }
}
