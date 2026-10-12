using System.Diagnostics;
using System.Text.Json.Nodes;
using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace SalesforceSync.LoadGenerator;

/// <summary>
/// Publishes account change events keyed by account external id (so Kafka keeps per-account order),
/// including rapid bursts to the same account to exercise the ordering/coalescing logic.
/// </summary>
public sealed class AccountEventProducer(
    IProducer<string, string> producer,
    IOptions<LoadGeneratorOptions> options,
    ILogger<AccountEventProducer> log) : BackgroundService
{
    private static readonly string[] Ratings = ["Hot", "Warm", "Cold"];
    private static readonly string[] Tiers = ["Bronze", "Silver", "Gold", "Platinum"];

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
            }

            // Simple pacing: keep the average rate at EventsPerSecond
            var due = interval * sent - sw.Elapsed;
            if (due > TimeSpan.Zero) await Task.Delay(due, stoppingToken);

            if (sent % 5_000 == 0) log.LogInformation("Produced {Sent} events", sent);
        }

        producer.Flush(TimeSpan.FromSeconds(10));
        log.LogInformation("Done: produced {Sent} events in {Elapsed}", sent, sw.Elapsed);
    }

    private static JsonObject BuildEvent(string accountKey, long version, Random random)
    {
        // Partial updates: each event carries a subset of fields, like a real change feed.
        var account = new JsonObject { ["Name"] = $"Account {accountKey}" };
        if (random.NextDouble() < 0.7) account["Rating"] = Ratings[random.Next(Ratings.Length)];
        if (random.NextDouble() < 0.5) account["Description"] = $"Updated to v{version}";

        var profile = new JsonObject();
        if (random.NextDouble() < 0.8) profile["Tier__c"] = Tiers[random.Next(Tiers.Length)];
        if (random.NextDouble() < 0.6) profile["Score__c"] = random.Next(0, 101);

        return new JsonObject
        {
            ["accountExternalId"] = accountKey,
            ["profileExternalId"] = accountKey.Replace("ACC-", "PRF-"),
            ["version"] = version,
            ["account"] = account,
            ["profile"] = profile
        };
    }
}
