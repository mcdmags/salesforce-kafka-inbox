using System.Text.Json;
using System.Text.Json.Nodes;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Dapper;
using SalesforceSync.Data;

namespace SalesforceSync.Inbox;

/// <summary>
/// Kafka -> Postgres inbox. Each topic carries one entity type (Kafka:Topics, e.g. account-changes -> account);
/// message key = the entity's key (external id), value = the raw JSON event with a "version".
/// The offset is stored for commit only after the row is in the inbox, and the unique (entity type, key, version)
/// constraint makes redelivery (and a retried insert) harmless. A message that can't be parsed is logged and skipped.
/// </summary>
public sealed class KafkaInboxConsumer(
    IConsumer<string, string> consumer,
    PostgresExecutionStrategy pg,
    IConfiguration config,
    ILogger<KafkaInboxConsumer> log) : BackgroundService
{
    /// <summary>Topic -> inbox entity type, unless configured under Kafka:Topics.</summary>
    public static readonly IReadOnlyDictionary<string, string> DefaultTopics = new Dictionary<string, string>
    {
        ["account-changes"] = "account",
        ["account-relationship-changes"] = "account-relationship"
    };

    private static readonly TimeSpan StoreRetryDelay = TimeSpan.FromSeconds(2);

    private Dictionary<string, string> _topics = new(StringComparer.Ordinal);

    // Consume() blocks, so run the loop off the host's startup path.
    protected override Task ExecuteAsync(CancellationToken stoppingToken) => Task.Run(() => RunAsync(stoppingToken), stoppingToken);

    private async Task RunAsync(CancellationToken ct)
    {
        var configured = config.GetSection("Kafka:Topics").Get<Dictionary<string, string>>();
        _topics = new Dictionary<string, string>(configured is { Count: > 0 } ? configured : DefaultTopics, StringComparer.Ordinal);
        foreach (var topic in _topics.Keys) await EnsureTopicAsync(topic);
        consumer.Subscribe(_topics.Keys);
        log.LogInformation("Consuming {Topics} into sf_inbox", string.Join(", ", _topics.Select(t => $"{t.Key} ({t.Value})")));

        try
        {
            while (!ct.IsCancellationRequested)
            {
                ConsumeResult<string, string>? result;
                try
                {
                    result = consumer.Consume(ct);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (ConsumeException ex) when (!ex.Error.IsFatal)
                {
                    log.LogWarning(ex, "Kafka consume error");
                    continue;
                }

                if (result?.Message?.Value is null) continue;

                if (!TryReadEvent(result, out var row, out var problem))
                {
                    // Retrying can't fix it, and retrying would block every later event on the partition: log where
                    // it is (to fix or replay at the source) and move on.
                    log.LogError("Skipping malformed event at {Offset} (key {Key}): {Problem}",
                        result.TopicPartitionOffset, result.Message.Key, problem);
                    StoreOffset(result);
                    continue;
                }

                try
                {
                    await pg.ExecuteAsync((conn, t) => conn.ExecuteAsync(new CommandDefinition(InboxSql.Insert,
                        new { entityType = row.EntityType, key = row.Key, version = row.Version, payload = row.Payload },
                        cancellationToken: t)), ct);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    log.LogError(ex, "Failed to store {Offset}; will retry", result.TopicPartitionOffset);
                    Rewind(result);
                    await Task.Delay(StoreRetryDelay, ct);
                    continue;
                }
                StoreOffset(result);
            }
        }
        finally
        {
            consumer.Close();
        }
    }

    /// <summary>
    /// Marks the message as handled; the client commits stored offsets in the background (EnableAutoOffsetStore=false
    /// + auto commit, see Program.cs). If a rebalance revoked the partition since the message was consumed, the store
    /// fails: harmless, its new owner re-reads from the last commit and the duplicate insert is ignored.
    /// </summary>
    private void StoreOffset(ConsumeResult<string, string> result)
    {
        try
        {
            consumer.StoreOffset(result);
        }
        catch (KafkaException ex)
        {
            log.LogInformation("Offset {Offset} not stored ({Reason}); the partition was likely reassigned",
                result.TopicPartitionOffset, ex.Error.Reason);
        }
    }

    /// <summary>Consumes the message again next. Not possible if the partition was reassigned, and not needed then.</summary>
    private void Rewind(ConsumeResult<string, string> result)
    {
        try
        {
            consumer.Seek(result.TopicPartitionOffset);
        }
        catch (KafkaException ex)
        {
            log.LogInformation("Can't rewind to {Offset} ({Reason}); the partition was likely reassigned",
                result.TopicPartitionOffset, ex.Error.Reason);
        }
    }

    /// <summary>The inbox row for a message, or why the message can never become one.</summary>
    private bool TryReadEvent(ConsumeResult<string, string> result, out InboxRow row, out string? problem)
    {
        row = default!;
        try
        {
            if (JsonNode.Parse(result.Message.Value) is not JsonObject json)
            {
                problem = "Event value is not a JSON object";
                return false;
            }

            var key = result.Message.Key ?? json["key"]?.GetValue<string>() ?? json["accountExternalId"]?.GetValue<string>();
            if (string.IsNullOrEmpty(key))
            {
                problem = "Event has no entity key";
                return false;
            }

            // Prefer the source system's version; fall back to the offset (only valid when keyed by entity).
            var version = json["version"] is JsonValue v ? v.GetValue<long>() : result.Offset.Value;

            row = new InboxRow(_topics[result.Topic], key, version, result.Message.Value);
            problem = null;
            return true;
        }
        catch (Exception ex) when (ex is JsonException or InvalidOperationException or FormatException)
        {
            // Not JSON, or "key" / "version" of the wrong JSON type
            problem = ex.Message;
            return false;
        }
    }

    private sealed record InboxRow(string EntityType, string Key, long Version, string Payload);

    private async Task EnsureTopicAsync(string topic)
    {
        var bootstrap = config.GetConnectionString("kafka");
        if (string.IsNullOrWhiteSpace(bootstrap)) return;

        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build();
        try
        {
            await admin.CreateTopicsAsync(
            [
                new TopicSpecification
                {
                    Name = topic,
                    NumPartitions = config.GetValue("Kafka:Partitions", 6),
                    ReplicationFactor = 1
                }
            ]);
            log.LogInformation("Created topic {Topic}", topic);
        }
        catch (CreateTopicsException ex) when (ex.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
        {
            // already there
        }
    }
}
