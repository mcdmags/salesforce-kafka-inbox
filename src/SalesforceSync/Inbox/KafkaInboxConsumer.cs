using System.Text.Json.Nodes;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Dapper;
using SalesforceSync.Data;

namespace SalesforceSync.Inbox;

/// <summary>
/// Kafka -> Postgres inbox. Each topic carries one entity type (Kafka:Topics, e.g. account-changes -> account);
/// message key = the entity's key (external id), value = the raw JSON event with a "version".
/// The offset is committed only after the row is stored, and the unique (entity type, key, version)
/// constraint makes redelivery (and a retried insert) harmless.
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
                catch (ConsumeException ex)
                {
                    log.LogWarning(ex, "Kafka consume error");
                    continue;
                }

                if (result?.Message?.Value is null) continue;

                try
                {
                    await StoreAsync(result, ct);
                    consumer.Commit(result);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    log.LogError(ex, "Failed to store {Offset}; will retry", result.TopicPartitionOffset);
                    consumer.Seek(result.TopicPartitionOffset);
                    await Task.Delay(TimeSpan.FromSeconds(2), ct);
                }
            }
        }
        finally
        {
            consumer.Close();
        }
    }

    private async Task StoreAsync(ConsumeResult<string, string> result, CancellationToken ct)
    {
        var entityType = _topics[result.Topic];
        var json = JsonNode.Parse(result.Message.Value)?.AsObject()
                   ?? throw new InvalidOperationException("Event value is not a JSON object");

        var key = result.Message.Key ?? json["key"]?.GetValue<string>() ?? json["accountExternalId"]?.GetValue<string>()
                  ?? throw new InvalidOperationException("Event has no entity key");

        // Prefer the source system's version; fall back to the offset (only valid when keyed by entity).
        var version = json["version"] is JsonValue v ? v.GetValue<long>() : result.Offset.Value;

        await pg.ExecuteAsync((conn, t) => conn.ExecuteAsync(new CommandDefinition(InboxSql.Insert,
            new { entityType, key, version, payload = result.Message.Value }, cancellationToken: t)), ct);
    }

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
