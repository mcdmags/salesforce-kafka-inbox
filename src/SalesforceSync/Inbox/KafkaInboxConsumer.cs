using System.Text.Json.Nodes;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Dapper;
using Npgsql;

namespace SalesforceSync.Inbox;

/// <summary>
/// Kafka -> Postgres inbox. Message key = account external id; value = raw JSON event
/// ({ "version": 123, "profileExternalId": "...", "account": {...}, "profile": {...} }).
/// The offset is committed only after the row is stored, and the unique (account, version)
/// constraint makes redelivery harmless.
/// </summary>
public sealed class KafkaInboxConsumer(
    IConsumer<string, string> consumer,
    NpgsqlDataSource db,
    IConfiguration config,
    ILogger<KafkaInboxConsumer> log) : BackgroundService
{
    private const string InsertSql = """
        insert into sf_inbox (account_ext_id, version, payload)
        values (@acct, @version, @payload::jsonb)
        on conflict (account_ext_id, version) do nothing
        """;

    // Consume() blocks, so run the loop off the host's startup path.
    protected override Task ExecuteAsync(CancellationToken stoppingToken) => Task.Run(() => RunAsync(stoppingToken), stoppingToken);

    private async Task RunAsync(CancellationToken ct)
    {
        var topic = config["Kafka:Topic"] ?? "account-changes";
        await EnsureTopicAsync(topic);
        consumer.Subscribe(topic);
        log.LogInformation("Consuming {Topic} into sf_inbox", topic);

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
        var json = JsonNode.Parse(result.Message.Value)?.AsObject()
                   ?? throw new InvalidOperationException("Event value is not a JSON object");

        var acct = result.Message.Key ?? json["accountExternalId"]?.GetValue<string>()
                   ?? throw new InvalidOperationException("Event has no account key");

        // Prefer the source system's version; fall back to the offset (only valid when keyed by account).
        var version = json["version"] is JsonValue v ? v.GetValue<long>() : result.Offset.Value;

        await using var conn = await db.OpenConnectionAsync(ct);
        await conn.ExecuteAsync(new CommandDefinition(InsertSql,
            new { acct, version, payload = result.Message.Value }, cancellationToken: ct));
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
