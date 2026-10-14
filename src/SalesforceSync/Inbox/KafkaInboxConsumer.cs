using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Dapper;
using SalesforceSync.Data;
using SalesforceSync.Sync;

namespace SalesforceSync.Inbox;

/// <summary>
/// Kafka -> Postgres inbox. The message value is stored unchanged as sf_inbox.payload; the entity type, key and
/// order are read from it (<see cref="InboxEventEnvelope"/>), so any subscribed topic (Kafka:Topics) may carry any
/// entity type, and the message key is not used. The offset is stored for commit only after the row is in the inbox,
/// and the unique (type, key, version, md5) index makes redelivery (and a retried insert) harmless. A message that
/// can never be synced (not a valid event, or a type nothing syncs) is logged and skipped.
/// </summary>
public sealed class KafkaInboxConsumer(
    IConsumer<string, string> consumer,
    PostgresExecutionStrategy pg,
    IServiceScopeFactory scopes,
    IConfiguration config,
    ILogger<KafkaInboxConsumer> log) : BackgroundService
{
    /// <summary>Topics to subscribe to, unless configured under Kafka:Topics.</summary>
    public static readonly IReadOnlyList<string> DefaultTopics = ["account-changes", "account-relationship-changes"];

    private static readonly TimeSpan StoreRetryDelay = TimeSpan.FromSeconds(2);

    private HashSet<string> _entityTypes = new(StringComparer.Ordinal);

    // Consume() blocks, so run the loop off the host's startup path.
    protected override Task ExecuteAsync(CancellationToken stoppingToken) => Task.Run(() => RunAsync(stoppingToken), stoppingToken);

    private async Task RunAsync(CancellationToken ct)
    {
        // Only events some sync handles are stored: anything else would sit in the inbox forever
        using (var scope = scopes.CreateScope())
            _entityTypes = scope.ServiceProvider.GetServices<ISalesforceEntitySync>().Select(s => s.EntityType).ToHashSet(StringComparer.Ordinal);

        var configured = config.GetSection("Kafka:Topics").Get<string[]>();
        var topics = configured is { Length: > 0 } ? configured : DefaultTopics;
        foreach (var topic in topics) await EnsureTopicAsync(topic);
        consumer.Subscribe(topics);
        log.LogInformation("Consuming {Topics} into sf_inbox (entity types {Types})", string.Join(", ", topics), string.Join(", ", _entityTypes));

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

                if (!InboxEventEnvelope.TryRead(result.Message.Value, _entityTypes, out _, out var problem))
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
                        new { payload = result.Message.Value }, cancellationToken: t)), ct);
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
