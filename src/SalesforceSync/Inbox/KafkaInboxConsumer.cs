using Confluent.Kafka;
using Confluent.Kafka.Admin;
using SalesforceSync.Sync;

namespace SalesforceSync.Inbox;

/// <summary>
/// Kafka -> Postgres inboxes. One topic (<c>Kafka:Topic</c>, <see cref="AccountAggregate.DefaultTopic"/>) carries
/// account aggregates: an Account with its children, before and after a change. Each message is split
/// (<see cref="AccountAggregate.TrySplit"/>) into events of the entities that changed (Account, relationships,
/// contacts; deletes included). Each event is read with its type's typed event and stored in that type's inbox table
/// with the entity's id, in the entity's one row unless that holds a newer event. The Kafka message key and arrival
/// order are not used: an entity's events are ordered by their own time (the aggregate's), so they may come over
/// any partition, in any order. The offset is stored for commit only after every event of the message is in its
/// inbox. A store is idempotent, so a crash halfway through a message is safe: redelivery overwrites the stored
/// events with themselves (or changes nothing once synced) and stores the rest for the first time. A message with
/// no valid aggregate can never be synced: it is logged with its offset and skipped. So is an entity event that is
/// invalid on its own; the others are stored.
/// </summary>
public sealed class KafkaInboxConsumer(
    IConsumer<string, string> consumer,
    IServiceScopeFactory scopes,
    IConfiguration config,
    ILogger<KafkaInboxConsumer> log) : BackgroundService
{
    private static readonly TimeSpan StoreRetryDelay = TimeSpan.FromSeconds(2);

    // Consume() blocks, so run the loop off the host's startup path.
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        return Task.Run(() => RunAsync(stoppingToken), stoppingToken);
    }

    private async Task RunAsync(CancellationToken ct)
    {
        // The syncs are only asked to read and store events here; the scope lives as long as the consumer uses them
        using var scope = scopes.CreateScope();
        var byType = scope.ServiceProvider.GetServices<ISalesforceEntitySync>().ToDictionary(s => s.EntityType, StringComparer.Ordinal);
        var topic = config["Kafka:Topic"] is { Length: > 0 } configured ? configured : AccountAggregate.DefaultTopic;

        await EnsureTopicAsync(topic);
        consumer.Subscribe(topic);
        log.LogInformation("Consuming {Topic} -> {Types}", topic, string.Join(", ", byType.Keys));

        try
        {
            while (!ct.IsCancellationRequested)
            {
                if (!TryConsume(ct, out var message))
                {
                    break;
                }

                if (message?.Message?.Value is null)
                {
                    continue;
                }

                await IngestAsync(message, byType, ct);
            }
        }
        finally
        {
            consumer.Close();
        }
    }

    /// <returns>False when the consumer is shutting down. True with a null message after a consume error that isn't fatal.</returns>
    private bool TryConsume(CancellationToken ct, out ConsumeResult<string, string>? message)
    {
        message = null;
        try
        {
            message = consumer.Consume(ct);
            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
        catch (ConsumeException ex) when (!ex.Error.IsFatal)
        {
            log.LogWarning(ex, "Kafka consume error");
            return true;
        }
    }

    /// <summary>
    /// Stores the valid entity events of an aggregate in their types' inboxes, then its offset. Skips what can never
    /// be synced.
    /// </summary>
    private async Task IngestAsync(ConsumeResult<string, string> message, IReadOnlyDictionary<string, ISalesforceEntitySync> byType,
        CancellationToken ct)
    {
        // Retrying can't fix an invalid event and would block every later event on the partition. Log where it is
        // (to fix or replay at the source) and move on
        if (!AccountAggregate.TrySplit(message.Message.Value, out var events, out var problem))
        {
            log.LogError("Skipping malformed account aggregate at {Offset}: {Problem}", message.TopicPartitionOffset, problem);
            StoreOffset(message);
            return;
        }

        try
        {
            foreach (var e in events)
            {
                var sync = byType[e.EntityType];
                if (sync.TryReadEvent(e.Json, out var entityId, out var eventTime, out var invalid))
                {
                    await sync.StoreEventAsync(entityId!, eventTime, e.Json, ct);
                }
                else
                {
                    log.LogError("Skipping malformed {Type} event of the aggregate at {Offset}: {Problem}", e.EntityType,
                        message.TopicPartitionOffset, invalid);
                }
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            log.LogError(ex, "Failed to store {Offset}; will retry", message.TopicPartitionOffset);
            Rewind(message);
            await Task.Delay(StoreRetryDelay, ct);
            return;
        }
        StoreOffset(message);
    }

    /// <summary>
    /// Marks the message as handled; the client commits stored offsets in the background (EnableAutoOffsetStore=false
    /// with auto commit, set in AddKafkaIngest). If a rebalance revoked the partition since the message was consumed,
    /// the store fails. Harmless: the new owner re-reads from the last commit, and a re-stored event changes
    /// nothing once synced.
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

    /// <summary>Consumes the message again next. Impossible, and not needed, if the partition was reassigned.</summary>
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
        if (string.IsNullOrWhiteSpace(bootstrap))
        {
            return;
        }

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