using Confluent.Kafka;
using Confluent.Kafka.Admin;
using SalesforceSync.Sync;

namespace SalesforceSync.Inbox;

/// <summary>
/// Kafka -> Postgres inbox. Each topic carries one entity type (<see cref="ISalesforceEntitySync.Topic"/>, or
/// <c>Kafka:Topics:&lt;EntityType&gt;</c>). A message is read with that type's typed event, and its value is stored
/// unchanged in the type's inbox table together with the entity's id taken from the event. The Kafka message key and
/// the order of arrival are not used: an entity's events are ordered by their own time when they are processed, so
/// they may come over any partition, in any order. The offset is stored for commit only after the row is in the
/// inbox; a redelivery (or a retried insert) just stores the event again, which ends superseded. A message that can
/// never be synced (not a valid event of its topic's type) is logged with its offset and skipped.
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

    /// <summary>Topic -> the entity type whose events it carries. Two types can't share a topic: a message says nothing but its topic about its type.</summary>
    public static IReadOnlyDictionary<string, ISalesforceEntitySync> TopicsOf(IEnumerable<ISalesforceEntitySync> syncs, IConfiguration config)
    {
        var byTopic = new Dictionary<string, ISalesforceEntitySync>(StringComparer.Ordinal);
        foreach (var sync in syncs)
        {
            var topic = config[$"Kafka:Topics:{sync.EntityType}"] is { Length: > 0 } configured ? configured : sync.Topic;
            if (!byTopic.TryAdd(topic, sync))
            {
                throw new InvalidOperationException(
                    $"Topic '{topic}' is configured for both '{byTopic[topic].EntityType}' and '{sync.EntityType}'; each entity type needs its own topic.");
            }
        }
        return byTopic;
    }

    private async Task RunAsync(CancellationToken ct)
    {
        // The syncs are only asked to read events here; the scope lives as long as the consumer uses them
        using var scope = scopes.CreateScope();
        var byTopic = TopicsOf(scope.ServiceProvider.GetServices<ISalesforceEntitySync>(), config);

        foreach (var topic in byTopic.Keys)
        {
            await EnsureTopicAsync(topic);
        }

        consumer.Subscribe(byTopic.Keys);
        log.LogInformation("Consuming {Topics}", string.Join(", ", byTopic.Select(t => $"{t.Key} -> {t.Value.EntityType}")));

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

                await IngestAsync(message, byTopic[message.Topic], ct);
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

    /// <summary>Stores a valid event in its type's inbox, then its offset; skips a message that can never be synced.</summary>
    private async Task IngestAsync(ConsumeResult<string, string> message, ISalesforceEntitySync sync, CancellationToken ct)
    {
        if (!sync.TryReadEvent(message.Message.Value, out var entityId, out var problem))
        {
            // Retrying can't fix it, and retrying would block every later event on the partition: log where
            // it is (to fix or replay at the source) and move on.
            log.LogError("Skipping malformed {Type} event at {Offset}: {Problem}", sync.EntityType, message.TopicPartitionOffset, problem);
            StoreOffset(message);
            return;
        }

        try
        {
            await sync.StoreEventAsync(entityId!, message.Message.Value, ct);
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
    /// + auto commit, see Program.cs). If a rebalance revoked the partition since the message was consumed, the store
    /// fails: harmless, its new owner re-reads from the last commit and the event stored twice ends superseded.
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