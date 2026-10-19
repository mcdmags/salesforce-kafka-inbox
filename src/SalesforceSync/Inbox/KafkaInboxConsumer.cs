using Confluent.Kafka;
using Confluent.Kafka.Admin;
using SalesforceSync.Persistence;
using SalesforceSync.Sync;

namespace SalesforceSync.Inbox;

/// <summary>
/// Kafka -> Postgres inboxes. One topic (<c>Kafka:Topic</c>, <see cref="AccountAggregate.DefaultTopic"/>) carries
/// account aggregates: Account + children, before/after a change. Each message read once
/// (<see cref="AccountAggregate.TryRead"/>); every entity type takes its events (Account, relationships, contacts;
/// deletes too, <see cref="IAggregateIngest.IngestAsync"/>) and stores them in its inbox table by entity id, in the
/// entity's one row unless it holds a newer event. Kafka key and arrival order unused: events are ordered by own time
/// (the aggregate's), so any partition, any order. Offset stored for commit only after all message's events are in
/// inboxes. Store is idempotent → crash mid-message is safe: redelivery overwrites stored events with themselves
/// (no-op once synced), stores the rest first time. Message with no valid aggregate can never sync: logged with
/// offset, skipped. Same for an individually invalid entity event; others stored.
/// </summary>
public sealed class KafkaInboxConsumer(
    IConsumer<string, string> consumer,
    IServiceScopeFactory scopes,
    IConfiguration config,
    AccountAggregate aggregate,
    ILogger<KafkaInboxConsumer> log) : BackgroundService
{
    private static readonly TimeSpan StoreRetryDelay = TimeSpan.FromSeconds(2);

    // Consume() blocks: run loop off host startup path
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        return Task.Run(() => RunAsync(stoppingToken), stoppingToken);
    }

    private async Task RunAsync(CancellationToken ct)
    {
        // Scope lives as long as consumer uses the ingests
        using var scope = scopes.CreateScope();
        var ingests = scope.ServiceProvider.GetServices<IAggregateIngest>().ToList();
        var topic = config["Kafka:Topic"] is { Length: > 0 } configured ? configured : AccountAggregate.DefaultTopic;

        await EnsureTopicAsync(topic);
        consumer.Subscribe(topic);
        log.LogInformation("Consuming {Topic} -> {Types}", topic, string.Join(", ", ingests.Select(s => s.EntityType)));

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

                await IngestAsync(message, ingests, ct);
            }
        }
        finally
        {
            consumer.Close();
        }
    }

    /// <returns>False on shutdown. True with null message after non-fatal consume error.</returns>
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
    /// Stores an aggregate's valid entity events in their inboxes, then its offset. Skips never-syncable.
    /// </summary>
    private async Task IngestAsync(ConsumeResult<string, string> message, IReadOnlyList<IAggregateIngest> ingests,
        CancellationToken ct)
    {
        // Retry can't fix invalid event, would block later partition events. Log location (fix/replay at source), move on
        if (!aggregate.TryRead(message.Message.Value, out var change, out var problem))
        {
            log.LogError("Skipping malformed account aggregate at {Offset}: {Problem}", message.TopicPartitionOffset, problem);
            StoreOffset(message);
            return;
        }

        try
        {
            foreach (var ingest in ingests)
            {
                foreach (var invalid in await ingest.IngestAsync(change, ct))
                {
                    log.LogError("Skipping malformed {Type} event of the aggregate at {Offset}: {Problem}", ingest.EntityType,
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
    /// Marks message handled; client commits stored offsets in background (EnableAutoOffsetStore=false with auto
    /// commit, set in AddKafkaIngest). Partition revoked by rebalance since consume → store fails. Harmless: new owner
    /// re-reads from last commit; re-stored event is a no-op once synced.
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

    /// <summary>Consume message again next. Impossible (and unneeded) if partition reassigned.</summary>
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