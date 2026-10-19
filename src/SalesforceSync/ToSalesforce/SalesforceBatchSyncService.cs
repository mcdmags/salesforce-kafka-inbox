using Microsoft.Extensions.Options;
using SalesforceSync.Data.Inbox;
using SalesforceSync.Infrastructure.RateLimiting;
using SalesforceSync.Infrastructure.Salesforce;
using SalesforceSync.Sync;

namespace SalesforceSync.ToSalesforce;

/// <summary>What went wrong with one record of an entity, and whether retrying the same data can help.</summary>
public sealed record Failure(string Error, bool Permanent);

/// <summary>
/// What a type does with a batch's valid events: send them to Salesforce, however it likes (<see cref="SalesforceCalls"/>
/// has the calls it is usually made of).
/// </summary>
/// <returns>The entities that failed, by entity id, with why. An entity not in it is done.</returns>
public delegate Task<Dictionary<string, List<Failure>>> SendStep<TEvent>(IReadOnlyList<ReadEvent<TEvent>> events, CancellationToken ct);

/// <summary>
/// The flow of one batch of one entity type, from the inbox to Salesforce and back; the same for every type. Only the
/// flow is here. What happens with the events (how they map, what is read, written and how answers are read) is the
/// type's own, passed in as the <see cref="SendStep{TEvent}"/>.
/// <list type="number">
/// <item>Nothing starts while this process is paused (Salesforce asked for it).</item>
/// <item><b>Claim</b> up to <see cref="SyncOptions.BatchSize"/> due entities from the inbox table: leased to this
/// worker, so no other worker (in any pod) takes them until the lease ends. One row per entity: its newest event.</item>
/// <item><b>Parse</b> each row's event (<see cref="EventReader{TEvent}"/>). A row that can't be read is dead.</item>
/// <item><b>Send</b>: the type's step. It runs outside any database transaction (Salesforce can't join one) and is
/// given up on before the lease runs out, so another worker never takes an entity while this one can still write it.</item>
/// <item><b>Record</b> each entity's outcome in the inbox: done, retried later, or dead. If the whole step fails, every
/// claimed entity is held (the write's outcome is unknown), deferred (rate limited), stopped (a mapping error) or retried.</item>
/// </list>
/// </summary>
public sealed class SalesforceBatchSyncService(ISalesforceRateLimiter limiter, IOptions<SyncOptions> options, ILogger<SalesforceBatchSyncService> log)
{
    /// <summary>
    /// Share of the lease a batch may spend before its Salesforce call is cancelled. The rest is margin for recording
    /// the outcome. A write cancelled this way has an unknown outcome and puts its entities on hold.
    /// </summary>
    private const double LeaseUsableFraction = 0.8;

    /// <summary>Recording a hold must not be cut short by the cancellation that caused it (shutdown), and must not hang it.</summary>
    private static readonly TimeSpan HoldRecordingTimeout = TimeSpan.FromSeconds(10);

    private readonly SyncOptions _opt = options.Value;

    /// <param name="reader">Reads and validates the type's events.</param>
    /// <param name="inbox">The type's inbox table.</param>
    /// <param name="send">What the type does with the valid events.</param>
    /// <param name="ct">Shutdown.</param>
    /// <returns>Inbox rows processed (0 = nothing to do, or no capacity right now).</returns>
    public async Task<int> SyncBatchAsync<TEvent, TRow>(EventReader<TEvent> reader, IRepository<TRow> inbox, SendStep<TEvent> send,
        CancellationToken ct)
        where TRow : InboxRow, IInboxRow, new()
    {
        if (limiter.IsPaused)
        {
            return 0;
        }

        // Started before the claim, so the margin is measured from when the lease began
        using var leaseCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        leaseCts.CancelAfter(_opt.Lease * LeaseUsableFraction);

        var claim = await inbox.ClaimAsync(ct);
        if (claim.Events.Count == 0)
        {
            return 0;
        }

        var (events, dead) = Parse(reader, claim.Events);

        Dictionary<string, List<Failure>> failures;
        try
        {
            failures = await send(events, leaseCts.Token);
        }
        catch (UnknownOutcomeException ex)
        {
            // Salesforce may still apply the write, so the entities aren't written again for a while. Caught on
            // shutdown too. If the hold can't be recorded (or the process is killed first), the lease still protects them
            log.LogWarning(ex, "{Type} batch: holding {Count} events for {Hold}", reader.EntityType, claim.Events.Count, _opt.UnknownOutcomeHold);
            using var recording = new CancellationTokenSource(HoldRecordingTimeout);
            await inbox.ReleaseAsync(claim, new Release(_opt.UnknownOutcomeHold, $"UNKNOWN_OUTCOME: {ex.Message}"), recording.Token);
            ct.ThrowIfCancellationRequested();
            return claim.Events.Count;
        }
        catch (MappingErrorException ex)
        {
            // The inbox stops (every due row waits) with the error on the rows to fix. Nothing is dead-lettered and the
            // rows go on after the deploy with no replay
            log.LogError(ex, "{Type} inbox stopped for {Hold} by {Count} mapping error(s), fix the mapping or add the field: {Errors}",
                reader.EntityType, _opt.MappingErrorHold, ex.Errors.Count,
                string.Join("; ", ex.Errors.Select(e => $"{e.Key}: {e.Error.Code} {e.Error.Message}")));
            await inbox.ReleaseAsync(claim, new Release(_opt.MappingErrorHold,
                $"MAPPING_ERROR: the {reader.EntityType} inbox is stopped by a mapping error ({ex.Message})",
                [.. ex.Errors.GroupBy(e => e.Key).Select(g => new FailedEvent(g.Key, string.Join("; ", g.Select(e => $"{e.Error.Code}: {e.Error.Message}"))))],
                StopInbox: true), ct);
            return claim.Events.Count;
        }
        catch (RateLimitedException ex)
        {
            log.LogInformation("Rate limited; deferring {Count} {Type} events for {Delay}", claim.Events.Count, reader.EntityType, ex.RetryAfter);
            await inbox.ReleaseAsync(claim, new Release(ex.RetryAfter), ct);
            return 0;
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            // A request failed that Salesforce provably did not apply (the read or a write): every entity is retried.
            // Records saved by earlier calls of the batch are safe: the retry reads them and writes only what differs
            log.LogError(ex, "{Type} batch failed", reader.EntityType);
            await inbox.FailAllAsync(claim, ex.Message, ct);
            return claim.Events.Count;
        }

        await inbox.RecordAsync(claim, Outcomes(reader.EntityType, events, dead, failures), ct);
        return claim.Events.Count;
    }

    /// <summary>
    /// Each claimed row's event, read. A row that can't be read, or is about another entity than its row (the lease is on
    /// the row's entity), is dead: retrying can't fix it.
    /// </summary>
    private static (List<ReadEvent<TEvent>> Events, List<FailedEvent> Dead) Parse<TEvent>(EventReader<TEvent> reader, IReadOnlyList<InboxEvent> rows)
    {
        List<ReadEvent<TEvent>> events = [];
        List<FailedEvent> dead = [];
        foreach (var row in rows)
        {
            if (!reader.TryRead(row.Payload, out var read, out var problem))
            {
                dead.Add(Invalid(reader, row, problem));
            }
            else if (!string.Equals(read.EntityId, row.EntityId, StringComparison.Ordinal))
            {
                dead.Add(Invalid(reader, row, $"the event is about '{read.EntityId}', but the row's entity_id is '{row.EntityId}'"));
            }
            else
            {
                events.Add(read);
            }
        }

        return (events, dead);
    }

    private static FailedEvent Invalid<TEvent>(EventReader<TEvent> reader, InboxEvent row, string why)
    {
        return new FailedEvent(row.EntityId, $"{SalesforceErrors.InvalidEvent}: {reader.EntityType} {row.EntityId}: {why}");
    }

    /// <summary>Done: no failure. Dead: a failure retrying can't fix, and the unreadable rows. Failed: the rest, retried with backoff.</summary>
    private BatchOutcomes Outcomes<TEvent>(string entityType, List<ReadEvent<TEvent>> events, List<FailedEvent> dead,
        Dictionary<string, List<Failure>> failures)
    {
        foreach (var invalid in dead)
        {
            log.LogWarning("Dead-lettered event {Id}: {Error}", invalid.EntityId, invalid.Error);
        }

        var done = new List<string>();
        var failed = new List<FailedEvent>();
        var allDead = new List<FailedEvent>(dead);
        foreach (var key in events.Select(e => e.EntityId))
        {
            if (!failures.TryGetValue(key, out var list))
            {
                done.Add(key);
                continue;
            }

            var failure = new FailedEvent(key, string.Join("; ", list.OrderByDescending(f => f.Permanent).Select(f => f.Error)));
            log.LogWarning("{Type} {Key}: {Error}", entityType, key, failure.Error);
            (list.Any(f => f.Permanent) ? allDead : failed).Add(failure);
        }

        return new BatchOutcomes(done, allDead, failed);
    }
}
