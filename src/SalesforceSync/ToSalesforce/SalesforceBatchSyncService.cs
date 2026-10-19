using Microsoft.Extensions.Options;
using SalesforceSync.Data.Inbox;
using SalesforceSync.Infrastructure.RateLimiting;
using SalesforceSync.Infrastructure.Salesforce;
using SalesforceSync.Sync;

namespace SalesforceSync.ToSalesforce;

/// <summary>One record's failure; Permanent = retrying same data can't help.</summary>
public sealed record Failure(string Error, bool Permanent);

/// <summary>
/// Type's handling of a batch's valid events: send to Salesforce however it likes (usual calls:
/// <see cref="SalesforceCalls"/>).
/// </summary>
/// <returns>Failed entities by entity id, with why. Not in it = done.</returns>
public delegate Task<Dictionary<string, List<Failure>>> SendStep<TEvent>(IReadOnlyList<ReadEvent<TEvent>> events, CancellationToken ct);

/// <summary>
/// Flow of one batch of one entity type, inbox → Salesforce → back; same for every type. Only flow here; event handling
/// (mapping, reads, writes, reading answers) is the type's, passed as <see cref="SendStep{TEvent}"/>.
/// <list type="number">
/// <item>Nothing starts while process is paused (Salesforce asked).</item>
/// <item><b>Claim</b> ≤ <see cref="SyncOptions.BatchSize"/> due entities from inbox table, leased to this worker: no
/// other worker (any pod) takes them until lease ends. One row per entity = its newest event.</item>
/// <item><b>Parse</b> each row's event (<see cref="EventReader{TEvent}"/>). Unreadable row = dead.</item>
/// <item><b>Send</b>: type's step. Outside any DB transaction (Salesforce can't join one); abandoned before lease ends,
/// so no other worker takes an entity while this one can still write it.</item>
/// <item><b>Record</b> each outcome in inbox: done, retried later, or dead. Whole step fails → all claimed entities
/// held (write outcome unknown), deferred (rate limited), stopped (mapping error) or retried.</item>
/// </list>
/// </summary>
public sealed class SalesforceBatchSyncService(ISalesforceRateLimiter limiter, IOptions<SyncOptions> options, ILogger<SalesforceBatchSyncService> log)
{
    /// <summary>
    /// Share of lease a batch may use before its Salesforce call is cancelled; rest = margin to record outcome. Write
    /// cancelled this way = unknown outcome → entities on hold.
    /// </summary>
    private const double LeaseUsableFraction = 0.8;

    /// <summary>Hold recording must survive the cancellation that caused it (shutdown), without hanging.</summary>
    private static readonly TimeSpan HoldRecordingTimeout = TimeSpan.FromSeconds(10);

    private readonly SyncOptions _opt = options.Value;

    /// <returns>Inbox rows processed (0 = nothing to do or no capacity).</returns>
    public async Task<int> SyncBatchAsync<TEvent, TRow>(EventReader<TEvent> reader, IRepository<TRow> inbox, SendStep<TEvent> send,
        CancellationToken ct)
        where TRow : InboxRow
    {
        if (limiter.IsPaused)
        {
            return 0;
        }

        // Started before claim: margin measured from lease start
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
            // Salesforce may still apply the write → no rewrite for a while. Caught on shutdown too. Hold not recorded
            // (or process killed first) → lease still protects them
            log.LogWarning(ex, "{Type} batch: holding {Count} events for {Hold}", reader.EntityType, claim.Events.Count, _opt.UnknownOutcomeHold);
            using var recording = new CancellationTokenSource(HoldRecordingTimeout);
            await inbox.ReleaseAsync(claim, new Release(_opt.UnknownOutcomeHold, $"UNKNOWN_OUTCOME: {ex.Message}"), recording.Token);
            ct.ThrowIfCancellationRequested();
            return claim.Events.Count;
        }
        catch (MappingErrorException ex)
        {
            // Inbox stops (every due row waits), error on rows to fix. Nothing dead-lettered; rows resume after deploy,
            // no replay
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
            // Request provably not applied by Salesforce (read or write) → retry every entity. Records saved by earlier
            // calls are safe: retry reads them, writes only diffs
            log.LogError(ex, "{Type} batch failed", reader.EntityType);
            await inbox.RecordAsync(claim, new BatchOutcomes([], [], [.. claim.Events.Select(e => new FailedEvent(e.EntityId, ex.Message))]), ct);
            return claim.Events.Count;
        }

        await inbox.RecordAsync(claim, Outcomes(reader.EntityType, events, dead, failures), ct);
        return claim.Events.Count;
    }

    /// <summary>
    /// Each claimed row's event, read. Dead (retry can't fix): unreadable row, or event about another entity than its
    /// row (lease is on row's entity).
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

    /// <summary>Done: no failure. Dead: unfixable-by-retry failures + unreadable rows. Failed: rest, retried with backoff.</summary>
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
