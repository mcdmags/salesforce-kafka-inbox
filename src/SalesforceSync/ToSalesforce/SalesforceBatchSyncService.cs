using Microsoft.Extensions.Options;
using SalesforceSync.Data.Inbox;
using SalesforceSync.Infrastructure.RateLimiting;
using SalesforceSync.Infrastructure.Salesforce;
using SalesforceSync.Sync;

namespace SalesforceSync.ToSalesforce;

/// <summary>
/// Flow of one batch of one entity type, inbox → Salesforce → back; same for every type. Only flow here; event handling
/// (mapping, reads, writes, reading answers) is the type's, passed as the <c>send</c> function.
/// <list type="number">
/// <item>Nothing starts while process is paused (Salesforce asked).</item>
/// <item><b>Claim</b> ≤ <see cref="SyncOptions.BatchSize"/> due entities from inbox table, leased to this worker: no
/// other worker (any pod) takes them until lease ends. One row per entity = its newest event.</item>
/// <item><b>Parse</b> each row's event (<see cref="EventReader{TEvent}"/>). Unreadable row = dead.</item>
/// <item><b>Send</b>: type's step. Outside any DB transaction (Salesforce can't join one); abandoned before lease ends,
/// so no other worker takes an entity while this one can still write it.</item>
/// <item><b>Record</b> each outcome in inbox: done, retried later, or dead. Whole step fails → all claimed entities
/// held (write outcome unknown), deferred (rate limited), paused (mapping error) or retried.</item>
/// </list>
/// </summary>
public sealed class SalesforceBatchSyncService(ISalesforceRateLimiter limiter, IOptions<SyncOptions> options, ILogger<SalesforceBatchSyncService> log)
{
    /// <summary>
    /// Salesforce calls stop at 80% of the lease, so a slow worker can't write after another worker has claimed the same
    /// entities (Salesforce can't reject a late, older write). The last 20% is left to record the outcome.
    /// </summary>
    private const double LeaseUsableFraction = 0.8;

    /// <summary>
    /// How long recording a hold may take. It gets its own timeout, not the batch's token: on shutdown that token is
    /// already cancelled, yet the hold must still be saved, without delaying shutdown for long.
    /// </summary>
    private static readonly TimeSpan HoldRecordingTimeout = TimeSpan.FromSeconds(10);

    private readonly SyncOptions _opt = options.Value;

    /// <summary>
    /// Syncs one batch of the type. <c>send</c> is the type's handling of the batch's events: it sends them to Salesforce
    /// however it needs (usually through <see cref="SalesforceCalls"/>) and returns the entities that failed, and why;
    /// any other entity is done.
    /// </summary>
    /// <returns>Inbox rows processed (0 = nothing to do or no capacity).</returns>
    public async Task<int> SyncBatchAsync<TEvent, TRow>(EventReader<TEvent> reader, IRepository<TRow> inbox,
        Func<IReadOnlyList<ReadEvent<TEvent>>, CancellationToken, Task<Failures>> send, CancellationToken ct)
        where TRow : InboxRow
    {
        if (limiter.IsPaused)
        {
            return 0;
        }

        // Stop calling Salesforce before the lease ends; started before the claim, so it counts from the lease start
        using var leaseCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        leaseCts.CancelAfter(_opt.Lease * LeaseUsableFraction);

        var claim = await inbox.ClaimAsync(ct);
        if (claim.Events.Count == 0)
        {
            return 0;
        }

        var (events, dead) = Parse(reader, claim.Events);

        Failures failures;
        try
        {
            failures = await send(events, leaseCts.Token);
        }
        catch (UnknownOutcomeException ex)
        {
            // Salesforce may still apply the write, so the entities wait before being written again. Also happens on
            // shutdown. If the hold can't be saved, the lease still keeps other workers off them until it ends
            log.LogWarning(ex, "{Type} batch: holding {Count} events for {Hold}", reader.EntityType, claim.Events.Count, _opt.UnknownOutcomeHold);
            using var recording = new CancellationTokenSource(HoldRecordingTimeout);
            await inbox.ReleaseAsync(claim, new Release(_opt.UnknownOutcomeHold, $"UNKNOWN_OUTCOME: {ex.Message}"), recording.Token);
            ct.ThrowIfCancellationRequested();
            return claim.Events.Count;
        }
        catch (PauseInboxException ex)
        {
            // Wrong for every entity of the type: inbox pauses (every due row waits), error on the rows. Nothing
            // dead-lettered, no attempt spent; a claim after the hold tries again (and pauses it again if not fixed)
            var (hold, code, cause, fix) = ex.Reason switch
            {
                PauseReason.StorageFull => (_opt.StorageFullHold, SalesforceErrors.StorageLimitExceeded, "full data storage",
                    "free space in the org (empty the Recycle Bin, delete test data)"),
                _ => (_opt.MappingErrorHold, "MAPPING_ERROR", "a mapping error", "fix the mapping or add the field")
            };
            log.LogError(ex, "{Type} inbox paused for {Hold} by {Cause}, {Fix}: {Errors}", reader.EntityType, hold, cause, fix,
                string.Join("; ", ex.Errors.Select(e => $"{e.Key}: {e.Error.Code} {e.Error.Message}")));
            await inbox.ReleaseAsync(claim, new Release(hold, $"{code}: the {reader.EntityType} inbox is paused by {cause} ({ex.Message})",
                [.. ex.Errors.GroupBy(e => e.Key).Select(g => new FailedEvent(g.Key, string.Join("; ", g.Select(e => $"{e.Error.Code}: {e.Error.Message}"))))],
                PauseInbox: true), ct);
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
            await inbox.MarkFailedAsync(claim, [.. claim.Events.Select(e => new FailedEvent(e.EntityId, ex.Message))], ct);
            return claim.Events.Count;
        }

        var (done, failed) = Outcomes(reader.EntityType, events, dead, failures);
        await inbox.MarkDoneAsync(claim, done, ct);
        await inbox.MarkFailedAsync(claim, failed, ct);
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
        return new FailedEvent(row.EntityId, $"{SalesforceErrors.InvalidEvent}: {reader.EntityType} {row.EntityId}: {why}", Permanent: true);
    }

    /// <summary>Done: no failure. Failed: the rest; permanent (dead now) if any failure is, or the row was unreadable.</summary>
    private (List<string> Done, List<FailedEvent> Failed) Outcomes<TEvent>(string entityType, List<ReadEvent<TEvent>> events, List<FailedEvent> dead,
        Failures failures)
    {
        foreach (var invalid in dead)
        {
            log.LogWarning("Dead-lettered event {Id}: {Error}", invalid.EntityId, invalid.Error);
        }

        var done = new List<string>();
        var failed = new List<FailedEvent>(dead);
        foreach (var key in events.Select(e => e.EntityId))
        {
            if (!failures.TryGetValue(key, out var list))
            {
                done.Add(key);
                continue;
            }

            var failure = new FailedEvent(key, string.Join("; ", list.OrderByDescending(f => f.Permanent).Select(f => f.Error)),
                Permanent: list.Any(f => f.Permanent));
            log.LogWarning("{Type} {Key}: {Error}", entityType, key, failure.Error);
            failed.Add(failure);
        }

        return (done, failed);
    }
}
