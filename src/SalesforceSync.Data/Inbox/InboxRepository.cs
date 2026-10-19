using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;

namespace SalesforceSync.Data.Inbox;

/// <summary>
/// <see cref="IRepository{TRow}"/> on Postgres, raw SQL through EF. No transactions: each statement commits alone.
/// Units retry (<see cref="PostgresRetryStrategy"/>), so every statement safe to repeat: store again harmless, lost
/// claim waits lease expiry, done again finds row idle. Failure run again counts one extra attempt (rare, accepted).
/// Crash between outcome statements: rest of claim waits lease expiry.
/// <para>
/// Lookups by key only, never join back to table: big backlog stats turned that into hash join over all rows.
/// </para>
/// </summary>
public sealed class InboxRepository<TRow>(IDbContextFactory<SyncDbContext> contexts, IOptions<SyncOptions> options) : IRepository<TRow>
    where TRow : InboxRow
{
    private static readonly string Table = TableOfRow();

    /// <summary>
    /// WHERE of every outcome: claimed rows still pending and still holding claimed event (@entityIds + @times).
    /// Row overwritten by newer event gets no outcome (else marked done unsent); <see cref="EndLeasesAsync"/> makes it due.
    /// <para>
    /// No race with store: same row, row lock orders them. Store first: event_time differs, no match (READ COMMITTED
    /// re-checks). Store second: overwrites finished row, pending again.
    /// </para>
    /// </summary>
    private const string StillClaimed = """
        i.entity_id = ANY(@entityIds) AND i.next_attempt_at IS NOT NULL
          AND EXISTS (SELECT 1 FROM unnest(@entityIds, @times) AS c(entity_id, event_time) WHERE c.entity_id = i.entity_id AND c.event_time = i.event_time)
        """;

    private readonly SyncOptions _opt = options.Value;

    private static string TableOfRow()
    {
        using var db = SyncDbContext.ForModel();
        return db.TableOf<TRow>();
    }

    /// <summary>
    /// Overwrite if event newer. Same time (Kafka redelivery) overwrites too, except done row (already in Salesforce).
    /// Pending row keeps next_attempt_at: if leased that is lease end, moving to now lets second worker claim.
    /// Lease holder's outcome makes it due (<see cref="StillClaimed"/>).
    /// </summary>
    public Task AddAsync(string entityId, DateTimeOffset eventTime, string payload, CancellationToken ct)
    {
        var time = InboxRow.ToWholeMicroseconds(eventTime);
        return contexts.RunAsync((db, t) =>
        {
            var sql = $"""
                INSERT INTO {Table} AS i (entity_id, event_time, payload, next_attempt_at)
                VALUES (@entityId, @eventTime, @payload::jsonb, now())
                ON CONFLICT (entity_id) DO UPDATE
                SET payload = excluded.payload, event_time = excluded.event_time, attempt_count = 0,
                    last_error = NULL, last_updated = now(),
                    next_attempt_at = coalesce(i.next_attempt_at, excluded.next_attempt_at)
                WHERE i.event_time < excluded.event_time
                   OR (i.event_time = excluded.event_time AND (i.next_attempt_at IS NOT NULL OR i.last_error IS NOT NULL))
                """;
            return db.Database.ExecuteAsync(sql, t, new { entityId, eventTime = time, payload });
        }, ct);
    }

    /// <summary>
    /// Lease longest-waiting due rows: next_attempt_at = lease end. SKIP LOCKED: workers claim side by side. READ
    /// COMMITTED re-checks row leased after our snapshot, drops it: no double lease (ClaimRaceTests). Ids into array,
    /// not CTE joined back.
    /// </summary>
    public Task<InboxClaim> ClaimAsync(CancellationToken ct)
    {
        // One statement, atomic alone. Lost commit: its rows wait lease expiry
        return contexts.RunAsync(async (db, t) =>
        {
            var sql = $"""
                UPDATE {Table} i
                SET next_attempt_at = now() + @lease, last_updated = now()
                WHERE i.entity_id = ANY(ARRAY(
                  SELECT d.entity_id FROM {Table} d
                  WHERE d.next_attempt_at <= now()
                  ORDER BY d.next_attempt_at
                  LIMIT @batchSize
                  FOR UPDATE SKIP LOCKED))
                RETURNING i.entity_id AS EntityId, i.payload::text AS Payload, i.event_time AS EventTime
                """;
            var events = await db.Database.QueryAsync<InboxEvent>(sql, t, new
            {
                lease = _opt.Lease,
                batchSize = _opt.BatchSize
            });
            return new InboxClaim(events);
        }, ct);
    }

    /// <summary>One statement per outcome kind, not per entity: fixed round trips per batch.</summary>
    public Task RecordAsync(InboxClaim claim, BatchOutcomes outcomes, CancellationToken ct)
    {
        return WithLeasesEndedAsync(claim, TimeSpan.Zero, async (db, t) =>
        {
            var recorded = new List<string>();
            if (outcomes.Synced.Count > 0)
            {
                recorded.AddRange(await DoneAsync(db, t, claim, outcomes.Synced));
            }
            if (outcomes.Dead.Count > 0)
            {
                recorded.AddRange(await FailAsync(db, t, claim, outcomes.Dead, maxRetries: 0));
            }
            if (outcomes.Failed.Count > 0)
            {
                recorded.AddRange(await FailAsync(db, t, claim, outcomes.Failed, _opt.MaxRetries));
            }
            return recorded;
        }, ct);
    }

    private static Task<List<string>> DoneAsync(SyncDbContext db, CancellationToken ct, InboxClaim claim, IReadOnlyList<string> entityIds)
    {
        var sql = $"""
            UPDATE {Table} i
            SET last_updated = now(), last_error = NULL, next_attempt_at = NULL
            WHERE {StillClaimed}
            RETURNING i.entity_id
            """;
        return db.Database.QueryAsync<string>(sql, ct, new { entityIds = entityIds.ToArray(), times = claim.TimesOf(entityIds) });
    }

    /// <summary>
    /// Exponential backoff, or dead letter (idle with error) at <paramref name="maxRetries"/>-th failure. @errors: one
    /// JSON id -> error, whole batch one statement. Scalar subquery parses once; CTE re-read per row (230 ms / 5,000).
    /// </summary>
    private Task<List<string>> FailAsync(SyncDbContext db, CancellationToken ct, InboxClaim claim, IReadOnlyList<FailedEvent> rows, int maxRetries)
    {
        var sql = $"""
            UPDATE {Table} i
            SET last_updated = now(), attempt_count = i.attempt_count + 1,
                last_error = (SELECT @errors::jsonb) ->> i.entity_id,
                next_attempt_at = CASE WHEN i.attempt_count + 1 < @maxRetries
                                       THEN now() + make_interval(secs => least(@maxDelaySecs, @baseDelaySecs * power(2, i.attempt_count))) END
            WHERE {StillClaimed}
            RETURNING i.entity_id
            """;
        string[] ids = [.. rows.Select(r => r.EntityId)];
        return db.Database.QueryAsync<string>(sql, ct, new
        {
            entityIds = ids,
            times = claim.TimesOf(ids),
            errors = ErrorsJson(rows),
            maxRetries,
            baseDelaySecs = _opt.RetryBaseDelay.TotalSeconds,
            maxDelaySecs = _opt.MaxRetryDelay.TotalSeconds
        });
    }

    /// <summary>
    /// Due again after <see cref="Release.Delay"/>, no attempt spent. Unknown write outcome holds entities till
    /// Salesforce surely applied or dropped it; new events wait too (store keeps pending due time).
    /// <see cref="Release.StopInbox"/> also delays all other due rows: mapping error fails them all, waiting loses
    /// nothing, retrying dead-letters.
    /// </summary>
    public Task ReleaseAsync(InboxClaim claim, Release release, CancellationToken ct)
    {
        return WithLeasesEndedAsync(claim, release.Delay, async (db, t) =>
        {
            var releaseClaimed = $"""
                UPDATE {Table} i
                SET last_updated = now(),
                    last_error = coalesce((SELECT @errors::jsonb) ->> i.entity_id, @error::text, i.last_error),
                    next_attempt_at = now() + @delay
                WHERE {StillClaimed}
                RETURNING i.entity_id
                """;
            var released = await db.Database.QueryAsync<string>(releaseClaimed, t, new
            {
                entityIds = claim.EntityIds,
                times = claim.EventTimes,
                errors = ErrorsJson(release.OwnErrors ?? []),
                error = release.Error,
                delay = release.Delay
            });

            if (release.StopInbox)
            {
                var stopRest = $"""
                    UPDATE {Table}
                    SET next_attempt_at = now() + @delay
                    WHERE next_attempt_at <= now()
                    """;
                await db.Database.ExecuteAsync(stopRest, t, new { delay = release.Delay });
            }

            return released;
        }, ct);
    }

    /// <summary>
    /// Run <paramref name="record"/>, then end lease of claimed rows it skipped (not attempted, or overwritten): due
    /// after <paramref name="delay"/>, not at lease expiry.
    /// </summary>
    private Task WithLeasesEndedAsync(InboxClaim claim, TimeSpan delay, Func<SyncDbContext, CancellationToken, Task<List<string>>> record,
        CancellationToken ct)
    {
        return contexts.RunAsync(async (db, t) =>
        {
            var recorded = (await record(db, t)).ToHashSet(StringComparer.Ordinal);
            if (recorded.Count < claim.Events.Count)
            {
                await EndLeasesAsync(db, t, [.. claim.EntityIds.Where(id => !recorded.Contains(id))], delay);
            }
        }, ct);
    }

    /// <summary>
    /// Caller passes ids outcomes did not return. Not last_updated = now(): store in same microsecond would look
    /// recorded, stay leased till expiry.
    /// </summary>
    private static Task<int> EndLeasesAsync(SyncDbContext db, CancellationToken ct, IReadOnlyList<string> entityIds, TimeSpan delay)
    {
        var sql = $"""
            UPDATE {Table} i
            SET next_attempt_at = now() + @delay, last_updated = now()
            WHERE i.entity_id = ANY(@entityIds) AND i.next_attempt_at IS NOT NULL
            """;
        return db.Database.ExecuteAsync(sql, ct, new { entityIds = entityIds.ToArray(), delay });
    }

    private static string ErrorsJson(IReadOnlyList<FailedEvent> rows)
    {
        return JsonSerializer.Serialize(rows.ToDictionary(r => r.EntityId, r => r.Error));
    }
}
