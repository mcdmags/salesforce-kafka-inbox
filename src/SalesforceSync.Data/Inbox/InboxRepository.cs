using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;

namespace SalesforceSync.Data.Inbox;

/// <summary>
/// <see cref="IRepository{TRow}"/> on Postgres, raw SQL through EF. Each unit of work runs through
/// <see cref="PostgresRetryStrategy"/>, so every statement must be safe to repeat after a commit whose outcome was lost:
/// storing the same event again is harmless, a retried claim first frees what the lost one leased, done again finds
/// its rows idle. Only a failure run again counts one extra attempt (rare, accepted).
/// <para>
/// Lookups go by key only (primary key, front of the ready index), never a join the planner could turn into a scan:
/// with a large backlog's statistics a join back to the table became a hash join over all of it.
/// </para>
/// </summary>
public sealed class InboxRepository<TRow>(IDbContextFactory<SyncDbContext> contexts, IOptions<SyncOptions> options) : IRepository<TRow>
    where TRow : InboxRow
{
    private static readonly string Table = TableOfRow();

    /// <summary>
    /// WHERE of every outcome: rows of the claim (@entityIds with the @times the claim read) that are still pending and
    /// still hold the claimed event. A row a newer event overwrote during the lease gets no outcome, else that event
    /// would be marked done without being sent; <see cref="EndLeasesAsync"/> makes it due instead.
    /// <para>
    /// No race with a store: both update the same row, so its lock orders them. If the store goes first, event_time
    /// differs and nothing matches (READ COMMITTED re-checks this WHERE on the row the store left). If it goes second,
    /// it overwrites the finished row and makes it pending again.
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
    /// Overwrites the row if the event is newer. Same time (Kafka redelivery) overwrites too, except a done row: it is
    /// already in Salesforce. A pending row keeps its next_attempt_at: if it is leased that is the lease end, and moving
    /// it to now would let a second worker claim the entity; the lease holder's outcome makes it due
    /// (<see cref="StillClaimed"/>).
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
    /// Leases the longest-waiting due rows by moving next_attempt_at to the lease end. SKIP LOCKED lets workers claim
    /// side by side; READ COMMITTED re-checks a row another worker leased after our snapshot and drops it, so no entity
    /// is leased twice (ClaimRaceTests). Ids are picked into an array, not a CTE joined back (see class doc).
    /// </summary>
    public Task<InboxClaim> ClaimAsync(CancellationToken ct)
    {
        // Retried after a lost commit: earlier attempts may have leased rows nobody works on; free them first instead
        // of waiting for their leases to expire
        var lostEntityIds = new List<string>();
        return contexts.RunInTransactionAsync(async (db, t) =>
        {
            if (lostEntityIds.Count > 0)
            {
                await EndLeasesAsync(db, t, lostEntityIds, TimeSpan.Zero);
            }

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
            lostEntityIds.AddRange(events.Select(e => e.EntityId));
            return new InboxClaim(events);
        }, ct);
    }

    /// <summary>One statement per kind of outcome, not per entity, so a batch is a fixed number of round trips.</summary>
    public Task RecordAsync(InboxClaim claim, BatchOutcomes outcomes, CancellationToken ct)
    {
        return InTransactionAsync(claim, TimeSpan.Zero, async (db, t) =>
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
    /// Backs off exponentially, or dead-letters (idle with error) at the <paramref name="maxRetries"/>-th failure.
    /// @errors is one JSON object id -> error, so a whole batch is one statement. It sits in a scalar subquery so
    /// Postgres parses it once per statement; as a CTE it was re-read per row (230 ms for 5,000 rows).
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
    /// Due again after <see cref="Release.Delay"/> without spending an attempt. An unknown write outcome holds the
    /// entities until Salesforce has surely applied or dropped the lost write; events arriving meanwhile wait too,
    /// because a store keeps a pending row's due time. <see cref="Release.StopInbox"/> also delays every other due row
    /// in the same transaction: a mapping error fails them all, and waiting loses nothing, while retrying would
    /// dead-letter them.
    /// </summary>
    public Task ReleaseAsync(InboxClaim claim, Release release, CancellationToken ct)
    {
        return InTransactionAsync(claim, release.Delay, async (db, t) =>
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
    /// Runs <paramref name="record"/>, then ends the lease of the claim's rows it did not update (not attempted, or
    /// overwritten by a newer event), due after <paramref name="delay"/>, so they don't sit until the lease expires.
    /// </summary>
    private Task InTransactionAsync(InboxClaim claim, TimeSpan delay, Func<SyncDbContext, CancellationToken, Task<List<string>>> record,
        CancellationToken ct)
    {
        return contexts.RunInTransactionAsync(async (db, t) =>
        {
            var recorded = (await record(db, t)).ToHashSet(StringComparer.Ordinal);
            if (recorded.Count < claim.Events.Count)
            {
                await EndLeasesAsync(db, t, [.. claim.EntityIds.Where(id => !recorded.Contains(id))], delay);
            }
        }, ct);
    }

    /// <summary>
    /// The caller passes the ids the outcome statements did not return, rather than this matching on
    /// last_updated = now(): a store can commit in the same microsecond, and its row would look recorded and stay
    /// leased until expiry.
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
