using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;

namespace SalesforceSync.Data.Inbox;

/// <summary>
/// <see cref="IRepository{TEntity}"/> on Postgres, any inbox row type. Each statement sits in the method that runs it;
/// the table comes from the EF model, so every row type gets the same SQL. Raw SQL through EF, each unit of work through
/// <see cref="PostgresRetryStrategy"/>.
/// <para>
/// One row per entity (entity_id unique): its newest event (a newer one overwrites, <see cref="AddAsync"/>) and its sync
/// state. next_attempt_at is when it is due, and a lease is that time moved to the lease's end. attempt_count, last_error:
/// idle (next_attempt_at null) without last_error = done, with = dead. SQL never reads inside the payload. Claiming
/// locks and leases the row, so two workers can't hold one entity.
/// </para>
/// <para>
/// Outcomes hit only claimed rows that are still pending and still hold the claimed event (<see cref="StillClaimed"/>),
/// and end the lease. The consumer writes the same row; Postgres's row lock orders the two. Every statement is safe to
/// repeat after an unknown commit (see PostgresRetryStrategy): same event stored again is harmless, a retried claim first
/// frees what the lost one leased, done run again finds its rows idle. Failure run again counts one more attempt (rare).
/// Outcomes take arrays: a batch is a fixed number of round trips.
/// </para>
/// <para>
/// Every lookup is by key (primary key, entity_id index, front of the ready index), never a join the planner could turn
/// into a scan, so skewed statistics change no plan and no planner setting is pinned.
/// </para>
/// </summary>
public sealed class InboxRepository<TEntity>(IDbContextFactory<SyncDbContext> contexts, IOptions<SyncOptions> options) : IRepository<TEntity>
    where TEntity : InboxRow, IInboxRow, new()
{
    /// <summary>
    /// WHERE of every outcome statement: of the claimed rows (@entityIds, with the event_time @times the claim read for
    /// each), those still pending (next_attempt_at set) and still holding that event. A row the consumer overwrote after
    /// the claim gets no outcome: it stays pending with the newer event, <see cref="DoneAsync"/> skips it, and
    /// <see cref="EndLeasesAsync"/> ends its lease so it is due. Lookup by primary key; @times are the values the claim
    /// read back, compared exactly.
    /// <para>
    /// Outcome and store can't interleave: both update the one row, so its lock orders them. Store committed first: its
    /// event_time differs, no match. Store holds the row: the outcome waits, then (READ COMMITTED, EvalPlanQual) re-checks
    /// this WHERE on the row the store left, event_time differs. Store comes later: waits, then overwrites the finished
    /// row, pending again and due.
    /// </para>
    /// </summary>
    private const string StillClaimed = """
        i.entity_id = ANY(@entityIds) AND i.next_attempt_at IS NOT NULL
          AND EXISTS (SELECT 1 FROM unnest(@entityIds, @times) AS c(entity_id, event_time) WHERE c.entity_id = i.entity_id AND c.event_time = i.event_time)
        """;

    private readonly SyncOptions _opt = options.Value;
    private string? _table;

    /// <summary>The inbox table of this row type, from the model.</summary>
    private string Table(SyncDbContext db)
    {
        return _table ??= db.TableOf<TEntity>();
    }

    /// <summary>
    /// One event into its entity's row unless the row holds a newer one, entity made due, one statement (@entityId,
    /// @eventTime, @payload). Row created, or overwritten if older: an event is the full record, the newest is all the
    /// sync needs. Overwritten = due again, no attempts, no error, whatever it was (done, dead, claimed: see
    /// <see cref="StillClaimed"/>). Same time as the row's (Kafka redelivery) overwrites too, unless the row is done: then
    /// it is in Salesforce already and nothing changes. Older event: nothing. New or idle entity is due at once; one
    /// already due, backing off, deferred, held or leased keeps its time (a lease's end), and the outcome transaction makes
    /// it due (<see cref="EndLeasesAsync"/>). A -> B -> A: the last A is newer than B, written. Safe to retry. Time cut to
    /// the microsecond (<see cref="InboxRow.ToWholeMicroseconds"/>).
    /// <para>
    /// READ COMMITTED: the conflicting row is locked; if a claim or outcome is updating it, the store waits, then applies
    /// to the row as left, WHERE checked on that version.
    /// </para>
    /// </summary>
    public Task AddAsync(string entityId, DateTimeOffset eventTime, string payload, CancellationToken ct)
    {
        var time = InboxRow.ToWholeMicroseconds(eventTime);
        return contexts.RunAsync((db, t) =>
        {
            var sql = $"""
                INSERT INTO {Table(db)} AS i (entity_id, event_time, payload, next_attempt_at)
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
    /// Leases up to @batchSize due rows, longest-waiting first, and returns them with payload and event time. Own
    /// transaction, so rows are read before the commit is sent. Lease = next_attempt_at moved to now + @lease: not due
    /// until then; an expired lease (dead worker) is simply due again, nobody resets it. FOR UPDATE SKIP LOCKED skips rows
    /// another transaction is claiming, recording or storing into. READ COMMITTED re-checks a row another worker leased
    /// after our snapshot and drops it (not due any more), so no entity is leased twice (ClaimRaceTests). Only rows with a
    /// due time are due: every finishing outcome sets next_attempt_at null. Picked ids are an array, not a CTE joined back:
    /// with backlog statistics the join became a hash join over the whole table.
    /// <para>
    /// Transaction ends before Salesforce is called; from then on the lease protects the entities, not a lock. Unknown
    /// commit outcome and the claim retried: earlier attempts may have leased rows nobody works on, so the retry first ends
    /// those leases (<see cref="EndLeasesAsync"/>), due at once instead of at expiry.
    /// </para>
    /// </summary>
    public Task<InboxClaim> ClaimAsync(CancellationToken ct)
    {
        // What the attempts so far leased: if this runs again, their commits were lost
        var lostEntityIds = new List<string>();
        return contexts.RunInTransactionAsync(async (db, t) =>
        {
            if (lostEntityIds.Count > 0)
            {
                await EndLeasesAsync(db, t, lostEntityIds, TimeSpan.Zero);
            }

            var table = Table(db);
            var sql = $"""
                UPDATE {table} i
                SET next_attempt_at = now() + @lease, last_updated = now()
                WHERE i.entity_id = ANY(ARRAY(
                  SELECT d.entity_id FROM {table} d
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

    /// <summary>
    /// One statement per kind of outcome (not per entity), in the batch's second short transaction; then leases end, rows
    /// not attempted due again at once. Safe to repeat: see <see cref="InTransactionAsync"/>.
    /// </summary>
    public Task RecordAsync(InboxClaim claim, BatchOutcomes outcomes, CancellationToken ct)
    {
        return InTransactionAsync(claim, TimeSpan.Zero, async (db, t) =>
        {
            var recorded = 0;
            if (outcomes.Synced.Count > 0)
            {
                recorded += await DoneAsync(db, t, claim, outcomes.Synced);
            }
            if (outcomes.Dead.Count > 0)
            {
                recorded += await FailAsync(db, t, claim, outcomes.Dead, maxRetries: 0);
            }
            if (outcomes.Failed.Count > 0)
            {
                recorded += await FailAsync(db, t, claim, outcomes.Failed, _opt.MaxRetries);
            }
            return recorded;
        }, ct);
    }

    /// <summary>
    /// Events are in Salesforce: done, so a redelivery changes nothing (<see cref="AddAsync"/>). Lease ends; entity idle
    /// until the consumer stores its next event.
    /// </summary>
    private Task<int> DoneAsync(SyncDbContext db, CancellationToken ct, InboxClaim claim, IReadOnlyList<string> entityIds)
    {
        var sql = $"""
            UPDATE {Table(db)} i
            SET last_updated = now(), last_error = NULL, next_attempt_at = NULL
            WHERE {StillClaimed}
            """;
        return db.Database.ExecuteAsync(sql, ct, new { entityIds = entityIds.ToArray(), times = claim.TimesOf(entityIds) });
    }

    /// <summary>
    /// Rows failed: attempt_count + 1; they stay due and back off exponentially by attempts, or are dead-lettered (idle)
    /// at the <paramref name="maxRetries"/>-th failure; lease ends. Permanent failure = dead at once: 0. @errors is a JSON
    /// object, entity id -> error: a whole batch in one statement. Kept as an uncorrelated scalar subquery, evaluated once
    /// per statement, then a key lookup per row (as a one-row CTE it was copied per row: 230 ms for 5,000 rows).
    /// </summary>
    private Task<int> FailAsync(SyncDbContext db, CancellationToken ct, InboxClaim claim, IReadOnlyList<FailedEvent> rows, int maxRetries)
    {
        var sql = $"""
            UPDATE {Table(db)} i
            SET last_updated = now(), attempt_count = i.attempt_count + 1,
                last_error = (SELECT @errors::jsonb) ->> i.entity_id,
                next_attempt_at = CASE WHEN i.attempt_count + 1 < @maxRetries
                                       THEN now() + make_interval(secs => least(@maxDelaySecs, @baseDelaySecs * power(2, i.attempt_count))) END
            WHERE {StillClaimed}
            """;
        string[] ids = [.. rows.Select(r => r.EntityId)];
        return db.Database.ExecuteAsync(sql, ct, new
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
    /// Hands claimed entities back without an attempt: lease ends, due after <see cref="Release.Delay"/>, with
    /// <see cref="Release.Error"/> on the rows (<see cref="Release.OwnErrors"/> where a row has one; no error = row's error
    /// left alone). The one way back for
    /// <list type="bullet">
    /// <item>rate limited claim (no error, limiter's delay);</item>
    /// <item>write with unknown outcome: hold, nobody touches the entities until Salesforce has applied or dropped the
    /// lost write; events arriving meanwhile wait too (a store keeps a row's due time);</item>
    /// <item>mapping error, which stops the inbox (<see cref="Release.StopInbox"/>): every other due row waits as long, same
    /// transaction. A leased row isn't due; a worker claiming one hits the same error and stops the inbox again. Stores
    /// keep the due time (<see cref="AddAsync"/>), so nothing is lost or dead-lettered and the rows go on after the fix.</item>
    /// </list>
    /// Rows a newer event overwrote, or that are no longer pending, aren't touched by the first statement;
    /// <see cref="EndLeasesAsync"/> ends the lease of the former.
    /// </summary>
    public Task ReleaseAsync(InboxClaim claim, Release release, CancellationToken ct)
    {
        return InTransactionAsync(claim, release.Delay, async (db, t) =>
        {
            var table = Table(db);
            var releaseClaimed = $"""
                UPDATE {table} i
                SET last_updated = now(),
                    last_error = coalesce((SELECT @errors::jsonb) ->> i.entity_id, @error::text, i.last_error),
                    next_attempt_at = now() + @delay
                WHERE {StillClaimed}
                """;
            var released = await db.Database.ExecuteAsync(releaseClaimed, t, new
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
                    UPDATE {table}
                    SET next_attempt_at = now() + @delay
                    WHERE next_attempt_at <= now()
                    """;
                await db.Database.ExecuteAsync(stopRest, t, new { delay = release.Delay });
            }

            return released;
        }, ct);
    }

    public Task FailAllAsync(InboxClaim claim, string error, CancellationToken ct)
    {
        FailedEvent[] rows = [.. claim.EntityIds.Select(id => new FailedEvent(id, error))];
        return InTransactionAsync(claim, TimeSpan.Zero, (db, t) => FailAsync(db, t, claim, rows, _opt.MaxRetries), ct);
    }

    /// <summary>
    /// Outcomes in one transaction: <paramref name="record"/> runs the outcome statements (each hits only claimed rows that
    /// are pending and hold the claimed event, <see cref="StillClaimed"/>) and returns the rows updated. Fewer than the
    /// claim's: <see cref="EndLeasesAsync"/> ends the lease on the rest (not attempted, or overwritten meanwhile), due after
    /// <paramref name="delay"/>. Safe to repeat: unknown commit, or entity finished meanwhile, leaves no pending row.
    /// </summary>
    private Task InTransactionAsync(InboxClaim claim, TimeSpan delay, Func<SyncDbContext, CancellationToken, Task<int>> record,
        CancellationToken ct)
    {
        return contexts.RunInTransactionAsync(async (db, t) =>
        {
            if (await record(db, t) < claim.Events.Count)
            {
                await EndLeasesAsync(db, t, claim.EntityIds, delay);
            }
        }, ct);
    }

    /// <summary>
    /// Ends the lease on pending rows (@entityIds) this transaction hasn't just recorded (a recorded row has last_updated =
    /// now(), the transaction's time); due after @delay, no attempt spent. Last in an outcome transaction these are rows
    /// not attempted (skipped for another entity's record) and rows a newer event overwrote during the lease (see
    /// <see cref="StillClaimed"/>). A rate limited claim is deferred with this statement alone. Due now (or when the hold
    /// ends) instead of at the lease's end, which a store kept as the overwritten row's next_attempt_at. A retried claim
    /// ends the leases of its lost attempts the same way.
    /// </summary>
    private Task<int> EndLeasesAsync(SyncDbContext db, CancellationToken ct, IReadOnlyList<string> entityIds, TimeSpan delay)
    {
        var sql = $"""
            UPDATE {Table(db)} i
            SET next_attempt_at = now() + @delay, last_updated = now()
            WHERE i.entity_id = ANY(@entityIds) AND i.next_attempt_at IS NOT NULL AND i.last_updated <> now()
            """;
        return db.Database.ExecuteAsync(sql, ct, new { entityIds = entityIds.ToArray(), delay });
    }

    /// <summary>{ "entity id": error }: the errors <see cref="FailAsync"/> and <see cref="ReleaseAsync"/> take.</summary>
    private static string ErrorsJson(IReadOnlyList<FailedEvent> rows)
    {
        return JsonSerializer.Serialize(rows.ToDictionary(r => r.EntityId, r => r.Error));
    }
}
