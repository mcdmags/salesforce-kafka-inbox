using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;

namespace SalesforceSync.Data.Inbox;

/// <summary>
/// <see cref="IRepository{TEntity}"/> on Postgres, for any inbox row type. Each statement is in the method that runs it,
/// with the table of the row type (from the EF model, so every row type's table has the same shape and the same
/// statements) put in, and runs through EF's raw SQL. Each unit of work runs through EF's execution strategy
/// (<see cref="PostgresRetryStrategy"/>).
/// <para>
/// An inbox has one row per entity (entity_id is unique) that holds the entity's newest event, a newer event overwriting it
/// (<see cref="AddAsync"/>), and the entity's sync state: when it is due (next_attempt_at), the lease of the worker
/// processing it (the lease is next_attempt_at moved to its end, after which the row is simply due again) and what
/// became of its event (attempt_count, last_error; an idle row, next_attempt_at null, is done without a last_error, else
/// dead). No statement reads inside the payload: an event's entity is its entity_id column, its time its event_time column
/// (both set by the consumer). An entity is claimed by locking and leasing its row, so two workers can never hold one
/// entity: they would lock the same row.
/// </para>
/// <para>
/// Every outcome statement updates only the claimed rows that are still pending and still hold the event
/// the claim read (<see cref="StillClaimed"/>), and ends their lease. The consumer writes the same row, so
/// Postgres's row lock orders the two (see <see cref="StillClaimed"/>). Every statement is safe to run again after an
/// unknown commit (see PostgresRetryStrategy): storing the same event again is harmless (see <see cref="AddAsync"/>),
/// a retried claim first releases what the lost attempt leased (<see cref="ClaimAsync"/>), and a done outcome run again
/// finds its rows idle. (A failure run again counts one more attempt: an unknown commit is rare, and it costs one retry.) Outcome statements take arrays, so recording a batch costs a fixed
/// number of round trips, not one per entity.
/// </para>
/// <para>
/// Every lookup is by key: a primary key (= ANY of an array, never a join the planner could turn into a scan), the
/// entity_id index, or the front of the ready index. With statistics as wrong as a queue's get (taken while
/// everything was idle, or during a backlog that has since drained) these plans still don't change, so no planner
/// setting is pinned.
/// </para>
/// </summary>
public sealed class InboxRepository<TEntity>(IDbContextFactory<SyncDbContext> contexts, IOptions<SyncOptions> options) : IRepository<TEntity>
    where TEntity : InboxRow, IInboxRow, new()
{
    /// <summary>
    /// The WHERE of every outcome statement: of the claimed rows (@entityIds, with the event_time @times the claim read
    /// for each), those that are still pending (next_attempt_at set) and still hold that event. A worker
    /// whose entity was finished by another records nothing; a row the consumer overwrote with a newer event after the claim gets no outcome: it stays
    /// pending with the newer event, <see cref="DoneAsync"/> doesn't mark it done, and <see cref="EndLeasesAsync"/> ends its
    /// lease, leaving it due. Each row is looked up by primary key; the times are compared as timestamptz, exactly: @times
    /// are the values the claim read back from the column.
    /// <para>
    /// Why an outcome and a store can't interleave: both update the entity's one row, so Postgres's row lock orders
    /// them. A store that committed before the outcome statement read the row changed its event_time, and the row
    /// doesn't match. One that has the row locked makes the outcome statement wait; under READ COMMITTED the
    /// statement then checks this WHERE again on the row the store left (EvalPlanQual), whose event_time differs.
    /// One that comes later waits for the outcome's transaction and then overwrites the finished row: pending again,
    /// and due (next_attempt_at, null once finished, becomes now).
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
    /// One event into its entity's row unless that holds a newer one, and the entity made due, in one statement
    /// (@entityId, @eventTime, @payload). The entity's row is created, or overwritten if it holds an older event: an event
    /// is the full record, so the newest is all the sync needs. Overwritten, the row is due again with no attempts and no
    /// error, whatever it was (done, dead, or claimed: see <see cref="StillClaimed"/>). An event as new as the row's (a
    /// Kafka redelivery) overwrites it too, unless the row is done (idle, with no error): then that time is in Salesforce
    /// already, and the redelivery changes nothing, without a claim or a Salesforce call. An older event changes nothing.
    /// A new or idle entity (next_attempt_at null) is due at once. An entity that is already due, backing off, deferred or
    /// held keeps its time, and so does a leased one: its next_attempt_at is the lease's end, so a worker that holds the
    /// entity keeps it, and the outcome transaction makes it due (<see cref="EndLeasesAsync"/>). The last event of
    /// A -> B -> A is newer than B and is written. Safe to retry for the same reason. The time is stored cut to the
    /// microsecond (<see cref="InboxRow.ToWholeMicroseconds"/>).
    /// <para>
    /// Under READ COMMITTED the conflicting row is locked, and if another transaction is updating it (a claim or an
    /// outcome), the store waits for that transaction and then applies to the row as it left it, its WHERE checked
    /// on that version.
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
    /// Picks and leases up to @batchSize due rows, the longest-waiting first, and returns them with their payloads
    /// and event times, in a transaction of its own, so the rows are read before the commit is sent. The lease is
    /// next_attempt_at moved to the lease's end (now + @lease): a leased row is not due until then, and
    /// an expired lease (its worker died) is simply due again and taken over without anyone resetting it. FOR UPDATE SKIP
    /// LOCKED: a row another transaction is claiming, recording or storing an event in is skipped, not waited for. Under
    /// READ COMMITTED a row whose lease another worker committed after this statement's snapshot is re-checked on its new
    /// version when it is locked, and dropped because it is not due any more; so one entity is never leased twice
    /// (ClaimRaceTests). Only rows with a due time are due: every outcome that finishes a row sets next_attempt_at null.
    /// The picked entity ids are an array, not a CTE joined back: with statistics from a backlog, the join was planned as
    /// a hash join over a scan of the whole table.
    /// <para>
    /// The transaction ends before Salesforce is called: from then on the lease (next_attempt_at as its end)
    /// protects the entities, not a lock. If a commit's outcome is unknown and the claim is retried, the attempts before
    /// may have leased rows nobody works on: the retry first ends those leases, by the rows they read, in one statement
    /// (<see cref="EndLeasesAsync"/>), so the rows are due at once, not when the leases expire.
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
    /// One statement per kind of outcome (not per entity), in the batch's second short transaction, then the leases
    /// end, the rows not attempted due again at once. Safe to repeat: see <see cref="InTransactionAsync"/>.
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
    /// The rows' events are in Salesforce: done, so a redelivery of the same event changes nothing (<see cref="AddAsync"/>).
    /// The lease ends and the entity is idle until a consumer stores its next event.
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
    /// Failure of the rows: one more failed attempt (attempt_count); the rows stay due and back off exponentially, by their
    /// attempts, or are dead-lettered (and idle) at the <paramref name="maxRetries"/>-th failure; the lease ends. A
    /// permanent failure is dead-lettered at once: 0. @errors is a JSON object, entity id -> error, so a whole batch's
    /// failures are one statement. The object is an uncorrelated scalar subquery, which Postgres evaluates once per
    /// statement and then only looks up a key in per row. (Joined in as a one-row CTE it was copied once per row:
    /// 230 ms for 5,000 rows.)
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
    /// Hands the claimed entities back without spending an attempt: their leases end and they are due again after
    /// <see cref="Release.Delay"/>, with <see cref="Release.Error"/> on their rows (their <see cref="Release.OwnErrors"/> where
    /// they have one; a row's error is left alone when there is none). This is the one way back for
    /// <list type="bullet">
    /// <item>a rate limited claim (no error, the limiter's delay);</item>
    /// <item>a write whose outcome is unknown: nobody may touch the entities until the hold has passed, by when Salesforce has
    /// applied the lost write or dropped it; events arriving meanwhile wait too (a store keeps a row's due time);</item>
    /// <item>a mapping error, which stops the inbox (<see cref="Release.StopInbox"/>): every other row that is due waits as long,
    /// in the same transaction. A leased row is not due, and a worker that claims one meets the same error and stops the
    /// inbox again. Events stored meanwhile keep the row's due time (<see cref="AddAsync"/>), so nothing is lost or
    /// dead-lettered, and the rows go on after the fix with no replay.</item>
    /// </list>
    /// Rows a newer event overwrote meanwhile, or that this claim no longer holds, are not touched by the first statement;
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
    /// Records outcomes in one transaction: <paramref name="record"/> runs the outcome statements, each of which updates
    /// only the claimed rows that are still pending and still hold the event the claim read
    /// (<see cref="StillClaimed"/>), and returns how many rows they updated. If that is fewer than the claim's,
    /// <see cref="EndLeasesAsync"/> ends the lease on the rest (rows not attempted, and rows a newer event
    /// overwrote meanwhile), due after <paramref name="delay"/>. Safe to repeat: an attempt whose commit outcome was
    /// unknown, or an entity finished meanwhile, leaves no pending row, and nothing is written.
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
    /// Ends the lease on the pending rows (@entityIds) that the transaction has not just recorded, due again after
    /// @delay, with no attempt spent: what becomes of a row that gets no outcome. Last in an outcome transaction, those are
    /// the rows not attempted (skipped because of another entity's record) and the rows a newer event overwrote during the
    /// lease (see <see cref="StillClaimed"/>), whatever was recorded for the others (a recorded row has last_updated =
    /// now(), the transaction's time); a claim rate limited as a whole is deferred with this statement alone. They are
    /// due now (held or deferred: when the hold or deferral ends) instead of at the lease's end, which the store kept as an
    /// overwritten row's next_attempt_at. A retried claim ends the leases of the attempts it lost the same way.
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

    /// <summary>{ "entity id": error }, as <see cref="FailAsync"/> and <see cref="ReleaseAsync"/> take the errors.</summary>
    private static string ErrorsJson(IReadOnlyList<FailedEvent> rows)
    {
        return JsonSerializer.Serialize(rows.ToDictionary(r => r.EntityId, r => r.Error));
    }
}
