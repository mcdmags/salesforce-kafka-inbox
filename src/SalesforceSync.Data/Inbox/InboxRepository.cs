using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;

namespace SalesforceSync.Data.Inbox;

/// <summary>
/// <see cref="IRepository{TEntity}"/> on Postgres, for any inbox row type. The statements are in this class, as
/// templates with <c>{inbox}</c> for the table, which comes from the EF model (so every row type's table has the same
/// shape and the same statements) and run through EF's raw SQL. Each unit of work runs through EF's execution strategy
/// (<see cref="PostgresRetryStrategy"/>).
/// <para>
/// An inbox has one row per entity (entity_id is unique) that holds the entity's newest event, a newer event overwriting it
/// (<see cref="StoreSql"/>), and the entity's sync state: when it is due (next_attempt_at), the lease of the worker
/// processing it (lock_token; the lease ends at next_attempt_at, after which the row is simply due again) and what
/// became of its event (status). No statement reads inside the payload: an event's entity is its entity_id column, its
/// time its event_time column (both set by the consumer). An entity is claimed by locking and leasing its row, so two
/// workers can never hold one entity: they would lock the same row.
/// </para>
/// <para>
/// Every outcome statement updates only the claimed rows that its claim's token still holds and that still hold the event
/// the claim read (<see cref="StillClaimedSql"/>), and ends their lease. The consumer writes the same row, so
/// Postgres's row lock orders the two (see <see cref="StillClaimedSql"/>). Every statement is safe to run again after an
/// unknown commit (see PostgresRetryStrategy): storing the same event again is harmless (see <see cref="AddAsync"/>),
/// a retried claim first releases what the lost attempt leased (<see cref="ClaimAsync"/>), and an outcome's first run
/// ended the lease, so a second finds no row it holds. Outcome statements take arrays, so recording a batch costs a fixed
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
    private readonly SyncOptions _opt = options.Value;
    private Statements? _sql;

    // ---------- The statements ----------

    /// <summary>
    /// Stores one event (@entityId, @eventTime, @payload) in one statement. The entity's row is created, or
    /// overwritten if it holds an older event: an event is the full record, so the newest is all the sync needs.
    /// Overwritten, the row is pending again with no attempts and no error, whatever it was (done, dead, or claimed:
    /// see <see cref="StillClaimedSql"/>). An event as new as the row's (a Kafka redelivery) overwrites it too, unless
    /// the row is done: then that time is in Salesforce already, and the redelivery changes nothing, without a
    /// claim or a Salesforce call. An older event changes nothing. A new or idle entity (next_attempt_at null) is
    /// due at once. An entity that is already due, backing off, deferred or held keeps its time, and so does a
    /// leased one: its next_attempt_at is the lease's end, so a worker that holds the entity keeps it, and the
    /// outcome transaction makes it due (<see cref="EndLeaseSql"/>).
    /// <para>
    /// Under READ COMMITTED the conflicting row is locked, and if another transaction is updating it (a claim or an
    /// outcome), the store waits for that transaction and then applies to the row as it left it, its WHERE checked
    /// on that version.
    /// </para>
    /// </summary>
    public const string StoreSql = """
        INSERT INTO {inbox} AS i (entity_id, event_time, payload, next_attempt_at)
        VALUES (@entityId, @eventTime, @payload::jsonb, now())
        ON CONFLICT (entity_id) DO UPDATE
        SET payload = excluded.payload, event_time = excluded.event_time, status = 'pending', attempt_count = 0,
            last_error = NULL, last_error_code = NULL, last_updated = now(),
            next_attempt_at = coalesce(i.next_attempt_at, excluded.next_attempt_at)
        WHERE i.event_time < excluded.event_time OR (i.event_time = excluded.event_time AND i.status <> 'done')
        """;

    /// <summary>
    /// Picks and leases up to @batchSize due rows, the longest-waiting first, and returns them with their payloads
    /// and event times. The lease is lock_token and next_attempt_at moved to the lease's end (now + @lease): a
    /// leased row is not due until then, and an expired lease (its worker died) is simply due again and taken over
    /// without anyone resetting it. FOR UPDATE SKIP LOCKED: a row another
    /// transaction is claiming, recording or storing an event in is skipped, not waited for. Under READ COMMITTED
    /// a row whose lease another worker committed after this statement's snapshot is re-checked on its new version
    /// when it is locked, and dropped because it is not due any more; so one entity is never leased twice
    /// (ClaimRaceTests). Only pending rows are due: every outcome that finishes a row sets next_attempt_at null.
    /// The picked entity ids are an array, not a CTE joined back: with statistics from a backlog, the join was planned as
    /// a hash join over a scan of the whole table.
    /// </summary>
    public const string ClaimSql = """
        UPDATE {inbox} i
        SET next_attempt_at = now() + @lease, lock_token = @token, last_updated = now()
        WHERE i.entity_id = ANY(ARRAY(
          SELECT d.entity_id FROM {inbox} d
          WHERE d.next_attempt_at <= now()
          ORDER BY d.next_attempt_at
          LIMIT @batchSize
          FOR UPDATE SKIP LOCKED))
        RETURNING i.entity_id AS EntityId, i.payload::text AS Payload, i.event_time AS EventTime
        """;

    /// <summary>
    /// The WHERE of every outcome statement: of the claimed rows (@entityIds, with the event_time @times the claim read
    /// for each), those that @token still holds and that still hold that event. A worker whose lease expired
    /// records nothing; a row the consumer overwrote with a newer event after the claim gets no outcome: it stays
    /// pending with the newer event, Done doesn't mark it done, and <see cref="EndLeaseSql"/> ends its lease, leaving it
    /// due. Each row is looked up by primary key; the times are compared as timestamptz, exactly: @times are the
    /// values the claim read back from the column.
    /// <para>
    /// Why an outcome and a store can't interleave: both update the entity's one row, so Postgres's row lock orders
    /// them. A store that committed before the outcome statement read the row changed its event_time, and the row
    /// doesn't match. One that has the row locked makes the outcome statement wait; under READ COMMITTED the
    /// statement then checks this WHERE again on the row the store left (EvalPlanQual), whose event_time differs.
    /// One that comes later waits for the outcome's transaction and then overwrites the finished row: pending again,
    /// and due (next_attempt_at, null once finished, becomes now).
    /// </para>
    /// </summary>
    public const string StillClaimedSql = """
        i.entity_id = ANY(@entityIds) AND i.lock_token = @token
          AND EXISTS (SELECT 1 FROM unnest(@entityIds, @times) AS c(entity_id, event_time) WHERE c.entity_id = i.entity_id AND c.event_time = i.event_time)
        """;

    /// <summary>
    /// The rows' events are in Salesforce: done, so a redelivery of the same event changes nothing (<see cref="StoreSql"/>).
    /// The lease ends and the entity is idle until a consumer stores its next event.
    /// </summary>
    public const string DoneSql = $$"""
        UPDATE {inbox} i
        SET status = 'done', last_updated = now(), last_error = NULL, last_error_code = NULL,
            next_attempt_at = NULL, lock_token = NULL
        WHERE {{StillClaimedSql}}
        """;

    /// <summary>
    /// Failure: one more failed attempt (attempt_count); the rows stay pending and back off exponentially, by their
    /// attempts, or are dead-lettered (and idle) at the @maxRetries-th; the lease ends. A permanent failure is
    /// dead-lettered at once: @maxRetries 0. @errors is a JSON object, entity id -> [error, code], so a whole batch's
    /// failures are one statement. The object is an uncorrelated scalar subquery, which Postgres evaluates once per
    /// statement and then only looks up a key in per row. (Joined in as a one-row CTE it was copied once per row:
    /// 230 ms for 5,000 rows.)
    /// </summary>
    public const string FailSql = $$"""
        UPDATE {inbox} i
        SET last_updated = now(), attempt_count = i.attempt_count + 1,
            last_error = (SELECT @errors::jsonb) -> i.entity_id ->> 0,
            last_error_code = (SELECT @errors::jsonb) -> i.entity_id ->> 1,
            status = CASE WHEN i.attempt_count + 1 >= @maxRetries THEN 'dead' ELSE 'pending' END,
            next_attempt_at = CASE WHEN i.attempt_count + 1 < @maxRetries
                                   THEN now() + make_interval(secs => least(@maxDelaySecs, @baseDelaySecs * power(2, i.attempt_count))) END,
            lock_token = NULL
        WHERE {{StillClaimedSql}}
        """;

    /// <summary>
    /// The write's outcome is unknown: the rows stay pending with @error and @code, no attempt spent, and are not
    /// due before @hold has passed; the lease ends, so the old worker can't record anything on them either.
    /// </summary>
    public const string HoldSql = $$"""
        UPDATE {inbox} i
        SET last_updated = now(), last_error = @error, last_error_code = @code,
            next_attempt_at = now() + @hold, lock_token = NULL
        WHERE {{StillClaimedSql}}
        """;

    /// <summary>
    /// A mapping error stops the inbox: the claimed rows stay pending, no attempt spent, not due before @hold has
    /// passed, with the error to fix on them: their own in @errors (a JSON object, entity id -> [error]) or, for the
    /// healthy ones that were only held with them, @error. @code names the cause for a query on last_error_code.
    /// </summary>
    public const string HaltSql = $$"""
        UPDATE {inbox} i
        SET last_updated = now(),
            last_error = coalesce((SELECT @errors::jsonb) -> i.entity_id ->> 0, @error),
            last_error_code = @code, next_attempt_at = now() + @hold, lock_token = NULL
        WHERE {{StillClaimedSql}}
        """;

    /// <summary>
    /// The rest of a stopped inbox: every row that is due now waits @hold too (a leased row is not due, and a
    /// worker that claims one meets the same error and stops the inbox again). Events stored meanwhile keep the
    /// row's due time (<see cref="StoreSql"/>), so nothing is lost or dead-lettered, and the rows go on after the fix
    /// with no replay.
    /// </summary>
    public const string HaltRestSql = """
        UPDATE {inbox}
        SET next_attempt_at = now() + @hold
        WHERE next_attempt_at <= now()
        """;

    /// <summary>
    /// Ends the lease on the rows (@entityIds) that any of @tokens still holds, due again after @delay, with no attempt spent:
    /// what becomes of a row that gets no outcome. Last in an outcome transaction, those are the rows not attempted
    /// (skipped because of another entity's record) and the rows a newer event overwrote during the lease (see
    /// <see cref="StillClaimedSql"/>), whatever was recorded for the others; a claim rate limited as a whole is
    /// deferred with this statement alone.
    /// They are pending, and due now (held or deferred: when the hold or deferral ends) instead of at the lease's
    /// end, which the store kept as an overwritten row's next_attempt_at. Ending a lease needs no check of the
    /// claimed event: the row is due again either way. A retried claim ends the leases of the attempts it lost
    /// the same way, all their tokens at once. The rows are looked up by @entityIds: lock_token has no index.
    /// </summary>
    public const string EndLeaseSql = """
        UPDATE {inbox} i
        SET lock_token = NULL, next_attempt_at = now() + @delay, last_updated = now()
        WHERE i.entity_id = ANY(@entityIds) AND i.lock_token = ANY(@tokens)
        """;

    /// <summary>The statements with the table of this row type, built once from the model's table name.</summary>
    private Statements Sql(SyncDbContext db)
    {
        return _sql ??= new Statements(db.TableOf<TEntity>());
    }

    private sealed class Statements(string table)
    {
        public string Store { get; } = StoreSql.Replace("{inbox}", table);
        public string Claim { get; } = ClaimSql.Replace("{inbox}", table);
        public string Done { get; } = DoneSql.Replace("{inbox}", table);
        public string Fail { get; } = FailSql.Replace("{inbox}", table);
        public string Hold { get; } = HoldSql.Replace("{inbox}", table);
        public string Halt { get; } = HaltSql.Replace("{inbox}", table);
        public string HaltRest { get; } = HaltRestSql.Replace("{inbox}", table);
        public string EndLease { get; } = EndLeaseSql.Replace("{inbox}", table);
    }

    // ---------- The unit of work of each outcome ----------

    /// <summary>
    /// One event into its entity's row unless that holds a newer one, and the entity made due
    /// (<see cref="StoreSql"/>, one statement). A Kafka redelivery of a synced event changes nothing, and
    /// one of an event not yet synced overwrites the row with itself, which is harmless; the last event of A -> B -> A
    /// is newer than B and is written. Safe to retry for the same reason. The time is stored cut to the microsecond
    /// (<see cref="InboxRow.ToWholeMicroseconds"/>).
    /// </summary>
    public Task AddAsync(string entityId, DateTimeOffset eventTime, string payload, CancellationToken ct)
    {
        var time = InboxRow.ToWholeMicroseconds(eventTime);
        return contexts.RunAsync((db, t) => db.Database.ExecuteAsync(Sql(db).Store, t,
                new { entityId, eventTime = time, payload }), ct);
    }

    /// <summary>
    /// Locks and leases a batch of due rows and reads them (<see cref="ClaimSql"/>), in a transaction of
    /// its own, so the rows are read before the commit is sent. The transaction ends before Salesforce is called: from
    /// then on the lease (lock_token, and next_attempt_at as its end) protects the entities, not a lock.
    /// <para>
    /// Each attempt leases under a fresh token. If a commit's outcome is unknown and the claim is retried, the attempts
    /// before may have leased rows nobody works on: the retry first ends those leases, by the lost tokens and the rows
    /// they read, in one statement (<see cref="EndLeaseSql"/>), so the rows are due at once, not when the
    /// leases expire, and are not mistaken for this attempt's.
    /// </para>
    /// </summary>
    public Task<InboxClaim> ClaimAsync(CancellationToken ct)
    {
        // What the attempts so far leased: if this runs again, their commits were lost
        var lostTokens = new List<Guid>();
        var lostEntityIds = new List<string>();
        return contexts.RunInTransactionAsync(async (db, t) =>
        {
            var sql = Sql(db);
            if (lostEntityIds.Count > 0)
            {
                await db.Database.ExecuteAsync(sql.EndLease, t,
                    new { entityIds = lostEntityIds.ToArray(), tokens = lostTokens.ToArray(), delay = TimeSpan.Zero });
            }

            var token = Guid.NewGuid();
            var events = await db.Database.QueryAsync<InboxEvent>(sql.Claim, t, new
            {
                token,
                lease = _opt.Lease,
                batchSize = _opt.BatchSize
            });
            lostTokens.Add(token);
            lostEntityIds.AddRange(events.Select(e => e.EntityId));
            return new InboxClaim(token, events);
        }, ct);
    }

    /// <summary>
    /// One statement per kind of outcome (not per entity), in the batch's second short transaction, then the leases
    /// end, the rows not attempted due again at once. Safe to repeat: see <see cref="InTransactionAsync"/>.
    /// </summary>
    public Task RecordAsync(InboxClaim claim, BatchOutcomes outcomes, CancellationToken ct)
    {
        var token = claim.Token;
        return InTransactionAsync(claim, TimeSpan.Zero, async (db, sql, t) =>
        {
            var recorded = 0;
            if (outcomes.Synced.Count > 0)
            {
                recorded += await db.Database.ExecuteAsync(sql.Done, t,
                    new { entityIds = outcomes.Synced.ToArray(), times = claim.TimesOf(outcomes.Synced), token });
            }
            if (outcomes.Dead.Count > 0)
            {
                recorded += await FailAsync(db, sql, claim, outcomes.Dead, maxRetries: 0, t);
            }
            if (outcomes.Failed.Count > 0)
            {
                recorded += await FailAsync(db, sql, claim, outcomes.Failed, _opt.MaxRetries, t);
            }
            return recorded;
        }, ct);
    }

    /// <summary>
    /// The write's outcome is unknown: nobody may touch these entities until the hold has passed, by when Salesforce
    /// has applied the lost write or dropped it. The rows stay pending, not due before then, and no longer leased, so
    /// the old worker can't record anything on them either; events arriving meanwhile wait too (a store keeps a row's
    /// due time). No attempt is spent.
    /// </summary>
    public Task HoldAsync(InboxClaim claim, string error, CancellationToken ct)
    {
        var hold = _opt.UnknownOutcomeHold;
        return InTransactionAsync(claim, hold, (db, sql, t) => db.Database.ExecuteAsync(sql.Hold, t, new
        {
            entityIds = claim.EntityIds,
            times = claim.EventTimes,
            token = claim.Token,
            error,
            code = InboxRow.UnknownOutcomeCode,
            hold
        }), ct);
    }

    /// <summary>
    /// A mapping error stops the inbox: the claimed entities are held with the error on their rows (their own for the
    /// <paramref name="broken"/> ones), then every other due row waits as long, in the same transaction.
    /// </summary>
    public Task HaltAsync(InboxClaim claim, IReadOnlyList<FailedEvent> broken, string error, CancellationToken ct)
    {
        var hold = _opt.MappingErrorHold;
        return InTransactionAsync(claim, hold, async (db, sql, t) =>
        {
            var held = await db.Database.ExecuteAsync(sql.Halt, t, new
            {
                entityIds = claim.EntityIds,
                times = claim.EventTimes,
                token = claim.Token,
                errors = ErrorsJson(broken),
                error,
                code = InboxRow.MappingErrorCode,
                hold
            });
            await db.Database.ExecuteAsync(sql.HaltRest, t, new { hold });
            return held;
        }, ct);
    }

    /// <summary>
    /// Rate limited: hands the entities back, not before <paramref name="delay"/>, without spending an attempt: only
    /// their leases end (<see cref="EndLeaseSql"/>).
    /// </summary>
    public Task DeferAsync(InboxClaim claim, TimeSpan delay, CancellationToken ct)
    {
        return contexts.RunAsync((db, t) => db.Database.ExecuteAsync(Sql(db).EndLease, t,
            new { entityIds = claim.EntityIds, tokens = new[] { claim.Token }, delay }), ct);
    }

    public Task FailAllAsync(InboxClaim claim, string error, string? code, CancellationToken ct)
    {
        FailedEvent[] rows = [.. claim.EntityIds.Select(id => new FailedEvent(id, error, code))];
        return InTransactionAsync(claim, TimeSpan.Zero, (db, sql, t) => FailAsync(db, sql, claim, rows, _opt.MaxRetries, t), ct);
    }

    /// <summary>
    /// Records outcomes in one transaction: <paramref name="record"/> runs the outcome statements, each of which updates
    /// only the claimed rows this lease still holds that still hold the event the claim read
    /// (<see cref="StillClaimedSql"/>), and returns how many rows they updated. If that is fewer than the claim's,
    /// <see cref="EndLeaseSql"/> ends the lease on the rest (rows not attempted, and rows a newer event
    /// overwrote meanwhile), due after <paramref name="delay"/>. Safe to repeat: an attempt whose commit outcome was
    /// unknown, or a lease lost meanwhile, leaves no row under this token, and nothing is written.
    /// </summary>
    private Task InTransactionAsync(InboxClaim claim, TimeSpan delay, Func<SyncDbContext, Statements, CancellationToken, Task<int>> record,
        CancellationToken ct)
    {
        return contexts.RunInTransactionAsync(async (db, t) =>
        {
            var sql = Sql(db);
            if (await record(db, sql, t) < claim.Events.Count)
            {
                await db.Database.ExecuteAsync(sql.EndLease, t, new { entityIds = claim.EntityIds, tokens = new[] { claim.Token }, delay });
            }
        }, ct);
    }

    /// <summary>The rows failed: retried with backoff, or dead-lettered at the <paramref name="maxRetries"/>-th failure (0: now).</summary>
    private Task<int> FailAsync(SyncDbContext db, Statements sql, InboxClaim claim, IReadOnlyList<FailedEvent> rows, int maxRetries,
        CancellationToken ct)
    {
        string[] ids = [.. rows.Select(r => r.EntityId)];
        return db.Database.ExecuteAsync(sql.Fail, ct, new
        {
            entityIds = ids,
            times = claim.TimesOf(ids),
            errors = ErrorsJson(rows),
            token = claim.Token,
            maxRetries,
            baseDelaySecs = _opt.RetryBaseDelay.TotalSeconds,
            maxDelaySecs = _opt.MaxRetryDelay.TotalSeconds
        });
    }

    /// <summary>{ "entity id": [error, code] }, as <see cref="FailSql"/> takes the errors.</summary>
    private static string ErrorsJson(IReadOnlyList<FailedEvent> rows)
    {
        return JsonSerializer.Serialize(rows.ToDictionary(r => r.EntityId, r => new[] { r.Error, r.Code }));
    }
}