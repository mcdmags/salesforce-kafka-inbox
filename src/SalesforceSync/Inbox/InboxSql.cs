using System.Collections.Concurrent;
using System.Text.RegularExpressions;

namespace SalesforceSync.Inbox;

/// <summary>
/// The statements for one inbox table (one per synced object type, all the same shape; see SyncDbContext). An inbox has
/// one row per entity (entity_id is unique) that holds the entity's newest event, a newer event overwriting it
/// (<see cref="Template.Store"/>), and the entity's sync state: when it is due (next_attempt_at), the lease of the worker
/// processing it (lock_token; the lease ends at next_attempt_at, after which the row is simply due again) and what
/// became of its event (status). No statement reads inside the payload: an event's entity is its entity_id column, its
/// time its event_time column (both set by the consumer). An entity is claimed by locking and leasing its row, so two
/// workers can never hold one entity: they would lock the same row.
/// <para>
/// Every outcome statement updates only the claimed rows that its claim's token still holds and that still hold the
/// event the claim read (<see cref="Template.StillClaimed"/>), and ends their lease. The consumer writes the same row, so
/// Postgres's row lock orders the two (see <see cref="Template.StillClaimed"/>). Every statement is safe to run again after an
/// unknown commit (see PostgresRetryStrategy): storing the same event again is harmless (see
/// InboxRepository.AddAsync), a retried claim first releases what the lost attempt leased (InboxRepository.ClaimAsync),
/// and an outcome's first run ended the lease, so a second finds no row it holds. Outcome statements take arrays, so
/// recording a batch costs a fixed number of round trips, not one per entity.
/// </para>
/// <para>
/// Every lookup is by key: a primary key (= ANY of an array, never a join the planner could turn into a scan), the
/// entity_id index, or the front of the ready index. With statistics as wrong as a queue's get (taken while
/// everything was idle, or during a backlog that has since drained) these plans still don't change, so no planner
/// setting is pinned.
/// </para>
/// </summary>
public sealed partial class InboxSql
{
    private static readonly ConcurrentDictionary<string, InboxSql> Cache = new(StringComparer.Ordinal);

    [GeneratedRegex("^[a-z_][a-z0-9_]{0,48}$", RegexOptions.CultureInvariant)]
    private static partial Regex PlainIdentifier();

    /// <summary>The statements for <paramref name="table"/>, built once per table.</summary>
    /// <exception cref="ArgumentException">The name is not a plain lower-case identifier.</exception>
    public static InboxSql For(string table)
    {
        return Cache.GetOrAdd(table, static t => new InboxSql(RequireIdentifier(t)));
    }

    /// <summary>
    /// Table names are spliced into SQL text (identifiers can't be parameters), so only plain identifiers are
    /// accepted. They come from code constants, never from configuration or events.
    /// </summary>
    public static string RequireIdentifier(string table)
    {
        return PlainIdentifier().IsMatch(table)
                ? table
                : throw new ArgumentException($"'{table}' is not a valid inbox table name (lower-case letters, digits and _)", nameof(table));
    }

    private InboxSql(string table)
    {
        Table = table;
        Store = Template.Store.Replace("{inbox}", table);
        Claim = Template.Claim.Replace("{inbox}", table);
        Done = Template.Done.Replace("{inbox}", table);
        Release = Template.Release.Replace("{inbox}", table);
        Fail = Template.Fail.Replace("{inbox}", table);
        Dead = Template.Dead.Replace("{inbox}", table);
        Hold = Template.Hold.Replace("{inbox}", table);
        EndLease = Template.EndLease.Replace("{inbox}", table);
    }

    public string Table { get; }

    /// <inheritdoc cref="Template.Store"/>
    public string Store { get; }

    /// <inheritdoc cref="Template.Claim"/>
    public string Claim { get; }

    /// <inheritdoc cref="Template.Done"/>
    public string Done { get; }

    /// <inheritdoc cref="Template.Release"/>
    public string Release { get; }

    /// <inheritdoc cref="Template.Fail"/>
    public string Fail { get; }

    /// <inheritdoc cref="Template.Dead"/>
    public string Dead { get; }

    /// <inheritdoc cref="Template.Hold"/>
    public string Hold { get; }

    /// <inheritdoc cref="Template.EndLease"/>
    public string EndLease { get; }

    /// <summary>The statements with <c>{inbox}</c> for the table name.</summary>
    public static class Template
    {
        /// <summary>
        /// Stores one event (@entityId, @eventTime, @payload) in one statement. The entity's row is created, or
        /// overwritten if it holds an older event: an event is the full record, so the newest is all the sync needs.
        /// Overwritten, the row is pending again with no attempts and no error, whatever it was (done, dead, or claimed:
        /// see <see cref="StillClaimed"/>). An event as new as the row's (a Kafka redelivery) overwrites it too, unless
        /// the row is done: then that time is in Salesforce already, and the redelivery changes nothing, without a
        /// claim or a Salesforce call. An older event changes nothing. A new or idle entity (next_attempt_at null) is
        /// due after @coalesceDelay (<see cref="SyncOptions.CoalesceDelay"/>), so the rest of a burst of its events
        /// overwrites the row before it is claimed and only the newest is written. An entity that is already due,
        /// backing off, deferred or held keeps its time, and so does a leased one: its next_attempt_at is the lease's
        /// end, so a worker that holds the entity keeps it, and the outcome transaction makes it due
        /// (<see cref="EndLease"/>).
        /// <para>
        /// Under READ COMMITTED the conflicting row is locked, and if another transaction is updating it (a claim or an
        /// outcome), the store waits for that transaction and then applies to the row as it left it, its WHERE checked
        /// on that version.
        /// </para>
        /// </summary>
        public const string Store = """
            insert into {inbox} as i (entity_id, event_time, payload, next_attempt_at)
            values (@entityId, @eventTime, @payload::jsonb, now() + @coalesceDelay)
            on conflict (entity_id) do update
            set payload = excluded.payload, event_time = excluded.event_time, status = 'pending', attempt_count = 0,
                last_error = null, last_error_code = null, last_updated = now(),
                next_attempt_at = coalesce(i.next_attempt_at, excluded.next_attempt_at)
            where i.event_time < excluded.event_time or (i.event_time = excluded.event_time and i.status <> 'done')
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
        /// The picked ids are an array, not a CTE joined back: with statistics from a backlog, the join was planned as
        /// a hash join over a scan of the whole table.
        /// </summary>
        public const string Claim = """
            update {inbox} i
            set next_attempt_at = now() + @lease, lock_token = @token, last_updated = now()
            where i.id = any(array(
              select d.id from {inbox} d
              where d.next_attempt_at <= now()
              order by d.next_attempt_at
              limit @batchSize
              for update skip locked))
            returning i.id as RowId, i.entity_id as EntityId, i.payload::text as Payload, i.event_time as EventTime
            """;

        /// <summary>
        /// The WHERE of every outcome statement: of the claimed rows (@ids, with the event_time @times the claim read
        /// for each), those that @token still holds and that still hold that event. A worker whose lease expired
        /// records nothing; a row the consumer overwrote with a newer event after the claim gets no outcome: it stays
        /// pending with the newer event, Done doesn't mark it done, and <see cref="EndLease"/> ends its
        /// lease, leaving it due. Each row is looked up by primary key; the times are compared as timestamptz, exactly:
        /// @times are the values the claim read back from the column.
        /// <para>
        /// Why an outcome and a store can't interleave: both update the entity's one row, so Postgres's row lock orders
        /// them. A store that committed before the outcome statement read the row changed its event_time, and the row
        /// doesn't match. One that has the row locked makes the outcome statement wait; under READ COMMITTED the
        /// statement then checks this WHERE again on the row the store left (EvalPlanQual), whose event_time differs.
        /// One that comes later waits for the outcome's transaction and then overwrites the finished row: pending again,
        /// and due (next_attempt_at, null once finished, becomes now + @coalesceDelay).
        /// </para>
        /// </summary>
        public const string StillClaimed = """
            i.id = any(@ids) and i.lock_token = @token
              and exists (select 1 from unnest(@ids, @times) as c(id, event_time) where c.id = i.id and c.event_time = i.event_time)
            """;

        /// <summary>
        /// The rows' events are in Salesforce: done, so a redelivery of the same event changes nothing (<see cref="Store"/>).
        /// The lease ends and the entity is idle until a consumer stores its next event.
        /// </summary>
        public const string Done = $$"""
            update {inbox} i
            set status = 'done', last_updated = now(), last_error = null, last_error_code = null,
                next_attempt_at = null, lock_token = null
            where {{StillClaimed}}
            """;

        /// <summary>
        /// Not attempted (skipped because of another entity's record, or rate limited): the lease ends and the row is
        /// due again, not before @delay, with no attempt spent.
        /// </summary>
        public const string Release = $$"""
            update {inbox} i
            set last_updated = now(), next_attempt_at = now() + @delay, lock_token = null
            where {{StillClaimed}}
            """;

        /// <summary>
        /// Transient failure: one more failed attempt (attempt_count); the rows stay pending and back off exponentially,
        /// by their attempts, or are dead-lettered (and idle) at the @maxRetries-th; the lease ends. @errors is a JSON
        /// object, row id -> [error, code], so a whole batch's failures are one statement. The object is an uncorrelated scalar subquery,
        /// which Postgres evaluates once per statement and then only looks up a key in per row. (Joined in as a one-row
        /// CTE it was copied once per row: 230 ms for 5,000 rows.)
        /// </summary>
        public const string Fail = $$"""
            update {inbox} i
            set last_updated = now(), attempt_count = i.attempt_count + 1,
                last_error = (select @errors::jsonb) -> i.id::text ->> 0,
                last_error_code = (select @errors::jsonb) -> i.id::text ->> 1,
                status = case when i.attempt_count + 1 >= @maxRetries then 'dead' else 'pending' end,
                next_attempt_at = case when i.attempt_count + 1 < @maxRetries
                                       then now() + make_interval(secs => least(@maxDelaySecs, @baseDelaySecs * power(2, i.attempt_count))) end,
                lock_token = null
            where {{StillClaimed}}
            """;

        /// <summary>
        /// Permanent failure: retrying the same data can't succeed, dead-letter now (idle; the lease ends). @errors as
        /// in <see cref="Fail"/>.
        /// </summary>
        public const string Dead = $$"""
            update {inbox} i
            set last_updated = now(),
                last_error = (select @errors::jsonb) -> i.id::text ->> 0,
                last_error_code = (select @errors::jsonb) -> i.id::text ->> 1,
                status = 'dead', next_attempt_at = null, lock_token = null
            where {{StillClaimed}}
            """;

        /// <summary>
        /// The write's outcome is unknown: the rows stay pending with @error and @code, no attempt spent, and are not
        /// due before @hold has passed; the lease ends, so the old worker can't record anything on them either.
        /// </summary>
        public const string Hold = $$"""
            update {inbox} i
            set last_updated = now(), last_error = @error, last_error_code = @code,
                next_attempt_at = now() + @hold, lock_token = null
            where {{StillClaimed}}
            """;

        /// <summary>
        /// Ends the lease on the rows (@ids) that @token still holds, due after @delay. Last in an outcome transaction,
        /// those are the rows a newer event overwrote during the lease (see <see cref="StillClaimed"/>): pending, and
        /// due now (held or deferred: when the hold or deferral ends) instead of at the lease's end, which the store
        /// kept as their next_attempt_at. Such a row gets no coalesce delay (<see cref="Store"/>): its event has already
        /// waited as long as the batch's Salesforce calls took, and newer events that arrive before the next claim
        /// overwrite it anyway. A retried claim ends the leases of the attempt it lost the same way (due now).
        /// </summary>
        public const string EndLease = """
            update {inbox} i
            set lock_token = null, next_attempt_at = now() + @delay
            where i.id = any(@ids) and i.lock_token = @token
            """;
    }
}