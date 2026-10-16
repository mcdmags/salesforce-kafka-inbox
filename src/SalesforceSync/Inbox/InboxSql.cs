using System.Collections.Concurrent;
using System.Text.RegularExpressions;

namespace SalesforceSync.Inbox;

/// <summary>
/// The statements for one inbox table (one per synced object type, all the same shape; see SyncDbContext). An inbox has
/// one row per entity (entity_id is unique) that holds the entity's newest event, a newer event overwriting it
/// (<see cref="Template.Store"/>), and the entity's sync state: when it is due (next_attempt_at), the lease of the worker
/// processing it (lock_token, locked_until) and its watermark (synced_time). No statement reads inside the payload: an
/// event's entity is its entity_id column, its time its event_time column (both set by the consumer). An entity is
/// claimed by locking and leasing its row, so two workers can never hold one entity: they would lock the same row.
/// <para>
/// Every outcome statement updates only the claimed rows that its claim's token still holds and that still hold the
/// event the claim read (<see cref="Template.Held"/>), and ends their lease. The consumer writes the same row, so
/// Postgres's row lock orders the two (see <see cref="Template.Held"/>). Every statement is safe to run again after an
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
        Supersede = Template.Supersede.Replace("{inbox}", table);
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

    /// <inheritdoc cref="Template.Supersede"/>
    public string Supersede { get; }

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
        /// overwritten unless it holds a newer event: an event is the full record, so the newest is all the sync needs.
        /// Overwritten, the row is pending again with no attempts and no error, whatever it was (done, superseded, dead,
        /// or claimed: see <see cref="Held"/>). An event as new as the row's (a Kafka redelivery) overwrites it too; its
        /// claim then finds it not newer than the watermark and ends it superseded without a Salesforce call. An older
        /// event changes nothing. A new or idle entity (next_attempt_at null) is due after @coalesceDelay
        /// (<see cref="SyncOptions.CoalesceDelay"/>), so the rest of a burst of its events overwrites the row before it
        /// is claimed and only the newest is written. An entity that is already due, backing off, deferred or held
        /// keeps its time. The watermark and the lease are left as they are: a worker that holds the entity keeps it.
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
                last_error = null, last_error_code = null, received_at = now(), last_updated = now(),
                next_attempt_at = coalesce(i.next_attempt_at, excluded.next_attempt_at)
            where i.event_time <= excluded.event_time
            """;

        /// <summary>
        /// Picks and leases up to @batchSize due rows, the longest-waiting first, bumps each one's attempt_count and
        /// returns them with their payloads, event times and watermarks. FOR UPDATE SKIP LOCKED: a row another
        /// transaction is claiming, recording or storing an event in is skipped, not waited for. Under READ COMMITTED
        /// a row whose lease another worker committed after this statement's snapshot is re-checked on its new version
        /// when it is locked, and dropped because it is leased now; so one entity is never leased twice
        /// (ClaimRaceTests). Only pending rows are due: every outcome that finishes a row sets next_attempt_at null.
        /// The lease leaves next_attempt_at as it was: the lock_token/locked_until check keeps a leased row out, and
        /// an expired lease is taken over without anyone resetting it. The picked ids are an array, not a CTE joined
        /// back: with statistics from a backlog, the join was planned as a hash join over a scan of the whole table.
        /// </summary>
        public const string Claim = """
            update {inbox} i
            set locked_until = now() + @lease, lock_token = @token, attempt_count = i.attempt_count + 1, last_updated = now()
            where i.id = any(array(
              select d.id from {inbox} d
              where d.next_attempt_at <= now() and (d.locked_until is null or d.locked_until < now())
              order by d.next_attempt_at
              limit @batchSize
              for update skip locked))
            returning i.id as RowId, i.entity_id as EntityId, i.payload::text as Payload, i.event_time as EventTime,
                      i.synced_time as SyncedTime
            """;

        /// <summary>
        /// The WHERE of every outcome statement: of the claimed rows (@ids, with the event_time @times the claim read
        /// for each), those that @token still holds and that still hold that event. A worker whose lease expired
        /// records nothing; a row the consumer overwrote with a newer event after the claim gets no outcome: it stays
        /// pending with the newer event, Done doesn't move the watermark to it, and <see cref="EndLease"/> ends its
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
        public const string Held = """
            i.id = any(@ids) and i.lock_token = @token
              and exists (select 1 from unnest(@ids, @times) as c(id, event_time) where c.id = i.id and c.event_time = i.event_time)
            """;

        /// <summary>
        /// The rows' events are in Salesforce: done, and each one's watermark moves to the event's time (its event_time
        /// in microseconds since the epoch; extract returns numeric, so exactly), never back. The lease ends and the
        /// entity is idle until a consumer stores its next event.
        /// </summary>
        public const string Done = $$"""
            update {inbox} i
            set status = 'done', last_updated = now(), last_error = null, last_error_code = null,
                synced_time = greatest(i.synced_time, (extract(epoch from i.event_time) * 1000000)::bigint),
                next_attempt_at = null, locked_until = null, lock_token = null
            where {{Held}}
            """;

        /// <summary>Not written (an event as new was already synced): the row leaves the queue, and the lease ends.</summary>
        public const string Supersede = $$"""
            update {inbox} i
            set status = 'superseded', last_updated = now(), last_error = null, last_error_code = null,
                next_attempt_at = null, locked_until = null, lock_token = null
            where {{Held}}
            """;

        /// <summary>
        /// Not attempted (skipped because of another entity's record, or rate limited): the lease ends and the row is
        /// due again, not before @delay, with no attempt spent.
        /// </summary>
        public const string Release = $$"""
            update {inbox} i
            set attempt_count = greatest(i.attempt_count - 1, 0), last_updated = now(),
                next_attempt_at = greatest(i.next_attempt_at, now() + @delay), locked_until = null, lock_token = null
            where {{Held}}
            """;

        /// <summary>
        /// Transient failure: the rows stay pending and back off exponentially, by their attempt_count, or are
        /// dead-lettered (and idle) once claimed @maxRetries times; the lease ends. @errors is a JSON object, row id ->
        /// [error, code], so a whole batch's failures are one statement. The object is an uncorrelated scalar subquery,
        /// which Postgres evaluates once per statement and then only looks up a key in per row. (Joined in as a one-row
        /// CTE it was copied once per row: 230 ms for 5,000 rows.)
        /// </summary>
        public const string Fail = $$"""
            update {inbox} i
            set last_updated = now(),
                last_error = (select @errors::jsonb) -> i.id::text ->> 0,
                last_error_code = (select @errors::jsonb) -> i.id::text ->> 1,
                status = case when i.attempt_count >= @maxRetries then 'dead' else 'pending' end,
                next_attempt_at = case when i.attempt_count < @maxRetries
                                       then now() + make_interval(secs => least(@maxDelaySecs, @baseDelaySecs * power(2, i.attempt_count - 1))) end,
                locked_until = null, lock_token = null
            where {{Held}}
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
                status = 'dead', next_attempt_at = null, locked_until = null, lock_token = null
            where {{Held}}
            """;

        /// <summary>
        /// The write's outcome is unknown: the rows stay pending with @error and @code, no attempt spent, and are not
        /// due before @hold has passed; the lease ends, so the old worker can't record anything on them either.
        /// </summary>
        public const string Hold = $$"""
            update {inbox} i
            set attempt_count = greatest(i.attempt_count - 1, 0), last_updated = now(), last_error = @error, last_error_code = @code,
                next_attempt_at = now() + @hold, locked_until = null, lock_token = null
            where {{Held}}
            """;

        /// <summary>
        /// Last in an outcome transaction: ends the lease on the claimed rows (@ids) that @token still holds after the
        /// outcomes, which are the rows a newer event overwrote during the lease (see <see cref="Held"/>). They are
        /// pending and stay due (the store kept next_attempt_at, which the claim left set), now or after @delay,
        /// whichever is later. Such a row gets no coalesce delay (<see cref="Store"/>): its event has already waited as
        /// long as the batch's Salesforce calls took, and newer events that arrive before the next claim overwrite it
        /// anyway.
        /// </summary>
        public const string EndLease = """
            update {inbox} i
            set locked_until = null, lock_token = null, next_attempt_at = greatest(i.next_attempt_at, now() + @delay)
            where i.id = any(@ids) and i.lock_token = @token
            """;
    }
}