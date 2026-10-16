using System.Collections.Concurrent;
using System.Text.RegularExpressions;

namespace SalesforceSync.Inbox;

/// <summary>
/// The statements for one inbox table (one per synced object type, all the same shape; see SyncDbContext) and its
/// entities' rows in sf_sync_state. An inbox has one row per entity (entity_id is unique) that holds the entity's
/// newest event: a newer event overwrites it (<see cref="Template.Store"/>). No statement reads inside the payload: an
/// event's entity is its entity_id column, its time its event_time column (both set by the consumer). An entity is
/// claimed by locking and leasing its state row (lock_token, locked_until), so two workers can never hold one entity:
/// they would lock the same row. Every statement is safe to run again after an unknown commit (see
/// PostgresRetryStrategy): outcomes apply only while the claim's token holds the entity's state row, and only to rows
/// that still hold the event the claim read (<see cref="Template.Unchanged"/>). Outcome statements take arrays, so
/// recording a batch costs a fixed number of round trips, not two per entity.
/// <para>
/// Every lookup is by key: a primary key (= ANY of an array, never a join the planner could turn into a scan), the
/// entity_id index, or the front of the ready index. With statistics as wrong as a queue's get (taken while
/// everything was idle, or during a backlog that has since drained) these plans still don't change, so no planner
/// setting is pinned. (A condition on the inbox's status next to "id = any(@ids)" is left out: it isn't needed, the
/// row of an entity whose state row this token holds is pending.)
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
        ClaimEvents = Template.ClaimEvents.Replace("{inbox}", table);
        Unchanged = Template.Unchanged.Replace("{inbox}", table);
        Done = Template.Done.Replace("{inbox}", table);
        Fail = Template.Fail.Replace("{inbox}", table);
        Dead = Template.Dead.Replace("{inbox}", table);
        EndLease = Template.EndLease.Replace("{inbox}", table);
    }

    public string Table { get; }

    /// <inheritdoc cref="Template.Store"/>
    public string Store { get; }

    /// <inheritdoc cref="Template.ClaimEvents"/>
    public string ClaimEvents { get; }

    /// <inheritdoc cref="Template.Unchanged"/>
    public string Unchanged { get; }

    /// <inheritdoc cref="Template.Done"/>
    public string Done { get; }

    /// <inheritdoc cref="Template.Fail"/>
    public string Fail { get; }

    /// <inheritdoc cref="Template.Dead"/>
    public string Dead { get; }

    /// <inheritdoc cref="Template.EndLease"/>
    public string EndLease { get; }

    /// <summary>
    /// Picks and leases up to @batchSize due entities of @entityType, the longest-waiting first, and returns them with
    /// their watermarks. FOR UPDATE SKIP LOCKED: an entity another transaction is claiming, recording or storing an
    /// event of is skipped, not waited for. Under READ COMMITTED a row whose lease another worker committed after this
    /// statement's snapshot is re-checked on its new version when it is locked, and dropped because it is leased now;
    /// so one entity is never leased twice (ClaimRaceTests). The lease
    /// leaves next_attempt_at as it was: the lock_token/locked_until check keeps a leased entity out, and an expired
    /// lease is taken over without anyone resetting it. The picked ids are an array, not a CTE joined back: with
    /// statistics from a backlog, the join was planned as a hash join over a scan of the whole table.
    /// </summary>
    public const string ClaimEntities = """
        update sf_sync_state s
        set locked_until = now() + @lease, lock_token = @token
        where s.entity_type = @entityType
          and s.entity_id = any(array(
            select d.entity_id from sf_sync_state d
            where d.entity_type = @entityType and d.next_attempt_at <= now()
              and (d.locked_until is null or d.locked_until < now())
            order by d.next_attempt_at
            limit @batchSize
            for update skip locked))
        returning s.entity_id as EntityId, s.synced_time as SyncedTime
        """;

    /// <summary>
    /// First in every outcome transaction: locks the state rows of @entityIds that @token still holds, until the
    /// transaction ends, and returns their ids. From then on no claim can take them over (it skips locked rows), and a
    /// consumer storing an event of one of them waits for this transaction before it touches the entity's inbox row
    /// (see <see cref="Template.Store"/> and <see cref="Template.EndLease"/>).
    /// In entity_id order, so two transactions never lock the same rows in opposite orders.
    /// </summary>
    public const string LockHeld = """
        select s.entity_id from sf_sync_state s
        where s.entity_type = @entityType and s.entity_id = any(@entityIds) and s.lock_token = @token
        order by s.entity_id
        for update
        """;

    /// <summary>The statements with <c>{inbox}</c> for the table name.</summary>
    public static class Template
    {
        /// <summary>
        /// Stores one event (@entityId, @eventTime, @payload) and makes its entity due, in one statement. The entity's
        /// inbox row is created, or overwritten unless it holds a newer event: an event is the full record, so the
        /// newest is all the sync needs. Overwritten, the row is pending again with no attempts and no error, whatever
        /// it was (done, superseded, dead, or claimed: see <see cref="Unchanged"/>). An event as new as the row's (a
        /// Kafka redelivery) overwrites it too; its claim then finds it not newer than the watermark and ends it
        /// superseded without a Salesforce call. An older event changes nothing in the inbox. A new entity's state row
        /// is created, and a new or idle one (next_attempt_at null) is due after @coalesceDelay
        /// (<see cref="SyncOptions.CoalesceDelay"/>), so the rest of a burst of its events overwrites the row before it
        /// is claimed and only the newest is written. An entity that is already due, backing off or held keeps its
        /// time.
        /// <para>
        /// The state row is upserted first (the inbox insert reads from it), which takes its row lock: a store waits for
        /// an outcome transaction that holds the entity's state row (<see cref="LockHeld"/>) before it touches the inbox
        /// row, which is what makes storing safe against that transaction recording outcomes and ending the lease (see
        /// <see cref="Unchanged"/> and <see cref="EndLease"/>).
        /// </para>
        /// </summary>
        public const string Store = """
            with state as (
              insert into sf_sync_state as s (entity_type, entity_id, next_attempt_at)
              values (@entityType, @entityId, now() + @coalesceDelay)
              on conflict (entity_type, entity_id)
              do update set next_attempt_at = coalesce(s.next_attempt_at, now() + @coalesceDelay)
              returning s.entity_id
            )
            insert into {inbox} as i (entity_id, event_time, payload)
            select state.entity_id, @eventTime, @payload::jsonb from state
            on conflict (entity_id) do update
            set payload = excluded.payload, event_time = excluded.event_time, status = 'pending', attempt_count = 0,
                last_error = null, last_error_code = null, received_at = now(), last_updated = now()
            where i.event_time <= excluded.event_time
            """;

        /// <summary>
        /// The claimed entities' rows, if pending (at most one per entity, found by the entity_id index), returned with
        /// their payloads and event times, each one's attempt_count bumped. Run after <see cref="ClaimEntities"/>, in a
        /// statement of its own: it sees every event committed before the entities were locked.
        /// </summary>
        public const string ClaimEvents = """
            update {inbox} i
            set attempt_count = i.attempt_count + 1, last_updated = now()
            where i.entity_id = any(@entityIds) and i.status = 'pending'
            returning i.id as RowId, i.entity_id as EntityId, i.payload::text as Payload, i.attempt_count as AttemptCount,
                      i.event_time as EventTime
            """;

        /// <summary>
        /// Second in every outcome transaction, after <see cref="LockHeld"/>: of the claimed rows (@ids, with the
        /// event_time @times the claim read for each), the ids of those that still hold that event. A row the consumer
        /// overwrote with a newer event after the claim is not among them, and no outcome is recorded on it: it stays
        /// pending with the newer event, Done doesn't move the watermark to it, and <see cref="EndLease"/> makes its
        /// entity due again. Checked once per transaction: no store can change these rows before it ends, since storing
        /// locks the entity's state row first (<see cref="Store"/>) and this transaction holds that lock. Each row is
        /// looked up by primary key; the times are compared as timestamptz, exactly: @times are the values the claim read
        /// back from the column.
        /// </summary>
        public const string Unchanged = """
            select c.id from unnest(@ids, @times) as c(id, event_time)
            where exists (select 1 from {inbox} i where i.id = c.id and i.event_time = c.event_time)
            """;

        /// <summary>
        /// The entities' events are in Salesforce: their rows are done, and each entity's watermark moves to
        /// the event's time (its event_time in microseconds since the epoch; extract returns numeric, so exactly), never
        /// back. Like every outcome, only while @token holds the entity's state row, so a worker whose lease expired
        /// neither completes events nor moves the watermark.
        /// </summary>
        public const string Done = """
            with done as (
              update {inbox} i
              set status = 'done', last_updated = now(), last_error = null, last_error_code = null
              where i.id = any(@ids)
                and exists (select 1 from sf_sync_state h
                            where h.entity_type = @entityType and h.entity_id = i.entity_id and h.lock_token = @token)
              returning i.entity_id, (extract(epoch from i.event_time) * 1000000)::bigint as synced_time
            )
            update sf_sync_state s
            set synced_time = greatest(s.synced_time, done.synced_time)
            from done
            where s.entity_type = @entityType and s.entity_id = done.entity_id
            """;

        /// <summary>
        /// Transient failure: the rows stay pending (dead-lettered once a row was claimed @maxRetries times) and their
        /// entities back off exponentially, by the row's attempt_count. @errors is a JSON object, row id -> [error,
        /// code], so a whole batch's failures are one statement. The events are found by primary
        /// key like every other outcome; the object is an uncorrelated scalar subquery, which Postgres evaluates once
        /// per statement and then only looks up a key in per row. (Joined in as a one-row CTE it was copied once per
        /// row: 230 ms for 5,000 rows.)
        /// </summary>
        public const string Fail = """
            with failed as (
              update {inbox} i
              set last_updated = now(),
                  last_error = (select @errors::jsonb) -> i.id::text ->> 0,
                  last_error_code = (select @errors::jsonb) -> i.id::text ->> 1,
                  status = case when i.attempt_count >= @maxRetries then 'dead' else 'pending' end
              where i.id = any(@ids)
                and exists (select 1 from sf_sync_state h
                            where h.entity_type = @entityType and h.entity_id = i.entity_id and h.lock_token = @token)
              returning i.entity_id, i.attempt_count
            )
            update sf_sync_state s
            set next_attempt_at = now() + make_interval(secs => least(@maxDelaySecs, @baseDelaySecs * power(2, failed.attempt_count - 1)))
            from failed
            where s.entity_type = @entityType and s.entity_id = failed.entity_id
            """;

        /// <summary>Permanent failure: retrying the same data can't succeed, dead-letter now. @errors as in <see cref="Fail"/>.</summary>
        public const string Dead = """
            update {inbox} i
            set last_updated = now(),
                last_error = (select @errors::jsonb) -> i.id::text ->> 0,
                last_error_code = (select @errors::jsonb) -> i.id::text ->> 1,
                status = 'dead'
            where i.id = any(@ids)
              and exists (select 1 from sf_sync_state h
                          where h.entity_type = @entityType and h.entity_id = i.entity_id and h.lock_token = @token)
            """;

        /// <summary>
        /// Last in an outcome transaction: ends the lease on @entityIds. An entity whose row is still pending (retried,
        /// released, or overwritten by a newer event during the lease) is due again: now, or when a failure's backoff
        /// (<see cref="Fail"/>) ends, whichever is later; one whose row is finished is idle (next_attempt_at null) until
        /// a consumer stores its next event. A row overwritten during the lease gets no coalesce delay
        /// (<see cref="Store"/>): its event has already waited as long as the batch's Salesforce calls took, and newer
        /// events that arrive before the next claim overwrite it anyway.
        /// <para>
        /// Why an event stored meanwhile is never left behind: storing upserts the entity's state row
        /// (<see cref="Store"/>), and this transaction locked that row first (<see cref="LockHeld"/>). A store that got
        /// the row lock before that had committed by the time this transaction got it, so this statement, which takes
        /// its snapshot later, sees the pending row and keeps the entity due. A store that comes later waits for this
        /// transaction to commit, then overwrites the inbox row as it was left and makes the idle entity due:
        /// next_attempt_at null becomes now + @coalesceDelay.
        /// </para>
        /// </summary>
        public const string EndLease = """
            update sf_sync_state s
            set locked_until = null, lock_token = null,
                next_attempt_at = case when exists (select 1 from {inbox} i where i.entity_id = s.entity_id and i.status = 'pending')
                                       then greatest(s.next_attempt_at, now()) end
            where s.entity_type = @entityType and s.entity_id = any(@entityIds) and s.lock_token = @token
            """;
    }
}