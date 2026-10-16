using System.Collections.Concurrent;
using System.Text.RegularExpressions;

namespace SalesforceSync.Inbox;

/// <summary>
/// The statements for one inbox table (one per synced object type, all the same shape; see SyncDbContext) and its
/// entities' rows in sf_sync_state. No statement reads inside the payload: an event's entity is its entity_id column,
/// and which event of an entity is the newest is decided in C#. An entity is claimed by locking and leasing its state
/// row (lock_token, locked_until), so two workers can never hold one entity: they would lock the same row. Every
/// statement is safe to run again after an unknown commit (see PostgresRetryStrategy): outcomes apply only while the
/// claim's token holds the entity's state row. Outcome statements take arrays, so recording a batch costs a fixed
/// number of round trips, not two per entity.
/// <para>
/// Every lookup is by key: a primary key (= ANY of an array, never a join the planner could turn into a scan), the
/// pending index per entity (LATERAL ... LIMIT), or the front of the ready index. With statistics as wrong as a queue's
/// get (taken while everything was idle, or during a backlog that has since drained) these plans still don't change,
/// so no planner setting is pinned. (A condition on the inbox's status next to "id = any(@ids)" is left out for that
/// reason: it let the planner read the ids off the whole pending index. It isn't needed: the events of an entity whose
/// state row this token holds are pending.)
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
    /// so one entity is never leased twice, whatever order its events were committed in (ClaimRaceTests). The lease
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
    /// consumer storing an event of one of them waits for this transaction (see <see cref="Template.EndLease"/>).
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
        /// Stores events (@entityIds[i], @payloads[i], in that order) and makes their entities due, in one statement:
        /// a new entity's state row is created, and a new or idle one (next_attempt_at null) is due after @coalesceDelay
        /// (<see cref="SyncOptions.CoalesceDelay"/>), so the rest of a burst of its events is claimed with it and merged
        /// into one write. An entity that is already due, backing off, deferred or held keeps its time. Upserting the
        /// state row takes its row lock, which is what makes storing safe against an outcome transaction ending the
        /// entity's lease at the same time (see <see cref="EndLease"/>). Entities in id order, so two stores never lock
        /// rows in opposite orders.
        /// </summary>
        public const string Store = """
            with stored as (
              insert into {inbox} (entity_id, payload)
              select u.entity_id, u.payload::jsonb
              from unnest(@entityIds::text[], @payloads::text[]) with ordinality as u(entity_id, payload, n)
              order by u.n
              returning entity_id
            )
            insert into sf_sync_state as s (entity_type, entity_id, next_attempt_at)
            select @entityType, stored.entity_id, now() + @coalesceDelay
            from stored group by stored.entity_id order by stored.entity_id
            on conflict (entity_type, entity_id)
            do update set next_attempt_at = coalesce(s.next_attempt_at, now() + @coalesceDelay)
            """;

        /// <summary>
        /// The claimed entities' events: per entity its oldest @maxPerEntity pending events (a LATERAL ... LIMIT probe
        /// of the pending index), returned with their payloads, each one's attempt_count bumped. Bounded because an
        /// event is the full record: of a deep backlog only the newest event will be written, so returning all of it
        /// would cost a write and a payload per event for nothing. The rest is taken by later claims and superseded by
        /// the watermark unless newer. Run after <see cref="ClaimEntities"/>, in a statement of its own: it sees every
        /// event committed before the entities were locked.
        /// </summary>
        public const string ClaimEvents = """
            update {inbox} u
            set attempt_count = u.attempt_count + 1, last_updated = now()
            from unnest(@entityIds) as e(entity_id)
            cross join lateral (
              select o.id from {inbox} o
              where o.entity_id = e.entity_id and o.status = 'pending'
              order by o.id limit @maxPerEntity) r
            where u.id = r.id
            returning u.id as RowId, u.entity_id as EntityId, u.payload::text as Payload, u.attempt_count as AttemptCount
            """;

        /// <summary>
        /// The entities' newest events are in Salesforce: those events are done, and each entity's watermark moves to
        /// the event's time (@times, a JSON object event id -> time, bound once as in <see cref="Fail"/>), never back.
        /// Like every outcome, only while @token holds the entity's state row, so a worker whose lease expired neither
        /// completes events nor moves the watermark.
        /// </summary>
        public const string Done = """
            with done as (
              update {inbox} i
              set status = 'done', last_updated = now(), last_error = null, last_error_code = null
              where i.id = any(@ids)
                and exists (select 1 from sf_sync_state h
                            where h.entity_type = @entityType and h.entity_id = i.entity_id and h.lock_token = @token)
              returning i.entity_id, ((select @times::jsonb) ->> i.id::text)::bigint as synced_time
            )
            update sf_sync_state s
            set synced_time = greatest(s.synced_time, done.synced_time)
            from done
            where s.entity_type = @entityType and s.entity_id = done.entity_id
            """;

        /// <summary>
        /// Transient failure: the events stay pending (dead-lettered once an event was claimed @maxRetries times) and
        /// their entity backs off exponentially, by the most-attempted of its failed events. @errors is a JSON object,
        /// event id -> [error, code], so a whole batch's failures are one statement. The events are found by primary
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
            set next_attempt_at = now() + make_interval(secs => least(@maxDelaySecs, @baseDelaySecs * power(2, f.attempts - 1)))
            from (select failed.entity_id, max(failed.attempt_count) as attempts from failed group by failed.entity_id) f
            where s.entity_type = @entityType and s.entity_id = f.entity_id
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
        /// Last in an outcome transaction: ends the lease on @entityIds. An entity with pending events left is due
        /// again: now, after @delay, or when a failure's backoff (<see cref="Fail"/>) ends, whichever is latest; one
        /// with none is idle (next_attempt_at null) until a consumer stores its next event. Events left pending get no
        /// coalesce delay (<see cref="Store"/>): they arrived during the lease, so they have already waited as long as
        /// the batch's Salesforce calls took, and more of the burst that arrive before the next claim merge with them.
        /// <para>
        /// Why an event stored meanwhile is never left behind: storing upserts the entity's state row
        /// (<see cref="Store"/>), and this transaction locked that row first (<see cref="LockHeld"/>). A store that got
        /// the row lock before that had committed by the time this transaction got it, so this statement, which takes
        /// its snapshot later, sees the event and keeps the entity due. A store that comes later waits for this
        /// transaction to commit and then updates the row as it left it: next_attempt_at null becomes now +
        /// @coalesceDelay.
        /// </para>
        /// </summary>
        public const string EndLease = """
            update sf_sync_state s
            set locked_until = null, lock_token = null,
                next_attempt_at = case when exists (select 1 from {inbox} i where i.entity_id = s.entity_id and i.status = 'pending')
                                       then greatest(s.next_attempt_at, now() + @delay) end
            where s.entity_type = @entityType and s.entity_id = any(@entityIds) and s.lock_token = @token
            """;
    }
}