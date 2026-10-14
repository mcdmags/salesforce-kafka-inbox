using System.Collections.Concurrent;
using System.Text.RegularExpressions;

namespace SalesforceSync.Inbox;

/// <summary>
/// The statements for one inbox table (one per synced object type, all the same shape; see sql/schema.sql). No
/// statement reads inside the payload: a row's entity is its entity_id column, and which event of an entity is the
/// newest is decided in C#. Rows are leased per entity. While a row is leased its next_attempt_at is the end of the
/// lease, so leased rows drop out of the claim's scan and come back on their own if the lease expires.
/// Every statement is safe to run again after an unknown commit (see PostgresExecutionStrategy): updates are
/// conditional on the lease token and the watermark only moves forward. Outcome statements take arrays, so recording
/// a batch costs a fixed number of round trips, not two per entity.
/// </summary>
public sealed partial class InboxSql
{
    /// <summary>
    /// How many claimable rows a claim looks at before reducing them to entities. Larger than a batch because several
    /// rows usually belong to one entity; bounded so a claim never walks a deep backlog row by row.
    /// </summary>
    public const int ClaimWindow = 500;

    private static readonly ConcurrentDictionary<string, InboxSql> Cache = new(StringComparer.Ordinal);

    [GeneratedRegex("^[a-z_][a-z0-9_]{0,48}$", RegexOptions.CultureInvariant)]
    private static partial Regex PlainIdentifier();

    /// <summary>The statements for <paramref name="table"/>, built once per table.</summary>
    /// <exception cref="ArgumentException">The name is not a plain lower-case identifier.</exception>
    public static InboxSql For(string table) => Cache.GetOrAdd(table, static t => new InboxSql(RequireIdentifier(t)));

    /// <summary>
    /// Table names are spliced into SQL text (identifiers can't be parameters), so only plain identifiers are
    /// accepted. They come from code constants, never from configuration or events.
    /// </summary>
    public static string RequireIdentifier(string table)
        => PlainIdentifier().IsMatch(table)
            ? table
            : throw new ArgumentException($"'{table}' is not a valid inbox table name (lower-case letters, digits and _)", nameof(table));

    private InboxSql(string table)
    {
        Table = table;
        LockHeads = Template.LockHeads.Replace("{inbox}", table);
        ClaimEntities = Template.ClaimEntities.Replace("{inbox}", table);
        ReleaseLostClaims = Template.ReleaseLostClaims.Replace("{inbox}", table);
        MarkSynced = Template.MarkSynced.Replace("{inbox}", table);
        Complete = Template.Complete.Replace("{inbox}", table);
        Fail = Template.Fail.Replace("{inbox}", table);
        Dead = Template.Dead.Replace("{inbox}", table);
        Defer = Template.Defer.Replace("{inbox}", table);
        Release = Template.Release.Replace("{inbox}", table);
        Hold = Template.Hold.Replace("{inbox}", table);
        Insert = Template.Insert.Replace("{inbox}", table);
    }

    public string Table { get; }

    /// <inheritdoc cref="Template.LockHeads"/>
    public string LockHeads { get; }

    /// <inheritdoc cref="Template.ClaimEntities"/>
    public string ClaimEntities { get; }

    /// <inheritdoc cref="Template.ReleaseLostClaims"/>
    public string ReleaseLostClaims { get; }

    /// <inheritdoc cref="Template.MarkSynced"/>
    public string MarkSynced { get; }

    /// <inheritdoc cref="Template.Complete"/>
    public string Complete { get; }

    /// <inheritdoc cref="Template.Fail"/>
    public string Fail { get; }

    /// <inheritdoc cref="Template.Dead"/>
    public string Dead { get; }

    /// <inheritdoc cref="Template.Defer"/>
    public string Defer { get; }

    /// <inheritdoc cref="Template.Release"/>
    public string Release { get; }

    /// <inheritdoc cref="Template.Hold"/>
    public string Hold { get; }

    /// <inheritdoc cref="Template.Insert"/>
    public string Insert { get; }

    /// <summary>
    /// Run first in every claim and outcome transaction. An inbox is a queue: its statistics describe some earlier
    /// moment (taken while every row was finished, or never taken after a bulk load), so a plan chosen from estimates
    /// is wrong exactly when a backlog arrives. Measured with statistics from a table of finished rows and 100,000
    /// new pending ones, the planner expected one pending row, read the whole ready index once per entity and sorted
    /// it: 1.5 s per statement instead of 2 ms. With these off, every lookup in the statements below can only be the
    /// ordered index scan it was written for (the indexes in sql/schema.sql), whatever the estimates say. JIT is off
    /// because a misestimated cost can also make Postgres compile a 1 ms statement for 200 ms. SET LOCAL: the
    /// settings end with the transaction, so a pooled connection never keeps them.
    /// </summary>
    public const string PinPlans = """
        set local enable_bitmapscan = off; set local enable_sort = off; set local enable_seqscan = off; set local jit = off
        """;

    /// <summary>
    /// <paramref name="statement"/> preceded by <see cref="PinPlans"/> in the same command, so pinning costs no round
    /// trip of its own (Npgsql sends the statements together; the SETs return no rows).
    /// </summary>
    public static string Pinned(string statement) => PinPlans + ";\n" + statement;

    /// <summary>The entities' watermarks. Not per table: sf_sync_state is keyed by entity type.</summary>
    public const string SyncedTimes = """
        select entity_id as EntityId, synced_time as Time
        from sf_sync_state where entity_type = @entityType and entity_id = any(@entityIds)
        """;

    /// <summary>The statements with <c>{inbox}</c> for the table name.</summary>
    public static class Template
    {
        /// <summary>
        /// Picks up to @batchSize entities to process: locks each one's head (its oldest pending row) and takes the
        /// entity's claim lock, an advisory lock on (table, entity) held until the claim transaction ends. The claim
        /// lock, not the head row lock, is what keeps two workers from claiming the same entity; see
        /// <see cref="ClaimEntities"/> for why. An entity whose claim lock another worker holds is skipped
        /// (pg_try_advisory_xact_lock does not wait).
        /// <list type="number">
        /// <item><c>w</c>: the first @window claimable rows, oldest first, straight off the ready index. "Due" is checked
        /// after the LIMIT, not as a range condition: the rows come in next_attempt_at order either way, and without
        /// the range the planner has no estimate to get wrong (on a table it has no statistics for yet, it would
        /// fetch and sort every pending row instead of reading the first few index entries).</item>
        /// <item><c>k</c>: the distinct entities among them, in that order.</item>
        /// <item><c>heads</c>, only if the window was full: every entity with a pending row, in id order (an index skip
        /// scan). A full window can consist of rows that can't be claimed yet (the tail of an entity that is in flight
        /// or backing off); this finds the entities behind them.</item>
        /// <item>Per entity, never per row: find its head, require it to be due and the entity to have no live lease,
        /// and lock the head by primary key. The conditions are on the locked row too, so they are re-checked if
        /// another worker changed it in the meantime. They are written as one CASE so that the only index that fits
        /// the locked row is its primary key: as plain conditions they also fit the ready index, and with stale
        /// statistics the planner fetched the head through it, reading every due row once per entity.</item>
        /// </list>
        /// LIMIT stops the walk as soon as the batch is full, so <c>heads</c> is normally never started. The probes are
        /// LATERAL ... LIMIT 1 subqueries rather than joins, and the transaction runs under <see cref="PinPlans"/>,
        /// which together keep the plan the same whatever the statistics say.
        /// </summary>
        public const string LockHeads = """
            with recursive
            w as materialized (
              select r.entity_id, row_number() over () as pos
              from (select i.entity_id, i.next_attempt_at from {inbox} i
                    where i.status = 'pending'
                    order by i.next_attempt_at, i.id
                    limit @window) r
              where r.next_attempt_at <= now()
            ),
            k as materialized (select w.entity_id from w group by w.entity_id order by min(w.pos)),
            heads(entity_id) as (
              (select o.entity_id from {inbox} o where o.status = 'pending' order by o.entity_id limit 1)
              union all
              select n.entity_id from heads cross join lateral (
                select o.entity_id from {inbox} o
                where o.status = 'pending' and o.entity_id > heads.entity_id
                order by o.entity_id limit 1) n
            )
            select l.entity_id
            from (select k.entity_id from k
                  union all
                  select heads.entity_id from heads
                  where (select count(*) from w) >= @window
                    and heads.entity_id not in (select k.entity_id from k)) c
            cross join lateral (
              select o.id from {inbox} o
              where o.entity_id = c.entity_id and o.status = 'pending'
              order by o.id limit 1) h
            cross join lateral (
              select x.entity_id from {inbox} x
              where x.id = h.id
                and case when x.status = 'pending' and x.next_attempt_at <= now()
                              and (x.locked_until is null or x.locked_until < now()) then true else false end
                and not exists (select 1 from {inbox} p
                                where p.entity_id = c.entity_id and p.locked_until > now())
              for update skip locked) l
            cross join lateral (
              select 1 where pg_try_advisory_xact_lock(hashtext('{inbox}'), hashtext(l.entity_id))) claim_lock
            limit @batchSize
            """;

        /// <summary>
        /// Leases the claimed entities: per entity its oldest @maxPerEntity pending rows (always including the head, so
        /// the entity counts as in flight), returned with their payloads. Bounded because an event is the full record:
        /// of a deep backlog only the newest event will be written, so leasing and returning all of it would cost a
        /// write and a payload per row for nothing. The rest is picked up by later claims and superseded by the
        /// watermark unless newer. Bumps retry_count; next_attempt_at becomes the end of the lease.
        /// <para>
        /// An entity on which another worker holds a live lease gets nothing, and so is not claimed: this check, made
        /// while holding the entity's claim lock (LockHeads), is the guarantee that one worker at a time holds an
        /// entity. Why it is safe under READ COMMITTED: a worker leases an entity only inside a transaction that holds
        /// the entity's claim lock until it commits. This statement runs after LockHeads took that lock, so its
        /// snapshot is taken after every earlier claim of the entity committed, and it sees their leases; a later
        /// claim can't take the lock before this one commits. The head row lock alone is not enough: the head is
        /// whatever row a worker's snapshot shows as oldest, and an event committed out of id order (two consumers
        /// storing events of one entity) makes it a different row for two workers. Then neither row lock conflicts,
        /// LockHeads' own lease check reads a snapshot from before the other worker's commit, and both leased the
        /// entity (ClaimRaceTests). Rows are also re-checked to still be pending when they are locked; as a CASE, like
        /// in LockHeads, because as a plain condition it let the planner read the updated rows off the ready index
        /// (with stale statistics: the whole index, once per row claimed).
        /// </para>
        /// </summary>
        public const string ClaimEntities = """
            update {inbox} u
            set lock_token = @token, locked_until = now() + @lease, next_attempt_at = now() + @lease,
                retry_count = u.retry_count + 1, last_updated = now()
            from unnest(@entityIds) as e(entity_id)
            cross join lateral (
              select o.id from {inbox} o
              where o.entity_id = e.entity_id and o.status = 'pending'
                and not exists (select 1 from {inbox} p
                                where p.entity_id = e.entity_id and p.locked_until > now()
                                  and p.lock_token is distinct from @token)
              order by o.id limit @maxPerEntity) r
            where u.id = r.id and case when u.status = 'pending' then true else false end
            returning u.id as Id, u.entity_id as EntityId, u.payload::text as Payload, u.retry_count as RetryCount
            """;

        /// <summary>
        /// Undoes ClaimEntities of earlier attempts of a retried claim whose commit outcome was unknown: no worker knows
        /// those tokens, so their rows would otherwise sit leased, unprocessed, until the lease expires. Restores
        /// retry_count. @entityIds are the entities those attempts locked: their leased rows are found through the
        /// leased index, a few rows per entity. (Nothing indexes lock_token, so looking for the tokens alone scanned
        /// every leased row, or the table.) The status check is a CASE for the same reason as in LockHeads: as a
        /// plain condition the pending and ready indexes fit too, and the planner took them.
        /// </summary>
        public const string ReleaseLostClaims = """
            update {inbox}
            set locked_until = null, lock_token = null, last_updated = now(), next_attempt_at = now(),
                retry_count = greatest(retry_count - 1, 0)
            where entity_id = any(@entityIds) and locked_until is not null and lock_token = any(@tokens)
              and case when status = 'pending' then true else false end
            """;

        /// <summary>
        /// Advances the watermarks of entities (@entityIds[i] to @times[i]), but only while this worker still holds the
        /// lease on the entity's rows, so a worker whose lease expired cannot move them. The lease check is a scalar
        /// LIMIT 1 probe of the leased index per entity (as EXISTS it can turn into a join over every leased row),
        /// in the index's own order so that it is read as an index scan.
        /// </summary>
        public const string MarkSynced = """
            insert into sf_sync_state (entity_type, entity_id, synced_time)
            select @entityType, s.entity_id, s.synced_time
            from unnest(@entityIds, @times) as s(entity_id, synced_time)
            where (select true from {inbox} l
                   where l.entity_id = s.entity_id and l.locked_until is not null and l.lock_token = @token
                   order by l.entity_id, l.locked_until limit 1)
            on conflict (entity_type, entity_id) do update
            set synced_time = greatest(sf_sync_state.synced_time, excluded.synced_time), synced_at = now()
            """;

        /// <summary>Done or superseded (@status): the row leaves the queue.</summary>
        public const string Complete = """
            update {inbox}
            set status = @status, locked_until = null, lock_token = null,
                last_updated = now(), last_error = null, last_error_code = null
            where id = any(@ids) and lock_token = @token
            """;

        /// <summary>
        /// Transient failure: retry with exponential backoff; dead-letter once retries are used up. @errors is a JSON
        /// object, row id -> [error, code], so a whole batch's failures are one statement. The rows are found by
        /// primary key like every other outcome; the object is an uncorrelated scalar subquery, which Postgres
        /// evaluates once per statement and then only looks up a key in per row. (Joined in as a one-row CTE it was
        /// copied once per row: 230 ms for 5,000 rows. Joined as arrays, the planner could drive the update from a
        /// scan for lock_token and compare every row with every id.)
        /// </summary>
        public const string Fail = """
            update {inbox} i
            set locked_until = null, lock_token = null, last_updated = now(),
                last_error = (select @errors::jsonb) -> i.id::text ->> 0,
                last_error_code = (select @errors::jsonb) -> i.id::text ->> 1,
                next_attempt_at = now() + make_interval(secs => least(@maxDelaySecs, @baseDelaySecs * power(2, i.retry_count - 1))),
                status = case when i.retry_count >= @maxRetries then 'dead' else 'pending' end
            where i.id = any(@ids) and i.lock_token = @token
            """;

        /// <summary>Permanent failure: retrying the same data can't succeed, dead-letter now. @errors as in <see cref="Fail"/>.</summary>
        public const string Dead = """
            update {inbox} i
            set locked_until = null, lock_token = null, last_updated = now(),
                last_error = (select @errors::jsonb) -> i.id::text ->> 0,
                last_error_code = (select @errors::jsonb) -> i.id::text ->> 1,
                status = 'dead'
            where i.id = any(@ids) and i.lock_token = @token
            """;

        /// <summary>Rate limited (all claimed rows): hand back without spending a retry, not before @delay.</summary>
        public const string Defer = """
            update {inbox}
            set locked_until = null, lock_token = null, last_updated = now(),
                retry_count = greatest(retry_count - 1, 0),
                next_attempt_at = now() + @delay
            where id = any(@ids) and lock_token = @token
            """;

        /// <summary>
        /// The write's outcome is unknown (all claimed rows): nobody may touch these entities until @hold has passed,
        /// by when Salesforce has applied the lost write or dropped it. The rows stay pending and leased, but under no
        /// token, so the old worker can't record anything on them either; the claim skips an entity whose head is
        /// leased, which also makes events arriving meanwhile wait. No retry is spent.
        /// </summary>
        public const string Hold = """
            update {inbox}
            set locked_until = now() + @hold, next_attempt_at = now() + @hold, lock_token = null, last_updated = now(),
                retry_count = greatest(retry_count - 1, 0), last_error = @error, last_error_code = @code
            where id = any(@ids) and lock_token = @token
            """;

        /// <summary>Not attempted (rolled back or skipped because of another entity's record): hand back for the next claim, no retry spent.</summary>
        public const string Release = """
            update {inbox}
            set locked_until = null, lock_token = null, last_updated = now(), next_attempt_at = now(),
                retry_count = greatest(retry_count - 1, 0)
            where id = any(@ids) and lock_token = @token
            """;

        /// <summary>
        /// One event. Nothing makes a row unique: a Kafka redelivery is stored again and ends superseded (its time is
        /// not newer than what was synced), which is harmless; a uniqueness rule on the payload would wrongly drop the
        /// last event of A -> B -> A.
        /// </summary>
        public const string Insert = """
            insert into {inbox} (entity_id, payload) values (@entityId, @payload::jsonb)
            """;
    }
}
