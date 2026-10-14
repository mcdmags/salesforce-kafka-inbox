using System.Collections.Concurrent;
using System.Text.RegularExpressions;

namespace SalesforceSync.Inbox;

/// <summary>
/// The statements for one inbox table (one per synced object type, all the same shape; see sql/schema.sql). No
/// statement reads inside the payload: a row's entity is its entity_id column, and which event of an entity is the
/// newest is decided in C#. A single sequential worker reads the next batch, syncs it and records the outcome, so
/// nothing is locked or leased: these statements are not safe for two workers on one inbox. Outcome statements take
/// arrays, so recording a batch costs a fixed number of round trips, not two per entity.
/// </summary>
public sealed partial class InboxSql
{
    /// <summary>
    /// How many due rows a read looks at before reducing them to entities. Larger than a batch because several rows
    /// usually belong to one entity; bounded so a read never walks a deep backlog row by row.
    /// </summary>
    public const int ReadWindow = 500;

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
        ReadBatch = Template.ReadBatch.Replace("{inbox}", table);
        MarkSynced = Template.MarkSynced.Replace("{inbox}", table);
        Fail = Template.Fail.Replace("{inbox}", table);
        Dead = Template.Dead.Replace("{inbox}", table);
    }

    public string Table { get; }

    /// <inheritdoc cref="Template.ReadBatch"/>
    public string ReadBatch { get; }

    /// <inheritdoc cref="Template.MarkSynced"/>
    public string MarkSynced { get; }

    /// <inheritdoc cref="Template.Fail"/>
    public string Fail { get; }

    /// <inheritdoc cref="Template.Dead"/>
    public string Dead { get; }

    /// <summary>
    /// Run first in every read and outcome transaction. An inbox is a queue: its statistics describe some earlier
    /// moment (taken while every row was finished, or never taken after a bulk load), so a plan chosen from estimates
    /// is wrong exactly when a backlog arrives. With these off, every lookup in the statements below can only be the
    /// ordered index scan it was written for (the indexes in sql/schema.sql), whatever the estimates say. JIT is off
    /// because a misestimated cost can also make Postgres compile a 1 ms statement for 200 ms. SET LOCAL: the
    /// settings end with the transaction, so a pooled connection never keeps them.
    /// </summary>
    public const string PinPlans = """
        set local enable_bitmapscan = off; set local enable_sort = off; set local enable_seqscan = off; set local jit = off
        """;

    /// <summary>The statements with <c>{inbox}</c> for the table name.</summary>
    public static class Template
    {
        /// <summary>
        /// The next batch, in arrival order: up to @batchSize entities, and per entity its oldest @maxPerEntity
        /// pending rows, with their payloads.
        /// <list type="number">
        /// <item><c>w</c>: the first @window due rows, oldest first, straight off the ready index, skipping every
        /// entity that has a pending row not due yet (backing off after a failure, deferred, or held after an
        /// unknown outcome): its newer events wait for it, so an entity's events are never written out of order.</item>
        /// <item><c>k</c>: the distinct entities among them, in that order, up to @batchSize.</item>
        /// <item>Per entity, its oldest pending rows (the pending index). Bounded because an event is the full record:
        /// of a deep backlog only the newest event will be written. The rest is read by later batches and superseded
        /// by the watermark unless newer.</item>
        /// </list>
        /// </summary>
        public const string ReadBatch = """
            with w as materialized (
              select i.entity_id, row_number() over () as pos
              from (select r.entity_id from {inbox} r
                    where r.status = 'pending' and r.next_attempt_at <= now()
                      and not exists (select 1 from {inbox} p
                                      where p.entity_id = r.entity_id and p.status = 'pending' and p.next_attempt_at > now())
                    order by r.next_attempt_at, r.id
                    limit @window) i
            ),
            k as materialized (select w.entity_id from w group by w.entity_id order by min(w.pos) limit @batchSize)
            select o.id as Id, o.entity_id as EntityId, o.payload::text as Payload, o.retry_count as RetryCount
            from k cross join lateral (
              select r.id, r.entity_id, r.payload, r.retry_count from {inbox} r
              where r.entity_id = k.entity_id and r.status = 'pending'
              order by r.id limit @maxPerEntity) o
            """;

        /// <summary>Advances the watermarks of entities (@entityIds[i] to @times[i]); a watermark only moves forward.</summary>
        public const string MarkSynced = """
            insert into sf_sync_state (entity_type, entity_id, synced_time)
            select @entityType, s.entity_id, s.synced_time
            from unnest(@entityIds, @times) as s(entity_id, synced_time)
            on conflict (entity_type, entity_id) do update
            set synced_time = greatest(sf_sync_state.synced_time, excluded.synced_time), synced_at = now()
            """;

        /// <summary>
        /// Transient failure: counts the attempt, retries with exponential backoff, and dead-letters once retries are
        /// used up. @errors is a JSON object, row id -> [error, code], so a whole batch's failures are one statement.
        /// The object is an uncorrelated scalar subquery, which Postgres evaluates once per statement and then only
        /// looks up a key in per row. Run again after a commit whose outcome was unknown (PostgresRetryStrategy), it
        /// counts the attempt twice: a retry is spent early, nothing else changes.
        /// </summary>
        public const string Fail = """
            update {inbox} i
            set retry_count = i.retry_count + 1, last_updated = now(),
                last_error = (select @errors::jsonb) -> i.id::text ->> 0,
                last_error_code = (select @errors::jsonb) -> i.id::text ->> 1,
                next_attempt_at = now() + make_interval(secs => least(@maxDelaySecs, @baseDelaySecs * power(2, i.retry_count))),
                status = case when i.retry_count + 1 >= @maxRetries then 'dead' else 'pending' end
            where i.id = any(@ids) and i.status = 'pending'
            """;

        /// <summary>Permanent failure: retrying the same data can't succeed, dead-letter now. @errors as in <see cref="Fail"/>.</summary>
        public const string Dead = """
            update {inbox} i
            set last_updated = now(),
                last_error = (select @errors::jsonb) -> i.id::text ->> 0,
                last_error_code = (select @errors::jsonb) -> i.id::text ->> 1,
                status = 'dead'
            where i.id = any(@ids) and i.status = 'pending'
            """;
    }
}