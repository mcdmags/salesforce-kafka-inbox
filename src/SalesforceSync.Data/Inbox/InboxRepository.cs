using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;

namespace SalesforceSync.Data.Inbox;

/// <summary>
/// <see cref="IRepository{TRow}"/> on Postgres, raw SQL through EF.
/// <para>
/// <b>Claim = receipt.</b> <see cref="ClaimAsync"/> leases due rows (next_attempt_at = lease end), returns per row: id,
/// event_time read, lease end set (<see cref="InboxEvent"/>). Every later statement of the batch
/// (<see cref="MarkDoneAsync"/>, <see cref="MarkFailedAsync"/>, <see cref="ReleaseAsync"/>, <see cref="EndLeasesAsync"/>)
/// sends receipt back for its claimed ids (<see cref="InboxClaim.Of"/>: only claimed ids, values from claim, never
/// re-read). Postgres applies it only where row still matches (<see cref="StillClaimed"/>). Why: Salesforce calls run
/// between claim and outcome, no transaction, nothing holds row; receipt tells "still ours" from "changed".
/// </para>
/// <para>
/// <b>What changes a leased row:</b>
/// <list type="bullet">
/// <item>Newer event stored (new event_time, attempts reset). Store keeps next_attempt_at (lease end): now would let
/// 2nd worker claim mid-write. Outcome skips row (event_time differs): newer event never marked done unsent, old
/// failure not counted on it. Skipped row's lease ended (lease end still matches): newer event due now, not at expiry.
/// After <see cref="ReleaseAsync"/>: due after release delay (hold must not race write Salesforce may still apply).</item>
/// <item>Lease ran out, other worker claimed (new lease end). Our outcomes, lease ends match nothing: late worker can't
/// record on or end other's lease. Rare: Salesforce call cancelled at 80% of lease.</item>
/// <item>Same statement ran already (reply lost, unit retried). 1st run replaced lease end (done: null, failed:
/// backoff): rerun matches nothing, failure counted once.</item>
/// </list>
/// Store + outcome same row same moment: row lock orders them; READ COMMITTED re-checks outcome WHERE on row store left.
/// </para>
/// <para>
/// <b>No transactions:</b> each statement commits alone. Claim 1 round trip not 3 (BEGIN, statement, COMMIT), rows leased
/// shorter (streaming 8 workers x 3 pods: 4.6 s vs 6.4 s). Safe: receipt makes every statement OK to run twice or late.
/// Accepted cost: lost claim commit, or crash between outcome statements, leaves rows leased till expiry (delayed, not
/// lost).
/// </para>
/// <para>
/// <b>Lease end, not lock_token column:</b> during lease only a claim changes next_attempt_at, 2 claims never same
/// microsecond: already identifies lease, no extra column.
/// </para>
/// <para>
/// <b>Key lookups only</b>, never join back to table: big backlog stats turned that into hash join over all rows. Hence
/// entity_id = ANY(@entityIds) beside receipt's unnest.
/// </para>
/// </summary>
public sealed class InboxRepository<TRow>(IDbContextFactory<SyncDbContext> contexts, IOptions<SyncOptions> options) : IRepository<TRow>
    where TRow : InboxRow
{
    private static readonly string Table = TableOfRow();

    /// <summary>
    /// WHERE of every outcome: claimed rows still under this claim's lease (@leaseEnds) and still holding the claimed
    /// event (@times). Row overwritten by newer event gets no outcome (else marked done unsent);
    /// <see cref="EndLeasesAsync"/> makes it due. Lease match: an outcome retried after its commit was lost, or run after
    /// the lease expired and another worker claimed the row, changes nothing (no double attempt count, no ended lease).
    /// <para>
    /// No race with store: same row, row lock orders them. Store first: event_time differs, no match (READ COMMITTED
    /// re-checks). Store second: overwrites finished row, pending again.
    /// </para>
    /// </summary>
    private const string StillClaimed = """
        i.entity_id = ANY(@entityIds)
          AND EXISTS (SELECT 1 FROM unnest(@entityIds, @times, @leaseEnds) AS c(entity_id, event_time, lease_end)
                      WHERE c.entity_id = i.entity_id AND c.event_time = i.event_time AND c.lease_end = i.next_attempt_at)
        """;

    private readonly SyncOptions _opt = options.Value;

    private static string TableOfRow()
    {
        using var db = SyncDbContext.ForModel();
        return db.TableOf<TRow>();
    }

    /// <summary>
    /// Overwrite if event newer. Same time (Kafka redelivery) overwrites too, except done row (already in Salesforce).
    /// Pending row keeps next_attempt_at: if leased that is lease end, moving to now lets second worker claim.
    /// Lease holder's outcome makes it due (<see cref="StillClaimed"/>).
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
    /// Lease longest-waiting due rows: next_attempt_at = lease end. SKIP LOCKED: workers claim side by side. READ
    /// COMMITTED re-checks row leased after our snapshot, drops it: no double lease (ClaimRaceTests). Ids into array,
    /// not CTE joined back.
    /// </summary>
    public Task<InboxClaim> ClaimAsync(CancellationToken ct)
    {
        // One statement, atomic alone. Lost commit: its rows wait lease expiry
        return contexts.RunAsync(async (db, t) =>
        {
            var sql = $"""
                UPDATE {Table} i
                SET next_attempt_at = now() + @lease, last_updated = now()
                WHERE i.entity_id = ANY(ARRAY(
                  SELECT d.entity_id FROM {Table} d
                  WHERE d.next_attempt_at <= now()
                  ORDER BY d.next_attempt_at
                  LIMIT @batchSize
                  FOR UPDATE SKIP LOCKED))
                RETURNING i.entity_id AS EntityId, i.payload::text AS Payload, i.event_time AS EventTime, i.next_attempt_at AS LeaseEnd
                """;
            var events = await db.Database.QueryAsync<InboxEvent>(sql, t, new
            {
                lease = _opt.Lease,
                batchSize = _opt.BatchSize
            });
            return new InboxClaim(events);
        }, ct);
    }

    public Task MarkDoneAsync(InboxClaim claim, IReadOnlyList<string> entityIds, CancellationToken ct)
    {
        if (entityIds.Count == 0)
        {
            return Task.CompletedTask;
        }

        var sql = $"""
            UPDATE {Table} i
            SET last_updated = now(), last_error = NULL, next_attempt_at = NULL
            WHERE {StillClaimed}
            RETURNING i.entity_id
            """;
        var rows = claim.Of(entityIds);
        return WithLeasesEndedAsync(rows, TimeSpan.Zero, (db, t) => db.Database.QueryAsync<string>(sql, t, new
        {
            entityIds = entityIds.ToArray(),
            times = rows.Select(r => r.EventTime).ToArray(),
            leaseEnds = rows.Select(r => r.LeaseEnd).ToArray()
        }), ct);
    }

    /// <summary>
    /// Exponential backoff, or dead letter (idle with error) at the MaxRetries-th failure, or at once if permanent
    /// (@deadIds). @errors: one JSON id -> error, so a whole batch is one statement. Scalar subquery parses it once; as
    /// a CTE it was re-read per row (230 ms / 5,000).
    /// </summary>
    public Task MarkFailedAsync(InboxClaim claim, IReadOnlyList<FailedEvent> failures, CancellationToken ct)
    {
        if (failures.Count == 0)
        {
            return Task.CompletedTask;
        }

        var sql = $"""
            UPDATE {Table} i
            SET last_updated = now(), attempt_count = i.attempt_count + 1,
                last_error = (SELECT @errors::jsonb) ->> i.entity_id,
                next_attempt_at = CASE WHEN i.attempt_count + 1 < @maxRetries AND i.entity_id <> ALL(@deadIds)
                                       THEN now() + make_interval(secs => least(@maxDelaySecs, @baseDelaySecs * power(2, i.attempt_count))) END
            WHERE {StillClaimed}
            RETURNING i.entity_id
            """;
        var rows = claim.Of(failures.Select(f => f.EntityId));
        return WithLeasesEndedAsync(rows, TimeSpan.Zero, (db, t) => db.Database.QueryAsync<string>(sql, t, new
        {
            entityIds = rows.Select(r => r.EntityId).ToArray(),
            times = rows.Select(r => r.EventTime).ToArray(),
            leaseEnds = rows.Select(r => r.LeaseEnd).ToArray(),
            errors = ErrorsJson(failures),
            deadIds = failures.Where(f => f.Permanent).Select(f => f.EntityId).ToArray(),
            maxRetries = _opt.MaxRetries,
            baseDelaySecs = _opt.RetryBaseDelay.TotalSeconds,
            maxDelaySecs = _opt.MaxRetryDelay.TotalSeconds
        }), ct);
    }

    /// <summary>
    /// Due again after <see cref="Release.Delay"/>, no attempt spent. Unknown write outcome holds entities till
    /// Salesforce surely applied or dropped it; new events wait too (store keeps pending due time).
    /// <see cref="Release.StopInbox"/> also delays all other due rows: mapping error fails them all, waiting loses
    /// nothing, retrying dead-letters.
    /// </summary>
    public Task ReleaseAsync(InboxClaim claim, Release release, CancellationToken ct)
    {
        return WithLeasesEndedAsync([.. claim.Events], release.Delay, async (db, t) =>
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
                leaseEnds = claim.LeaseEnds,
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
    /// Run <paramref name="record"/> on <paramref name="rows"/>, then end the lease of those it skipped (a newer event
    /// overwrote them): due after <paramref name="delay"/>, not at lease expiry.
    /// </summary>
    private Task WithLeasesEndedAsync(IReadOnlyList<InboxEvent> rows, TimeSpan delay,
        Func<SyncDbContext, CancellationToken, Task<List<string>>> record, CancellationToken ct)
    {
        return contexts.RunAsync(async (db, t) =>
        {
            var recorded = (await record(db, t)).ToHashSet(StringComparer.Ordinal);
            if (recorded.Count < rows.Count)
            {
                await EndLeasesAsync(db, t, [.. rows.Where(r => !recorded.Contains(r.EntityId))], delay);
            }
        }, ct);
    }

    /// <summary>
    /// Caller passes the rows outcomes did not return. Not last_updated = now(): store in same microsecond would look
    /// recorded, stay leased till expiry. No event_time match: an overwritten row must end its lease too (a store keeps
    /// the lease end); the lease end match leaves a row another worker claimed since alone.
    /// </summary>
    private static Task<int> EndLeasesAsync(SyncDbContext db, CancellationToken ct, IReadOnlyList<InboxEvent> rows, TimeSpan delay)
    {
        var sql = $"""
            UPDATE {Table} i
            SET next_attempt_at = now() + @delay, last_updated = now()
            WHERE i.entity_id = ANY(@entityIds)
              AND EXISTS (SELECT 1 FROM unnest(@entityIds, @leaseEnds) AS c(entity_id, lease_end)
                          WHERE c.entity_id = i.entity_id AND c.lease_end = i.next_attempt_at)
            """;
        return db.Database.ExecuteAsync(sql, ct, new
        {
            entityIds = rows.Select(r => r.EntityId).ToArray(),
            leaseEnds = rows.Select(r => r.LeaseEnd).ToArray(),
            delay
        });
    }

    private static string ErrorsJson(IReadOnlyList<FailedEvent> rows)
    {
        return JsonSerializer.Serialize(rows.ToDictionary(r => r.EntityId, r => r.Error));
    }
}
