using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;

namespace SalesforceSync.Data.Inbox;

/// <summary>
/// The inbox table: one row per entity (an account, a contact, ...) holding the newest event still to be sent to
/// Salesforce. A worker first claims a batch of rows: it marks them as taken until a time in the future (the lease),
/// so other workers leave them alone while it calls Salesforce. Afterwards it records the result (done, retry later,
/// failed for good). A result is only written to a row that is still the worker's and still holds the event it sent:
/// if a newer event arrived meanwhile, the row stays pending and the newer event is sent next.
/// More detail: README.md, "How a lease works".
/// </summary>
public sealed class InboxRepository<TRow>(IDbContextFactory<SyncDbContext> contexts, IOptions<SyncOptions> options) : IRepository<TRow>
    where TRow : InboxRow
{
    private static readonly string Table = TableOfRow();

    /// <summary>
    /// The condition every result statement adds: the row is still claimed by this worker and still holds the event
    /// it sent (same event time, same lease end as the claim returned).
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
    /// Saves an incoming event, replacing the entity's older one. If a worker is sending the entity right now, the row
    /// stays claimed by it; the new event is sent once that worker is finished.
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

    /// <summary>Takes a batch of rows to send, the longest waiting first, so no other worker takes them meanwhile.</summary>
    public Task<InboxClaim> ClaimAsync(CancellationToken ct)
    {
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
    /// Records failures: each row is tried again later (waiting longer after every failure), or given up on after
    /// MaxRetries, or at once when retrying can't help.
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
    /// Gives the batch back to be tried again after <see cref="Release.Delay"/>, without counting it as a failed try
    /// (for example when Salesforce asked us to slow down). <see cref="Release.StopInbox"/> also holds back every other
    /// row of this table.
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
    /// Records the results, then frees the claimed rows they skipped (a newer event arrived), so those are sent again
    /// right away instead of when the claim would have run out.
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

    /// <summary>Frees claimed rows, ready to send after <paramref name="delay"/>, if this worker still holds them.</summary>
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
