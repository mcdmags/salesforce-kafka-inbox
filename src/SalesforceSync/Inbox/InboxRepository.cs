using System.Globalization;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using SalesforceSync.Data;
using SalesforceSync.Salesforce;
using SalesforceSync.Sync;
using SalesforceSync.Sync.Upsert;

namespace SalesforceSync.Inbox;

/// <summary>
/// <see cref="IRepository{TEntity}"/> on Postgres, for any inbox row type: the store, claim and outcome statements and
/// the end of a lease are the <see cref="InboxSql"/> texts of the row type's table, run through EF's raw SQL; releasing
/// a lost claim's leases is LINQ (<c>ExecuteUpdate</c>). The table comes from the EF model. Each unit of work runs
/// through EF's execution strategy (<see cref="PostgresRetryStrategy"/>).
/// </summary>
public sealed class InboxRepository<TEntity>(IDbContextFactory<SyncDbContext> contexts, IOptions<SyncOptions> options) : IRepository<TEntity>
    where TEntity : InboxRow, IInboxRow, new()
{
    private readonly SyncOptions _opt = options.Value;

    /// <summary>
    /// One event into its entity's row unless that holds a newer one, and the entity made due
    /// (<see cref="InboxSql.Template.Store"/>, one statement). A Kafka redelivery overwrites the row with the same event
    /// and ends superseded (its time is not newer than what was synced), which is harmless; the last event of A -> B ->
    /// A is newer than B and is written. Safe to retry for the same reason. The time is stored cut to the microsecond as
    /// the engine cuts it (<see cref="PreparedBatch.ToWholeMicroseconds"/>), so the watermark InboxSql.Done takes from it
    /// is the time the engine compares the entity's later events with.
    /// </summary>
    public Task AddAsync(string entityId, DateTimeOffset eventTime, string payload, CancellationToken ct)
    {
        var time = PreparedBatch.ToWholeMicroseconds(eventTime);
        return contexts.RunAsync((db, t) => db.Database.ExecuteAsync(InboxSql.For(db.TableOf<TEntity>()).Store, t,
                new { entityId, eventTime = time, payload, coalesceDelay = _opt.CoalesceDelay }), ct);
    }

    /// <summary>
    /// Locks and leases a batch of due rows and reads them with their watermarks (<see cref="InboxSql.Template.Claim"/>),
    /// in a transaction of its own, so the rows are read before the commit is sent. A fresh token per attempt: if a
    /// commit's outcome is unknown and the claim is retried, rows leased by the lost attempt are not mistaken for this
    /// one's. The retry releases them first, so they don't wait for their lease to expire (and a lease nobody works on
    /// is never taken over). The transaction ends before Salesforce is called: from then on the lease (lock_token,
    /// locked_until) protects the entities, not a lock.
    /// </summary>
    public Task<InboxClaim> ClaimAsync(CancellationToken ct)
    {
        var attempted = new List<Guid>();
        var attemptedRowIds = new HashSet<long>();
        return contexts.RunInTransactionAsync(async (db, t) =>
        {
            if (attemptedRowIds.Count > 0)
            {
                var lost = attempted.ToArray();
                var lostRowIds = attemptedRowIds.ToArray();
                await db.Set<TEntity>()
                    .Where(r => lostRowIds.Contains(r.Id) && r.LockToken != null && lost.Contains(r.LockToken.Value))
                    .ExecuteUpdateAsync(u => u
                        .SetProperty(r => r.LockedUntil, (DateTimeOffset?)null)
                        .SetProperty(r => r.LockToken, (Guid?)null), t);
            }

            var token = Guid.NewGuid();
            attempted.Add(token);
            var events = await db.Database.QueryAsync<InboxEvent>(InboxSql.For(db.TableOf<TEntity>()).Claim, t, new
            {
                token,
                lease = _opt.Lease,
                batchSize = Math.Min(_opt.BatchSize, ReadQueryBuilder.MaxEntitiesPerBatch)
            });
            attemptedRowIds.UnionWith(events.Select(e => e.RowId));
            return new InboxClaim(token, events);
        }, ct);
    }

    /// <summary>
    /// One statement per kind of outcome (not per entity), in the batch's second short transaction, then the leases
    /// end. Safe to repeat: see <see cref="InTransactionAsync"/>.
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
                    new { ids = outcomes.Synced.ToArray(), times = claim.TimesOf(outcomes.Synced), token });
            }
            if (outcomes.Superseded.Count > 0)
            {
                recorded += await db.Database.ExecuteAsync(sql.Supersede, t,
                    new { ids = outcomes.Superseded.ToArray(), times = claim.TimesOf(outcomes.Superseded), token });
            }
            if (outcomes.Released.Count > 0)
            {
                recorded += await ReleaseAsync(db, sql, claim, outcomes.Released, t);
            }
            if (outcomes.Dead.Count > 0)
            {
                var ids = Ids(outcomes.Dead);
                recorded += await db.Database.ExecuteAsync(sql.Dead, t,
                    new { ids, times = claim.TimesOf(ids), errors = ErrorsJson(outcomes.Dead), token });
            }
            if (outcomes.Failed.Count > 0)
            {
                recorded += await FailAsync(db, sql, claim, outcomes.Failed, t);
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
            ids = claim.RowIds,
            times = claim.EventTimes,
            token = claim.Token,
            error,
            code = UnknownOutcomeException.Code,
            hold
        }), ct);
    }

    public Task FailAllAsync(InboxClaim claim, string error, string? code, CancellationToken ct)
    {
        return InTransactionAsync(claim, TimeSpan.Zero,
            (db, sql, t) => FailAsync(db, sql, claim, [.. claim.RowIds.Select(id => new FailedEvent(id, error, code))], t), ct);
    }

    /// <summary>
    /// Records outcomes in one transaction: <paramref name="record"/> runs the outcome statements, each of which updates
    /// only the claimed rows this lease still holds that still hold the event the claim read
    /// (<see cref="InboxSql.Template.StillClaimed"/>), and returns how many rows they updated. If that is fewer than the claim's,
    /// <see cref="InboxSql.Template.EndLease"/> ends the lease on the rest (rows a newer event overwrote meanwhile),
    /// due after <paramref name="delay"/> (the hold of a held claim, else zero). Safe to repeat: an attempt whose commit outcome was unknown, or a lease lost
    /// meanwhile, leaves no row under this token, and nothing is written.
    /// </summary>
    private Task InTransactionAsync(InboxClaim claim, TimeSpan delay, Func<SyncDbContext, InboxSql, CancellationToken, Task<int>> record,
        CancellationToken ct)
    {
        return contexts.RunInTransactionAsync(async (db, t) =>
        {
            var sql = InboxSql.For(db.TableOf<TEntity>());
            if (await record(db, sql, t) < claim.Events.Count)
            {
                await db.Database.ExecuteAsync(sql.EndLease, t, new { ids = claim.RowIds, token = claim.Token, delay });
            }
        }, ct);
    }

    /// <summary>Not attempted (skipped because of another entity's record): no attempt spent.</summary>
    private static Task<int> ReleaseAsync(SyncDbContext db, InboxSql sql, InboxClaim claim, IReadOnlyList<long> ids, CancellationToken ct)
    {
        return db.Database.ExecuteAsync(sql.Release, ct, new { ids = ids.ToArray(), times = claim.TimesOf(ids), token = claim.Token });
    }

    private Task<int> FailAsync(SyncDbContext db, InboxSql sql, InboxClaim claim, IReadOnlyList<FailedEvent> rows, CancellationToken ct)
    {
        var ids = Ids(rows);
        return db.Database.ExecuteAsync(sql.Fail, ct, new
        {
            ids,
            times = claim.TimesOf(ids),
            errors = ErrorsJson(rows),
            token = claim.Token,
            maxRetries = _opt.MaxRetries,
            baseDelaySecs = _opt.RetryBaseDelay.TotalSeconds,
            maxDelaySecs = _opt.MaxRetryDelay.TotalSeconds
        });
    }

    private static long[] Ids(IReadOnlyList<FailedEvent> rows)
    {
        return [.. rows.Select(r => r.RowId)];
    }

    /// <summary>{ "row id": [error, code] }, as InboxSql.Fail / Dead take the errors.</summary>
    private static string ErrorsJson(IReadOnlyList<FailedEvent> rows)
    {
        var errors = new Dictionary<string, string?[]>(StringComparer.Ordinal);
        foreach (var row in rows)
        {
            errors[row.RowId.ToString(CultureInfo.InvariantCulture)] = [row.Error, row.Code];
        }
        return JsonSerializer.Serialize(errors);
    }
}