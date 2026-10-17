using System.Globalization;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using SalesforceSync.Data;
using SalesforceSync.Salesforce;
using SalesforceSync.Sync.Upsert;

namespace SalesforceSync.Inbox;

/// <summary>
/// <see cref="IRepository{TEntity}"/> on Postgres, for any inbox row type: the store, claim and outcome statements and
/// the end of a lease are the <see cref="InboxSql"/> texts of the row type's table, run through EF's raw SQL. The table
/// comes from the EF model. Each unit of work runs through EF's execution strategy (<see cref="PostgresRetryStrategy"/>).
/// </summary>
public sealed class InboxRepository<TEntity>(IDbContextFactory<SyncDbContext> contexts, IOptions<SyncOptions> options) : IRepository<TEntity>
    where TEntity : InboxRow, IInboxRow, new()
{
    private readonly SyncOptions _opt = options.Value;

    /// <summary>
    /// One event into its entity's row unless that holds a newer one, and the entity made due
    /// (<see cref="InboxSql.Template.Store"/>, one statement). A Kafka redelivery of a synced event changes nothing, and
    /// one of an event not yet synced overwrites the row with itself, which is harmless; the last event of A -> B -> A
    /// is newer than B and is written. Safe to retry for the same reason. The time is stored cut to the microsecond
    /// (<see cref="InboxRow.ToWholeMicroseconds"/>).
    /// </summary>
    public Task AddAsync(string entityId, DateTimeOffset eventTime, string payload, CancellationToken ct)
    {
        var time = InboxRow.ToWholeMicroseconds(eventTime);
        return contexts.RunAsync((db, t) => db.Database.ExecuteAsync(InboxSql.For(db.TableOf<TEntity>()).Store, t,
                new { entityId, eventTime = time, payload }), ct);
    }

    /// <summary>
    /// Locks and leases a batch of due rows and reads them (<see cref="InboxSql.Template.Claim"/>), in a transaction of
    /// its own, so the rows are read before the commit is sent. The transaction ends before Salesforce is called: from
    /// then on the lease (lock_token, and next_attempt_at as its end) protects the entities, not a lock.
    /// <para>
    /// Each attempt leases under a fresh token. If a commit's outcome is unknown and the claim is retried, the attempts
    /// before may have leased rows nobody works on: the retry first ends those leases, by the lost tokens and the rows
    /// they read, in one statement (<see cref="InboxSql.Template.EndLease"/>), so the rows are due at once, not when the
    /// leases expire, and are not mistaken for this attempt's.
    /// </para>
    /// </summary>
    public Task<InboxClaim> ClaimAsync(CancellationToken ct)
    {
        // What the attempts so far leased: if this runs again, their commits were lost
        var lostTokens = new List<Guid>();
        var lostRowIds = new List<long>();
        return contexts.RunInTransactionAsync(async (db, t) =>
        {
            var sql = InboxSql.For(db.TableOf<TEntity>());
            if (lostRowIds.Count > 0)
            {
                await db.Database.ExecuteAsync(sql.EndLease, t,
                    new { ids = lostRowIds.ToArray(), tokens = lostTokens.ToArray(), delay = TimeSpan.Zero });
            }

            var token = Guid.NewGuid();
            var events = await db.Database.QueryAsync<InboxEvent>(sql.Claim, t, new
            {
                token,
                lease = _opt.Lease,
                batchSize = Math.Min(_opt.BatchSize, ReadQueryBuilder.MaxEntitiesPerBatch)
            });
            lostTokens.Add(token);
            lostRowIds.AddRange(events.Select(e => e.RowId));
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
                    new { ids = outcomes.Synced.ToArray(), times = claim.TimesOf(outcomes.Synced), token });
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
            ids = claim.RowIds,
            times = claim.EventTimes,
            token = claim.Token,
            error,
            code = UnknownOutcomeException.Code,
            hold
        }), ct);
    }

    /// <summary>
    /// Rate limited: hands the entities back, not before <paramref name="delay"/>, without spending an attempt: only
    /// their leases end (<see cref="InboxSql.Template.EndLease"/>).
    /// </summary>
    public Task DeferAsync(InboxClaim claim, TimeSpan delay, CancellationToken ct)
    {
        return contexts.RunAsync((db, t) => db.Database.ExecuteAsync(InboxSql.For(db.TableOf<TEntity>()).EndLease, t,
            new { ids = claim.RowIds, tokens = new[] { claim.Token }, delay }), ct);
    }

    public Task FailAllAsync(InboxClaim claim, string error, string? code, CancellationToken ct)
    {
        FailedEvent[] rows = [.. claim.RowIds.Select(id => new FailedEvent(id, error, code))];
        return InTransactionAsync(claim, TimeSpan.Zero, (db, sql, t) => FailAsync(db, sql, claim, rows, _opt.MaxRetries, t), ct);
    }

    /// <summary>
    /// Records outcomes in one transaction: <paramref name="record"/> runs the outcome statements, each of which updates
    /// only the claimed rows this lease still holds that still hold the event the claim read
    /// (<see cref="InboxSql.Template.StillClaimed"/>), and returns how many rows they updated. If that is fewer than the claim's,
    /// <see cref="InboxSql.Template.EndLease"/> ends the lease on the rest (rows not attempted, and rows a newer event
    /// overwrote meanwhile), due after <paramref name="delay"/>. Safe to repeat: an attempt whose commit outcome was
    /// unknown, or a lease lost meanwhile, leaves no row under this token, and nothing is written.
    /// </summary>
    private Task InTransactionAsync(InboxClaim claim, TimeSpan delay, Func<SyncDbContext, InboxSql, CancellationToken, Task<int>> record,
        CancellationToken ct)
    {
        return contexts.RunInTransactionAsync(async (db, t) =>
        {
            var sql = InboxSql.For(db.TableOf<TEntity>());
            if (await record(db, sql, t) < claim.Events.Count)
            {
                await db.Database.ExecuteAsync(sql.EndLease, t, new { ids = claim.RowIds, tokens = new[] { claim.Token }, delay });
            }
        }, ct);
    }

    /// <summary>The rows failed: retried with backoff, or dead-lettered at the <paramref name="maxRetries"/>-th failure (0: now).</summary>
    private Task<int> FailAsync(SyncDbContext db, InboxSql sql, InboxClaim claim, IReadOnlyList<FailedEvent> rows, int maxRetries,
        CancellationToken ct)
    {
        long[] ids = [.. rows.Select(r => r.RowId)];
        return db.Database.ExecuteAsync(sql.Fail, ct, new
        {
            ids,
            times = claim.TimesOf(ids),
            errors = ErrorsJson(rows),
            token = claim.Token,
            maxRetries,
            baseDelaySecs = _opt.RetryBaseDelay.TotalSeconds,
            maxDelaySecs = _opt.MaxRetryDelay.TotalSeconds
        });
    }

    /// <summary>{ "row id": [error, code] }, as InboxSql.Fail takes the errors.</summary>
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