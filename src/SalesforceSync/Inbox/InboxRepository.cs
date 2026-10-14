using System.Globalization;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using SalesforceSync.Data;
using SalesforceSync.Salesforce;
using SalesforceSync.Sync;
using SalesforceSync.Sync.Upsert;
using static SalesforceSync.Data.SqlCommands;

namespace SalesforceSync.Inbox;

/// <summary>
/// <see cref="IRepository{TEntity}"/> on Postgres, for any inbox row type: the claim and the failure statements are the
/// <see cref="InboxSql"/> texts of the row type's table, run through EF's raw SQL; the outcomes that are a plain
/// conditional UPDATE are LINQ (<c>ExecuteUpdate</c>), one statement with the same WHERE as the SQL it replaced.
/// Each unit of work runs through EF's execution strategy (<see cref="PostgresRetryStrategy"/>).
/// </summary>
public sealed class InboxRepository<TEntity>(IDbContextFactory<SyncDbContext> contexts, IOptions<SyncOptions> options) : IRepository<TEntity>
    where TEntity : InboxRow, new()
{
    private readonly SyncOptions _opt = options.Value;

    /// <summary>
    /// One event. Nothing makes a row unique: a Kafka redelivery is stored again and ends superseded (its time is
    /// not newer than what was synced), which is harmless; a uniqueness rule on the payload would wrongly drop the
    /// last event of A -> B -> A. Safe to retry for the same reason.
    /// </summary>
    public Task AddAsync(string entityId, string payload, CancellationToken ct)
        => contexts.RunAsync(async (db, t) =>
        {
            db.Set<TEntity>().Add(new TEntity { EntityId = entityId, Payload = payload });
            await db.SaveChangesAsync(t);
        }, ct);

    /// <summary>
    /// Locks and leases a batch, and reads the claimed entities' watermarks in the same transaction. A fresh token per
    /// attempt: if a commit's outcome is unknown and the claim is retried, rows leased by the lost attempt are not
    /// mistaken for this one's. The retry releases them first, so they don't wait for their lease to expire (and a
    /// lease nobody works on is never taken over).
    /// </summary>
    public Task<InboxClaim> ClaimAsync(string entityType, CancellationToken ct)
    {
        var attempted = new List<Guid>();
        return contexts.RunInTransactionAsync(async (db, t) =>
        {
            var sql = InboxSql.For(db.TableOf<TEntity>());

            if (attempted.Count > 0)
            {
                await db.Database.ExecuteAsync(sql.ReleaseLostClaims, t, Param("tokens", attempted.ToArray()));
            }

            var token = Guid.NewGuid();
            attempted.Add(token);
            var entityIds = (await db.Database.QueryAsync<string>(sql.LockHeads, t,
                Param("window", InboxSql.ClaimWindow),
                Param("batchSize", Math.Min(_opt.BatchSize, ReadQueryBuilder.MaxEntitiesPerBatch)))).ToArray();
            if (entityIds.Length == 0)
            {
                return new InboxClaim(entityType, token, [], new Dictionary<string, long>());
            }

            var events = await db.Database.QueryAsync<InboxEvent>(sql.ClaimEntities, t,
                Param("entityIds", entityIds), Param("token", token), Param("lease", _opt.Lease),
                Param("maxPerEntity", _opt.MaxEventsPerEntity));
            var synced = await db.SyncState.AsNoTracking()
                .Where(s => s.EntityType == entityType && entityIds.Contains(s.EntityId))
                .ToDictionaryAsync(s => s.EntityId, s => s.SyncedTime, StringComparer.Ordinal, t);
            return new InboxClaim(entityType, token, events, synced);
        }, ct);
    }

    /// <summary>
    /// One statement per kind of outcome (not per entity). Safe to repeat: every statement is conditional on the
    /// lease token.
    /// </summary>
    public Task RecordAsync(InboxClaim claim, BatchOutcomes outcomes, CancellationToken ct)
    {
        var token = claim.Token;
        return contexts.RunInTransactionAsync(async (db, t) =>
        {
            var sql = InboxSql.For(db.TableOf<TEntity>());

            if (outcomes.Synced.Count > 0)
            {
                // Watermarks first: Complete releases the lease that MarkSynced checks
                await db.Database.ExecuteAsync(sql.MarkSynced, t,
                    Param("entityType", claim.EntityType), Param("token", token),
                    Param("entityIds", outcomes.Synced.Select(s => s.EntityId).ToArray()),
                    Param("times", outcomes.Synced.Select(s => s.EventTime).ToArray()));
                await CompleteAsync(db, outcomes.Synced.Select(s => s.EventId).ToArray(), token, "done", t);
            }
            if (outcomes.Superseded.Count > 0)
            {
                await CompleteAsync(db, outcomes.Superseded.ToArray(), token, "superseded", t);
            }
            if (outcomes.Released.Count > 0)
            {
                await ReleaseAsync(db, outcomes.Released.ToArray(), token, t);
            }
            if (outcomes.Dead.Count > 0)
            {
                await db.Database.ExecuteAsync(sql.Dead, t, Param("ids", Ids(outcomes.Dead)), Param("errors", ErrorsJson(outcomes.Dead)),
                    Param("token", token));
            }
            if (outcomes.Failed.Count > 0)
            {
                await FailAsync(db, sql, outcomes.Failed, token, t);
            }
            if (outcomes.Held.Count > 0)
            {
                await HoldAsync(db, Ids(outcomes.Held), token, outcomes.Held[0].Error, t);
            }
            if (outcomes.Deferred.Count > 0)
            {
                await DeferAsync(db, outcomes.Deferred.ToArray(), token, outcomes.DeferFor, t);
            }
        }, ct);
    }

    public Task HoldAsync(InboxClaim claim, string error, CancellationToken ct)
        => contexts.RunAsync((db, t) => HoldAsync(db, claim.EventIds, claim.Token, error, t), ct);

    public Task DeferAsync(InboxClaim claim, TimeSpan delay, CancellationToken ct)
        => contexts.RunAsync((db, t) => DeferAsync(db, claim.EventIds, claim.Token, delay, t), ct);

    public Task FailAllAsync(InboxClaim claim, string error, string? code, CancellationToken ct)
    {
        var all = claim.Events.Select(e => new FailedEvent(e.Id, error, code)).ToList();
        return contexts.RunAsync((db, t) => FailAsync(db, InboxSql.For(db.TableOf<TEntity>()), all, claim.Token, t), ct);
    }

    /// <summary>The rows of a claim that this lease still holds: every outcome below updates only these.</summary>
    private static IQueryable<TEntity> Leased(SyncDbContext db, long[] ids, Guid token)
        => db.Set<TEntity>().Where(r => ids.Contains(r.Id) && r.LockToken == token);

    /// <summary>Done or superseded (<paramref name="status"/>): the row leaves the queue.</summary>
    private static Task<int> CompleteAsync(SyncDbContext db, long[] ids, Guid token, string status, CancellationToken ct)
        => Leased(db, ids, token).ExecuteUpdateAsync(u => u
            .SetProperty(r => r.Status, status)
            .SetProperty(r => r.LockedUntil, (DateTimeOffset?)null)
            .SetProperty(r => r.LockToken, (Guid?)null)
            .SetProperty(r => r.LastUpdated, _ => DateTimeOffset.UtcNow)
            .SetProperty(r => r.LastError, (string?)null)
            .SetProperty(r => r.LastErrorCode, (string?)null), ct);

    /// <summary>Not attempted (rolled back or skipped because of another entity's record): hand back for the next claim, no retry spent.</summary>
    private static Task<int> ReleaseAsync(SyncDbContext db, long[] ids, Guid token, CancellationToken ct)
        => Leased(db, ids, token).ExecuteUpdateAsync(u => u
            .SetProperty(r => r.LockedUntil, (DateTimeOffset?)null)
            .SetProperty(r => r.LockToken, (Guid?)null)
            .SetProperty(r => r.LastUpdated, _ => DateTimeOffset.UtcNow)
            .SetProperty(r => r.NextAttemptAt, _ => DateTimeOffset.UtcNow)
            .SetProperty(r => r.RetryCount, r => Math.Max(r.RetryCount - 1, 0)), ct);

    /// <summary>Rate limited: hand back without spending a retry, not before <paramref name="delay"/>.</summary>
    private static Task<int> DeferAsync(SyncDbContext db, long[] ids, Guid token, TimeSpan delay, CancellationToken ct)
        => Leased(db, ids, token).ExecuteUpdateAsync(u => u
            .SetProperty(r => r.LockedUntil, (DateTimeOffset?)null)
            .SetProperty(r => r.LockToken, (Guid?)null)
            .SetProperty(r => r.LastUpdated, _ => DateTimeOffset.UtcNow)
            .SetProperty(r => r.RetryCount, r => Math.Max(r.RetryCount - 1, 0))
            .SetProperty(r => r.NextAttemptAt, _ => DateTimeOffset.UtcNow + delay), ct);

    /// <summary>
    /// The write's outcome is unknown: nobody may touch these entities until the hold has passed, by when Salesforce
    /// has applied the lost write or dropped it. The rows stay pending and leased, but under no token, so the old
    /// worker can't record anything on them either; the claim skips an entity whose head is leased, which also makes
    /// events arriving meanwhile wait. No retry is spent.
    /// </summary>
    private Task<int> HoldAsync(SyncDbContext db, long[] ids, Guid token, string error, CancellationToken ct)
    {
        var hold = _opt.UnknownOutcomeHold;
        var code = UnknownOutcomeException.Code;
        return Leased(db, ids, token).ExecuteUpdateAsync(u => u
            .SetProperty(r => r.LockedUntil, _ => DateTimeOffset.UtcNow + hold)
            .SetProperty(r => r.NextAttemptAt, _ => DateTimeOffset.UtcNow + hold)
            .SetProperty(r => r.LockToken, (Guid?)null)
            .SetProperty(r => r.LastUpdated, _ => DateTimeOffset.UtcNow)
            .SetProperty(r => r.RetryCount, r => Math.Max(r.RetryCount - 1, 0))
            .SetProperty(r => r.LastError, error)
            .SetProperty(r => r.LastErrorCode, code), ct);
    }

    private Task<int> FailAsync(SyncDbContext db, InboxSql sql, IReadOnlyList<FailedEvent> rows, Guid token, CancellationToken ct)
        => db.Database.ExecuteAsync(sql.Fail, ct,
            Param("ids", Ids(rows)), Param("errors", ErrorsJson(rows)), Param("token", token),
            Param("maxRetries", _opt.MaxRetries), Param("baseDelaySecs", _opt.RetryBaseDelay.TotalSeconds),
            Param("maxDelaySecs", _opt.MaxRetryDelay.TotalSeconds));

    private static long[] Ids(IReadOnlyList<FailedEvent> rows) => rows.Select(r => r.EventId).ToArray();

    /// <summary>{ "row id": [error, code] }, as InboxSql.Fail / Dead take the errors.</summary>
    private static string ErrorsJson(IReadOnlyList<FailedEvent> rows)
    {
        var errors = new Dictionary<string, string?[]>(StringComparer.Ordinal);
        foreach (var row in rows)
        {
            errors[row.EventId.ToString(CultureInfo.InvariantCulture)] = [row.Error, row.Code];
        }
        return JsonSerializer.Serialize(errors);
    }
}
