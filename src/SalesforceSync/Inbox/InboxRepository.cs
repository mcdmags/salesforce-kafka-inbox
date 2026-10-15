using System.Globalization;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using SalesforceSync.Data;
using SalesforceSync.Salesforce;
using SalesforceSync.Sync.Upsert;
using static SalesforceSync.Data.SqlCommands;

namespace SalesforceSync.Inbox;

/// <summary>
/// <see cref="IRepository{TEntity}"/> on Postgres, for any inbox row type: the store, claim, done and failure statements
/// and the end of a lease are the <see cref="InboxSql"/> texts of the row type's table, run through EF's raw SQL; the
/// outcomes that are a plain conditional UPDATE are LINQ (<c>ExecuteUpdate</c>). The row type's entity type (its
/// entities' key in sf_sync_state) comes from the EF model, like its table. Each unit of work runs through EF's
/// execution strategy (<see cref="PostgresRetryStrategy"/>).
/// </summary>
public sealed class InboxRepository<TEntity>(IDbContextFactory<SyncDbContext> contexts, IOptions<SyncOptions> options) : IRepository<TEntity>
    where TEntity : InboxRow, new()
{
    private readonly SyncOptions _opt = options.Value;

    /// <summary>
    /// One event, and its entity made due (<see cref="InboxSql.Template.Store"/>, one statement). Nothing makes a row
    /// unique: a Kafka redelivery is stored again and ends superseded (its time is not newer than what was synced),
    /// which is harmless; a uniqueness rule on the payload would wrongly drop the last event of A -> B -> A. Safe to
    /// retry for the same reason.
    /// </summary>
    public Task AddAsync(string entityId, string payload, CancellationToken ct)
    {
        return contexts.RunAsync((db, t) => db.Database.ExecuteAsync(InboxSql.For(db.TableOf<TEntity>()).Store, t,
                Param("entityType", db.EntityTypeOf<TEntity>()), Param("entityIds", new[] { entityId }),
                Param("payloads", new[] { payload }), Param("coalesceDelay", _opt.CoalesceDelay)), ct);
    }

    /// <summary>
    /// Locks and leases a batch of entities with their watermarks, then reads their oldest pending events, in one
    /// transaction. A fresh token per attempt: if a commit's outcome is unknown and the claim is retried, entities
    /// leased by the lost attempt are not mistaken for this one's. The retry releases them first, so they don't wait
    /// for their lease to expire (and a lease nobody works on is never taken over). The transaction ends before
    /// Salesforce is called: from then on the lease (lock_token, locked_until) protects the entities, not a lock.
    /// </summary>
    public Task<InboxClaim> ClaimAsync(CancellationToken ct)
    {
        var attempted = new List<Guid>();
        var attemptedEntityIds = new HashSet<string>(StringComparer.Ordinal);
        return contexts.RunInTransactionAsync(async (db, t) =>
        {
            var sql = InboxSql.For(db.TableOf<TEntity>());
            var type = db.EntityTypeOf<TEntity>();

            if (attemptedEntityIds.Count > 0)
            {
                var lost = attempted.ToArray();
                var lostEntityIds = attemptedEntityIds.ToArray();
                await db.SyncStates
                    .Where(s => s.EntityType == type && lostEntityIds.Contains(s.EntityId) && s.LockToken != null
                                && lost.Contains(s.LockToken.Value))
                    .ExecuteUpdateAsync(u => u
                        .SetProperty(s => s.LockedUntil, (DateTimeOffset?)null)
                        .SetProperty(s => s.LockToken, (Guid?)null), t);
            }

            var token = Guid.NewGuid();
            attempted.Add(token);
            var claimed = await db.Database.QueryAsync<ClaimedEntity>(InboxSql.ClaimEntities, t,
                Param("entityType", type), Param("token", token), Param("lease", _opt.Lease),
                Param("batchSize", Math.Min(_opt.BatchSize, ReadQueryBuilder.MaxEntitiesPerBatch)));
            if (claimed.Count == 0)
            {
                return new InboxClaim(token, [], new Dictionary<string, long>());
            }
            var entityIds = claimed.Select(c => c.EntityId).ToArray();
            attemptedEntityIds.UnionWith(entityIds);

            var events = await db.Database.QueryAsync<InboxEvent>(sql.ClaimEvents, t,
                Param("entityIds", entityIds), Param("maxPerEntity", _opt.MaxEventsPerEntity));
            // Due without a pending event (its events finished by hand, say): idle again now, not leased for nothing
            var idle = entityIds.Except(events.Select(e => e.EntityId), StringComparer.Ordinal).ToArray();
            if (idle.Length > 0)
            {
                await EndLeaseAsync(db, sql, idle, token, t);
            }
            var synced = claimed.Where(c => c.SyncedTime is not null)
                .ToDictionary(c => c.EntityId, c => c.SyncedTime!.Value, StringComparer.Ordinal);
            return new InboxClaim(token, events, synced);
        }, ct);
    }

    /// <summary>
    /// One statement per kind of outcome (not per entity), in the batch's second short transaction, then the leases
    /// end. Safe to repeat: see <see cref="InLeaseAsync"/>.
    /// </summary>
    public Task RecordAsync(InboxClaim claim, BatchOutcomes outcomes, CancellationToken ct)
    {
        var token = claim.Token;
        return InLeaseAsync(claim, async (db, sql, type, t) =>
        {
            if (outcomes.Synced.Count > 0)
            {
                await db.Database.ExecuteAsync(sql.Done, t, Param("entityType", type),
                    Param("ids", outcomes.Synced.Select(s => s.EventId).ToArray()), Param("times", TimesJson(outcomes.Synced)),
                    Param("token", token));
            }
            if (outcomes.Superseded.Count > 0)
            {
                await SupersedeAsync(db, type, [.. outcomes.Superseded], token, t);
            }
            if (outcomes.Released.Count > 0)
            {
                await ReleaseAsync(db, type, [.. outcomes.Released], token, t);
            }
            if (outcomes.Dead.Count > 0)
            {
                await db.Database.ExecuteAsync(sql.Dead, t, Param("entityType", type), Param("ids", Ids(outcomes.Dead)),
                    Param("errors", ErrorsJson(outcomes.Dead)), Param("token", token));
            }
            if (outcomes.Failed.Count > 0)
            {
                await FailAsync(db, sql, type, outcomes.Failed, token, t);
            }
            await EndLeaseAsync(db, sql, claim.EntityIds, token, t);
        }, ct);
    }

    /// <summary>
    /// The write's outcome is unknown: nobody may touch these entities until the hold has passed, by when Salesforce
    /// has applied the lost write or dropped it. The events stay pending and the entities leased, but under no token,
    /// so the old worker can't record anything on them either; events arriving meanwhile wait too. No attempt is spent.
    /// </summary>
    public Task HoldAsync(InboxClaim claim, string error, CancellationToken ct)
    {
        var hold = _opt.UnknownOutcomeHold;
        var code = UnknownOutcomeException.Code;
        var token = claim.Token;
        var entityIds = claim.EntityIds;
        return InLeaseAsync(claim, async (db, _, type, t) =>
        {
            await Held(db, type, claim.EventIds, token).ExecuteUpdateAsync(u => u
                .SetProperty(r => r.LastUpdated, _ => DateTimeOffset.UtcNow)
                .SetProperty(r => r.AttemptCount, r => Math.Max(r.AttemptCount - 1, 0))
                .SetProperty(r => r.LastError, error)
                .SetProperty(r => r.LastErrorCode, code), t);
            await db.SyncStates
                .Where(s => s.EntityType == type && entityIds.Contains(s.EntityId) && s.LockToken == token)
                .ExecuteUpdateAsync(u => u
                    .SetProperty(s => s.LockedUntil, _ => DateTimeOffset.UtcNow + hold)
                    .SetProperty(s => s.NextAttemptAt, _ => DateTimeOffset.UtcNow + hold)
                    .SetProperty(s => s.LockToken, (Guid?)null), t);
        }, ct);
    }

    public Task FailAllAsync(InboxClaim claim, string error, string? code, CancellationToken ct)
    {
        var all = claim.Events.Select(e => new FailedEvent(e.Id, error, code)).ToList();
        return InLeaseAsync(claim, async (db, sql, type, t) =>
        {
            await FailAsync(db, sql, type, all, claim.Token, t);
            await EndLeaseAsync(db, sql, claim.EntityIds, claim.Token, t);
        }, ct);
    }

    /// <summary>
    /// Records outcomes in one transaction that first locks the claim's state rows its token still holds
    /// (<see cref="InboxSql.LockHeld"/>). None: the lease was lost, or an attempt whose commit outcome was unknown
    /// already recorded everything, and nothing is written. Every statement is also conditional on the token itself.
    /// </summary>
    private Task InLeaseAsync(InboxClaim claim, Func<SyncDbContext, InboxSql, string, CancellationToken, Task> record, CancellationToken ct)
    {
        return contexts.RunInTransactionAsync(async (db, t) =>
        {
            var type = db.EntityTypeOf<TEntity>();
            var held = await db.Database.QueryAsync<string>(InboxSql.LockHeld, t, Param("entityType", type),
                Param("entityIds", claim.EntityIds), Param("token", claim.Token));
            if (held.Count > 0)
            {
                await record(db, InboxSql.For(db.TableOf<TEntity>()), type, t);
            }
        }, ct);
    }

    /// <summary>The events of a claim whose entities this lease still holds: every outcome below updates only these.</summary>
    private static IQueryable<TEntity> Held(SyncDbContext db, string type, long[] ids, Guid token)
    {
        return db.Set<TEntity>().Where(r => ids.Contains(r.Id)
            && db.SyncStates.Any(s => s.EntityType == type && s.EntityId == r.EntityId && s.LockToken == token));
    }

    /// <summary>Not written (an event of the same entity is newer, or as new was already synced): the row leaves the queue.</summary>
    private static Task<int> SupersedeAsync(SyncDbContext db, string type, long[] ids, Guid token, CancellationToken ct)
    {
        return Held(db, type, ids, token).ExecuteUpdateAsync(u => u
                .SetProperty(r => r.Status, "superseded")
                .SetProperty(r => r.LastUpdated, _ => DateTimeOffset.UtcNow)
                .SetProperty(r => r.LastError, (string?)null)
                .SetProperty(r => r.LastErrorCode, (string?)null), ct);
    }

    /// <summary>Not attempted (skipped because of another entity's record): no attempt spent.</summary>
    private static Task<int> ReleaseAsync(SyncDbContext db, string type, long[] ids, Guid token, CancellationToken ct)
    {
        return Held(db, type, ids, token).ExecuteUpdateAsync(u => u
                .SetProperty(r => r.LastUpdated, _ => DateTimeOffset.UtcNow)
                .SetProperty(r => r.AttemptCount, r => Math.Max(r.AttemptCount - 1, 0)), ct);
    }

    private Task<int> FailAsync(SyncDbContext db, InboxSql sql, string type, IReadOnlyList<FailedEvent> rows, Guid token, CancellationToken ct)
    {
        return db.Database.ExecuteAsync(sql.Fail, ct, Param("entityType", type),
                Param("ids", Ids(rows)), Param("errors", ErrorsJson(rows)), Param("token", token),
                Param("maxRetries", _opt.MaxRetries), Param("baseDelaySecs", _opt.RetryBaseDelay.TotalSeconds),
                Param("maxDelaySecs", _opt.MaxRetryDelay.TotalSeconds));
    }

    /// <summary>
    /// <see cref="InboxSql.Template.EndLease"/>: the entities are due again if they have events left, else idle.
    /// </summary>
    private static Task<int> EndLeaseAsync(SyncDbContext db, InboxSql sql, string[] entityIds, Guid token, CancellationToken ct)
    {
        return db.Database.ExecuteAsync(sql.EndLease, ct, Param("entityType", db.EntityTypeOf<TEntity>()),
            Param("entityIds", entityIds), Param("token", token));
    }

    private static long[] Ids(IReadOnlyList<FailedEvent> rows)
    {
        return [.. rows.Select(r => r.EventId)];
    }

    /// <summary>{ "row id": event time }, as InboxSql.Done takes the times.</summary>
    private static string TimesJson(IReadOnlyList<SyncedEntity> synced)
    {
        return JsonSerializer.Serialize(synced.ToDictionary(s => s.EventId.ToString(CultureInfo.InvariantCulture), s => s.EventTime));
    }

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

    /// <summary>A row of InboxSql.ClaimEntities: a claimed entity and its watermark, if anything was synced yet.</summary>
    private sealed record ClaimedEntity(string EntityId, long? SyncedTime);
}