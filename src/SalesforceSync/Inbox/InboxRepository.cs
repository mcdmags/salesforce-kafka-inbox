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
/// <see cref="IRepository{TEntity}"/> on Postgres, for any inbox row type: the read and the failure statements are the
/// <see cref="InboxSql"/> texts of the row type's table, run through EF's raw SQL; the outcomes that are a plain
/// conditional UPDATE are LINQ (<c>ExecuteUpdate</c>). Each unit of work runs through EF's execution strategy
/// (<see cref="PostgresRetryStrategy"/>). Written for one sequential worker per inbox: nothing is locked or leased.
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
    {
        return contexts.RunAsync(async (db, t) =>
            {
                db.Set<TEntity>().Add(new TEntity { EntityId = entityId, Payload = payload });
                await db.SaveChangesAsync(t);
            }, ct);
    }

    /// <summary>Reads the next batch and the batch's entities' watermarks in one transaction. Changes nothing.</summary>
    public Task<InboxBatch> ReadBatchAsync(string entityType, CancellationToken ct)
    {
        return contexts.RunInTransactionAsync(async (db, t) =>
        {
            await db.Database.ExecuteAsync(InboxSql.PinPlans, t);
            var events = await db.Database.QueryAsync<InboxEvent>(InboxSql.For(db.TableOf<TEntity>()).ReadBatch, t,
                Param("window", InboxSql.ReadWindow),
                Param("batchSize", Math.Min(_opt.BatchSize, ReadQueryBuilder.MaxEntitiesPerBatch)),
                Param("maxPerEntity", _opt.MaxEventsPerEntity));
            if (events.Count == 0)
            {
                return new InboxBatch(entityType, [], new Dictionary<string, long>());
            }

            var entityIds = events.Select(e => e.EntityId).Distinct(StringComparer.Ordinal).ToArray();
            var synced = await db.SyncState.AsNoTracking()
                .Where(s => s.EntityType == entityType && entityIds.Contains(s.EntityId))
                .ToDictionaryAsync(s => s.EntityId, s => s.SyncedTime, StringComparer.Ordinal, t);
            return new InboxBatch(entityType, events, synced);
        }, ct);
    }

    /// <summary>One statement per kind of outcome (not per entity), in one transaction.</summary>
    public Task RecordAsync(InboxBatch batch, BatchOutcomes outcomes, CancellationToken ct)
    {
        return contexts.RunInTransactionAsync(async (db, t) =>
        {
            var sql = InboxSql.For(db.TableOf<TEntity>());

            await db.Database.ExecuteAsync(InboxSql.PinPlans, t);
            if (outcomes.Synced.Count > 0)
            {
                await db.Database.ExecuteAsync(sql.MarkSynced, t,
                    Param("entityType", batch.EntityType),
                    Param("entityIds", outcomes.Synced.Select(s => s.EntityId).ToArray()),
                    Param("times", outcomes.Synced.Select(s => s.EventTime).ToArray()));
                await CompleteAsync(db, [.. outcomes.Synced.Select(s => s.EventId)], "done", t);
            }
            if (outcomes.Superseded.Count > 0)
            {
                await CompleteAsync(db, [.. outcomes.Superseded], "superseded", t);
            }
            if (outcomes.Dead.Count > 0)
            {
                await db.Database.ExecuteAsync(sql.Dead, t, Param("ids", Ids(outcomes.Dead)), Param("errors", ErrorsJson(outcomes.Dead)));
            }
            if (outcomes.Failed.Count > 0)
            {
                await FailAsync(db, sql, outcomes.Failed, t);
            }
        }, ct);
    }

    /// <summary>
    /// The write's outcome is unknown: nothing of these entities is read again until the hold has passed, by when
    /// Salesforce has applied the lost write or dropped it. Their newer events wait too (see <see cref="InboxSql.Template.ReadBatch"/>).
    /// No retry is spent.
    /// </summary>
    public Task HoldAsync(InboxBatch batch, string error, CancellationToken ct)
    {
        var hold = _opt.UnknownOutcomeHold;
        var code = UnknownOutcomeException.Code;
        return contexts.RunAsync((db, t) => Pending(db, batch.EventIds).ExecuteUpdateAsync(u => u
            .SetProperty(r => r.NextAttemptAt, _ => DateTimeOffset.UtcNow + hold)
            .SetProperty(r => r.LastUpdated, _ => DateTimeOffset.UtcNow)
            .SetProperty(r => r.LastError, error)
            .SetProperty(r => r.LastErrorCode, code), t), ct);
    }

    /// <summary>Rate limited: not read again before <paramref name="delay"/>, no retry spent.</summary>
    public Task DeferAsync(InboxBatch batch, TimeSpan delay, CancellationToken ct)
    {
        return contexts.RunAsync((db, t) => Pending(db, batch.EventIds).ExecuteUpdateAsync(u => u
            .SetProperty(r => r.LastUpdated, _ => DateTimeOffset.UtcNow)
            .SetProperty(r => r.NextAttemptAt, _ => DateTimeOffset.UtcNow + delay), t), ct);
    }

    public Task FailAllAsync(InboxBatch batch, string error, string? code, CancellationToken ct)
    {
        var all = batch.Events.Select(e => new FailedEvent(e.Id, error, code)).ToList();
        return contexts.RunAsync((db, t) => FailAsync(db, InboxSql.For(db.TableOf<TEntity>()), all, t), ct);
    }

    /// <summary>The rows of a batch that are still pending: every outcome below updates only these.</summary>
    private static IQueryable<TEntity> Pending(SyncDbContext db, long[] ids)
    {
        return db.Set<TEntity>().Where(r => ids.Contains(r.Id) && r.Status == "pending");
    }

    /// <summary>Done or superseded (<paramref name="status"/>): the row leaves the queue.</summary>
    private static Task<int> CompleteAsync(SyncDbContext db, long[] ids, string status, CancellationToken ct)
    {
        return Pending(db, ids).ExecuteUpdateAsync(u => u
                .SetProperty(r => r.Status, status)
                .SetProperty(r => r.LastUpdated, _ => DateTimeOffset.UtcNow)
                .SetProperty(r => r.LastError, (string?)null)
                .SetProperty(r => r.LastErrorCode, (string?)null), ct);
    }

    private Task<int> FailAsync(SyncDbContext db, InboxSql sql, IReadOnlyList<FailedEvent> rows, CancellationToken ct)
    {
        return db.Database.ExecuteAsync(sql.Fail, ct,
                Param("ids", Ids(rows)), Param("errors", ErrorsJson(rows)),
                Param("maxRetries", _opt.MaxRetries), Param("baseDelaySecs", _opt.RetryBaseDelay.TotalSeconds),
                Param("maxDelaySecs", _opt.MaxRetryDelay.TotalSeconds));
    }

    private static long[] Ids(IReadOnlyList<FailedEvent> rows)
    {
        return [.. rows.Select(r => r.EventId)];
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
}