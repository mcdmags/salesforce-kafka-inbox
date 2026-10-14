using System.Globalization;
using System.Text.Json;
using Dapper;
using Microsoft.Extensions.Options;
using SalesforceSync.Data;
using SalesforceSync.Salesforce;
using SalesforceSync.Sync;
using SalesforceSync.Sync.Upsert;

namespace SalesforceSync.Inbox;

/// <summary>
/// <see cref="IInboxRepository"/> on Postgres: the <see cref="InboxSql"/> statements of the claim's table, each unit of
/// work run through <see cref="PostgresExecutionStrategy"/>.
/// </summary>
public sealed class InboxRepository(PostgresExecutionStrategy pg, IOptions<SyncOptions> options) : IInboxRepository
{
    private readonly SyncOptions _opt = options.Value;

    /// <summary>
    /// Locks and leases a batch, and reads the claimed entities' watermarks in the same transaction. A fresh token per
    /// attempt: if a commit's outcome is unknown and the claim is retried, rows leased by the lost attempt are not
    /// mistaken for this one's. The retry releases them first, so they don't wait for their lease to expire (and a
    /// lease nobody works on is never taken over).
    /// </summary>
    public Task<InboxClaim> ClaimAsync(string inboxTable, string entityType, CancellationToken ct)
    {
        var sql = InboxSql.For(inboxTable);
        var attempted = new List<Guid>();
        var attemptedEntityIds = new HashSet<string>(StringComparer.Ordinal);
        return pg.ExecuteInTransactionAsync(async (conn, tx, t) =>
        {
            CommandDefinition Command(string statement, object args) => new(statement, args, tx, cancellationToken: t);

            if (attemptedEntityIds.Count > 0)
                await conn.ExecuteAsync(Command(InboxSql.Pinned(sql.ReleaseLostClaims),
                    new { tokens = attempted.ToArray(), entityIds = attemptedEntityIds.ToArray() }));

            var token = Guid.NewGuid();
            attempted.Add(token);
            var entityIds = (await conn.QueryAsync<string>(Command(InboxSql.Pinned(sql.LockHeads), new
            {
                window = InboxSql.ClaimWindow,
                batchSize = Math.Min(_opt.BatchSize, ReadQueryBuilder.MaxEntitiesPerBatch)
            }))).ToArray();
            if (entityIds.Length == 0) return new InboxClaim(inboxTable, entityType, token, [], new Dictionary<string, long>());
            attemptedEntityIds.UnionWith(entityIds);

            var events = (await conn.QueryAsync<InboxEvent>(Command(sql.ClaimEntities,
                new { entityIds, token, lease = _opt.Lease, maxPerEntity = _opt.MaxEventsPerEntity }))).ToList();
            var synced = (await conn.QueryAsync<(string EntityId, long Time)>(Command(InboxSql.SyncedTimes, new { entityType, entityIds })))
                .ToDictionary(r => r.EntityId, r => r.Time, StringComparer.Ordinal);
            return new InboxClaim(inboxTable, entityType, token, events, synced);
        }, ct);
    }

    /// <summary>
    /// One statement per kind of outcome (not per entity). Safe to repeat: every statement is conditional on the
    /// lease token.
    /// </summary>
    public Task RecordAsync(InboxClaim claim, BatchOutcomes outcomes, CancellationToken ct)
    {
        var sql = InboxSql.For(claim.InboxTable);
        var token = claim.Token;
        return pg.ExecuteInTransactionAsync(async (conn, tx, t) =>
        {
            // The first statement sent also pins the plans for the transaction (same round trip)
            var first = true;
            Task Run(string statement, object args)
            {
                var text = first ? InboxSql.Pinned(statement) : statement;
                first = false;
                return conn.ExecuteAsync(new CommandDefinition(text, args, tx, cancellationToken: t));
            }

            if (outcomes.Synced.Count > 0)
            {
                // Watermarks first: Complete releases the lease that MarkSynced checks
                await Run(sql.MarkSynced, new
                {
                    entityType = claim.EntityType, token,
                    entityIds = outcomes.Synced.Select(s => s.EntityId).ToArray(),
                    times = outcomes.Synced.Select(s => s.EventTime).ToArray()
                });
                await Run(sql.Complete, new { ids = outcomes.Synced.Select(s => s.EventId).ToArray(), token, status = "done" });
            }
            if (outcomes.Superseded.Count > 0)
                await Run(sql.Complete, new { ids = outcomes.Superseded.ToArray(), token, status = "superseded" });
            if (outcomes.Released.Count > 0)
                await Run(sql.Release, new { ids = outcomes.Released.ToArray(), token });
            if (outcomes.Dead.Count > 0)
                await Run(sql.Dead, new { ids = Ids(outcomes.Dead), errors = ErrorsJson(outcomes.Dead), token });
            if (outcomes.Failed.Count > 0)
                await Run(sql.Fail, FailArgs(outcomes.Failed, token));
            if (outcomes.Held.Count > 0)
                await Run(sql.Hold, HoldArgs(Ids(outcomes.Held), token, outcomes.Held[0].Error));
            if (outcomes.Deferred.Count > 0)
                await Run(sql.Defer, new { ids = outcomes.Deferred.ToArray(), token, delay = outcomes.DeferFor });
        }, ct);
    }

    public Task HoldAsync(InboxClaim claim, string error, CancellationToken ct)
        => pg.ExecuteAsync(InboxSql.For(claim.InboxTable).Hold, HoldArgs(claim.EventIds, claim.Token, error), ct);

    public Task DeferAsync(InboxClaim claim, TimeSpan delay, CancellationToken ct)
        => pg.ExecuteAsync(InboxSql.For(claim.InboxTable).Defer, new { ids = claim.EventIds, token = claim.Token, delay }, ct);

    public Task FailAllAsync(InboxClaim claim, string error, string? code, CancellationToken ct)
    {
        var all = claim.Events.Select(e => new FailedEvent(e.Id, error, code)).ToList();
        return pg.ExecuteAsync(InboxSql.For(claim.InboxTable).Fail, FailArgs(all, claim.Token), ct);
    }

    private object FailArgs(IReadOnlyList<FailedEvent> rows, Guid token) => new
    {
        ids = Ids(rows),
        errors = ErrorsJson(rows),
        token,
        maxRetries = _opt.MaxRetries,
        baseDelaySecs = _opt.RetryBaseDelay.TotalSeconds,
        maxDelaySecs = _opt.MaxRetryDelay.TotalSeconds
    };

    private object HoldArgs(long[] ids, Guid token, string error)
        => new { ids, token, hold = _opt.UnknownOutcomeHold, error, code = UnknownOutcomeException.Code };

    private static long[] Ids(IReadOnlyList<FailedEvent> rows) => rows.Select(r => r.EventId).ToArray();

    /// <summary>{ "row id": [error, code] }, as InboxSql.Fail / Dead take the errors.</summary>
    private static string ErrorsJson(IReadOnlyList<FailedEvent> rows)
    {
        var errors = new Dictionary<string, string?[]>(StringComparer.Ordinal);
        foreach (var row in rows) errors[row.EventId.ToString(CultureInfo.InvariantCulture)] = [row.Error, row.Code];
        return JsonSerializer.Serialize(errors);
    }
}
