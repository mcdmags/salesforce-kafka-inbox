using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.Options;
using Npgsql;
using SalesforceSync.Data;
using SalesforceSync.Data.Inbox;

namespace SalesforceSync.IntegrationTests;

/// <summary>
/// Claim retry after commit landed but reply lost. Own schema: pods (public schema) never see its rows.
/// </summary>
[Collection(SyncCollection.Name)]
public sealed class ClaimRetryTests(SyncFixture app)
{
    private const string Schema = "claim_retry_test";

    [Fact]
    public async Task A_claim_retried_after_an_unknown_commit_leaves_what_the_lost_attempt_leased_until_the_lease_expires()
    {
        await using var db = await app.CreateSchemaAsync(Schema);
        await SyncSchema.MigrateAsync(SyncDbContext.CreateFactory(db));
        var entity = SyncFixture.NewAccountId();
        await using (var conn = await db.OpenConnectionAsync())
        {
            await conn.StoreAsync(SyncFixture.InboxTable(), entity, SyncFixture.BaseTime, "{}");
        }

        // Claim commits, reply "fails": entity leased, worker can't know
        var lostCommit = new LoseFirstReply();
        var options = new DbContextOptionsBuilder<SyncDbContext>();
        SyncDbContext.Configure(options, db, new PostgresRetryOptions { BaseDelay = TimeSpan.FromMilliseconds(1) });
        var inbox = new InboxRepository<AccountInboxRow>(
            new PooledDbContextFactory<SyncDbContext>(options.AddInterceptors(lostCommit).Options), Options.Create(new SyncOptions()));

        var claim = await inbox.ClaimAsync(CancellationToken.None);

        lostCommit.Claims.Should().Be(2, "the claim ran again");
        claim.Events.Should().BeEmpty("the lost attempt still holds the entity");
        await using var check = await db.OpenConnectionAsync();
        (await check.QuerySingleAsync<DateTime>($"SELECT next_attempt_at FROM {SyncFixture.InboxTable()} WHERE entity_id = @entity", new { entity }))
            .Should().BeAfter(DateTime.UtcNow, "the lost attempt's lease stays until it expires");

        await check.ExecuteAsync($"DROP SCHEMA {Schema} CASCADE");
    }

    /// <summary>
    /// Throws after first claim ran (and committed), like connection dropped before reply: lost lease is real.
    /// </summary>
    private sealed class LoseFirstReply : DbCommandInterceptor
    {
        public int Claims { get; private set; }

        public override ValueTask<DbDataReader> ReaderExecutedAsync(DbCommand command, CommandExecutedEventData eventData,
            DbDataReader result, CancellationToken cancellationToken = default)
        {
            if (!command.CommandText.Contains("FOR UPDATE SKIP LOCKED", StringComparison.Ordinal))
            {
                return ValueTask.FromResult(result);
            }
            Claims++;
            if (Claims == 1)
            {
                result.Dispose();
                throw new NpgsqlException("connection reset", new IOException("broken pipe"));
            }
            return ValueTask.FromResult(result);
        }
    }
}
