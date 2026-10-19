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
/// The claim's retry after a commit whose outcome is unknown. Runs in a schema of its own, so the pods (which work the
/// public schema) never see its rows.
/// </summary>
[Collection(SyncCollection.Name)]
public sealed class ClaimRetryTests(SyncFixture app)
{
    private const string Schema = "claim_retry_test";

    [Fact]
    public async Task A_claim_retried_after_an_unknown_commit_releases_what_the_lost_attempt_leased()
    {
        await using var db = await app.CreateSchemaAsync(Schema);
        await SyncSchema.MigrateAsync(SyncDbContext.CreateFactory(db));
        var entity = SyncFixture.NewAccountId();
        await using (var conn = await db.OpenConnectionAsync())
        {
            await conn.StoreAsync(SyncFixture.InboxTable(), entity, SyncFixture.BaseTime, "{}");
        }

        // The claim commits, and then the commit "fails": the entity is leased, but the worker can't know it
        var lostCommit = new LoseFirstCommit();
        var options = new DbContextOptionsBuilder<SyncDbContext>();
        SyncDbContext.Configure(options, db, new PostgresRetryOptions { BaseDelay = TimeSpan.FromMilliseconds(1) });
        var inbox = new InboxRepository<AccountInboxRow>(
            new PooledDbContextFactory<SyncDbContext>(options.AddInterceptors(lostCommit).Options), Options.Create(new SyncOptions()));

        var claim = await inbox.ClaimAsync(CancellationToken.None);

        lostCommit.Commits.Should().Be(2, "the claim ran again");
        claim.Events.Should().ContainSingle();
        await using var check = await db.OpenConnectionAsync();
        (await check.QuerySingleAsync<DateTime>($"SELECT next_attempt_at FROM {SyncFixture.InboxTable()} WHERE entity_id = @entity", new { entity }))
            .Should().BeAfter(DateTime.UtcNow, "the retry leased the entity (the lost attempt's lease was released, not left to expire)");

        await check.ExecuteAsync($"DROP SCHEMA {Schema} CASCADE");
    }

    private sealed class LoseFirstCommit : DbTransactionInterceptor
    {
        public int Commits { get; private set; }

        public override Task TransactionCommittedAsync(DbTransaction transaction, TransactionEndEventData eventData,
            CancellationToken cancellationToken = default)
        {
            Commits++;
            return Commits == 1 ? throw new NpgsqlException("connection reset", new IOException("broken pipe")) : Task.CompletedTask;
        }
    }
}