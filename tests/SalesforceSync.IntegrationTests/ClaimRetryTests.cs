using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.Options;
using Npgsql;
using SalesforceSync.Data;
using SalesforceSync.Inbox;
using SalesforceSync.Sync;

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
        await using (var admin = await app.Db.OpenConnectionAsync())
        {
            await admin.ExecuteAsync($"drop schema if exists {Schema} cascade; create schema {Schema}");
        }
        var connectionString = new NpgsqlConnectionStringBuilder(app.ConnectionString)
        {
            SearchPath = Schema,
            ApplicationName = SyncFixture.TestsApplicationName
        }.ConnectionString;
        await using var db = NpgsqlDataSource.Create(connectionString);

        await InboxSchema.EnsureCreatedAsync(SyncDbContext.CreateFactory(db), [SyncFixture.InboxTable()]);
        var entity = SyncFixture.NewAccountId();
        await using (var conn = await db.OpenConnectionAsync())
        {
            for (var i = 0; i < 3; i++)
            {
                await conn.ExecuteAsync(PgSql.Insert(SyncFixture.InboxTable()), new { entityId = entity, payload = "{}" });
            }
        }

        // The claim commits, and then the commit "fails": the rows are leased, but the worker can't know it
        var lostCommit = new LoseFirstCommit();
        var options = new DbContextOptionsBuilder<SyncDbContext>();
        SyncDbContext.Configure(options, db, new PostgresRetryOptions { BaseDelay = TimeSpan.FromMilliseconds(1) });
        var inbox = new InboxRepository<AccountInboxRow>(
            new PooledDbContextFactory<SyncDbContext>(options.AddInterceptors(lostCommit).Options), Options.Create(new SyncOptions()));

        var claim = await inbox.ClaimAsync(AccountSyncService.Type, CancellationToken.None);

        lostCommit.Commits.Should().Be(2, "the claim ran again");
        claim.Events.Should().HaveCount(3);
        await using var check = await db.OpenConnectionAsync();
        var leases = await check.QueryAsync<(Guid? Token, int Retries)>(
            $"select lock_token, retry_count from {SyncFixture.InboxTable()} where entity_id = @entity", new { entity });
        leases.Should().OnlyContain(l => l.Token == claim.Token, "the lost attempt's leases were released, not left to expire");
        leases.Should().OnlyContain(l => l.Retries == 1, "the lost attempt's retry was given back");

        await check.ExecuteAsync($"drop schema {Schema} cascade");
    }

    private sealed class LoseFirstCommit : DbTransactionInterceptor
    {
        public int Commits { get; private set; }

        public override async Task TransactionCommittedAsync(DbTransaction transaction, TransactionEndEventData eventData,
            CancellationToken cancellationToken = default)
        {
            Commits++;
            if (Commits == 1)
            {
                throw new NpgsqlException("connection reset", new IOException("broken pipe"));
            }
            await Task.CompletedTask;
        }
    }
}