using System.Net.Http.Json;
using System.Text.Json.Nodes;
using Dapper;
using Xunit.Abstractions;

namespace SalesforceSync.IntegrationTests;

/// <summary>
/// Proves per-account ordering under real concurrency: <see cref="MultiPodFixture.PodCount"/> worker pods
/// x <see cref="MultiPodFixture.WorkersPerPod"/> loops each, while the same accounts keep receiving updates.
/// </summary>
[Collection(MultiPodCollection.Name)]
public sealed class ConcurrencyTests(MultiPodFixture app, ITestOutputHelper output)
{
    private const int Accounts = 60;
    private const int Versions = 25;

    [Fact]
    public async Task Parallel_pods_never_process_the_same_account_concurrently_and_end_at_the_newest_version()
    {
        var ids = Enumerable.Range(0, Accounts).Select(_ => SyncFixture.NewAccountId()).ToArray();

        // Waves of updates to every account, so accounts get new events while workers are already processing them.
        // Every version changes every record, so each one must be written and the newest must win.
        for (var v = 1; v <= Versions; v++)
        {
            foreach (var id in ids)
                app.Publish(id, v,
                    new JsonObject { ["Name"] = $"Concurrent {id}", ["Description"] = $"v{v}" },
                    new JsonObject { ["Score__c"] = v });
            app.Flush();
            await Task.Delay(150);
        }

        // Wait for the drain, but check the race invariants even if it never finishes: a race typically shows up
        // as stuck or mis-ordered rows, and the claim audit says why.
        Dictionary<string, long> statuses;
        try
        {
            statuses = await Eventually.ValueAsync(
                () => InboxStatusesAsync(ids),
                s => s.Values.Sum() == Accounts * Versions && !s.ContainsKey("pending"),
                "all concurrent events to be processed",
                TimeSpan.FromMinutes(2));
        }
        catch (TimeoutException ex)
        {
            output.WriteLine(ex.Message);
            statuses = await InboxStatusesAsync(ids);
        }

        // 1. Direct check of mutual exclusion: no two claims on the same account ever overlapped in time
        await using var conn = await app.Db.OpenConnectionAsync();
        var overlaps = (await conn.QueryAsync<string>(
            """
            select format('%s: claim %s (seq %s, versions %s) while claim %s (seq %s..%s, versions %s) was open',
                          entity_key, second_token, second_claim_seq, second_versions,
                          first_token, first_claim_seq, coalesce(first_release_seq::text, 'never'), first_versions)
            from sf_claim_overlaps where entity_key = any(@ids)
            """,
            new { ids })).ToList();
        var audit = await conn.QuerySingleAsync<(long Claims, long Unreleased, long ReclaimedAccounts)>("""
            select count(*),
                   count(*) filter (where release_seq is null),
                   (select count(*) from (select 1 from sf_claim_audit where entity_key = any(@ids)
                                          group by entity_key having count(*) > 1) x)
            from sf_claim_audit where entity_key = any(@ids)
            """, new { ids });
        output.WriteLine($"claims={audit.Claims} unreleased={audit.Unreleased} accountsClaimedMoreThanOnce={audit.ReclaimedAccounts} overlaps={overlaps.Count}");
        overlaps.Should().BeEmpty("no two claims on the same account may overlap, e.g. {0}", string.Join(" | ", overlaps.Take(3)));

        // 2. Nothing in Salesforce prevents an out-of-order write, so it would show up in the write history:
        //    every account's Description and every profile's Score__c must only ever move forward
        var stats = await app.GetStatsAsync();
        output.WriteLine($"fake org: {stats}");
        stats.GetProperty("graphsFailed").GetInt64().Should().Be(0);
        var regressions = new List<string>();
        foreach (var id in ids)
        {
            var accountVersions = (await app.GetHistoryAsync("Account", id))
                .Where(w => w.ContainsKey("Description"))
                .Select(w => long.Parse(((string)w["Description"]!)[1..])).ToList();
            var profileVersions = (await app.GetHistoryAsync("Profile__c", SyncFixture.ProfileIdFor(id)))
                .Where(w => w.ContainsKey("Score__c"))
                .Select(w => (long)w["Score__c"]!).ToList();
            if (!IsIncreasing(accountVersions) || !IsIncreasing(profileVersions))
                regressions.Add($"{id}: account writes v[{string.Join(",", accountVersions)}], profile writes v[{string.Join(",", profileVersions)}]");
        }
        regressions.Should().BeEmpty("no account may receive an older version after a newer one, e.g. {0}",
            string.Join(" | ", regressions.Take(3)));

        // 3. Everything processed, nothing superseded (events arrive in order, so superseded would mean an
        //    account's watermark ran ahead of its events), every claim released, and real contention happened
        statuses.Should().Equal(new Dictionary<string, long> { ["done"] = Accounts * Versions });
        audit.Unreleased.Should().Be(0, "every claim must be released");
        audit.ReclaimedAccounts.Should().BeGreaterThan(Accounts / 2,
            "most of the {0} accounts should be claimed repeatedly (real contention)", Accounts);

        // 4. Every account and profile holds the newest values
        foreach (var id in ids)
        {
            var account = await app.GetRecordAsync("Account", id);
            var profile = await app.GetRecordAsync("Profile__c", SyncFixture.ProfileIdFor(id));
            account.Should().NotBeNull();
            profile.Should().NotBeNull();
            ((string?)account!["Description"]).Should().Be($"v{Versions}");
            ((decimal?)profile!["Score__c"]).Should().Be(Versions);
        }

        // 5. The work really was spread across pods
        var clients = await app.Salesforce.GetFromJsonAsync<Dictionary<string, long>>("/__fake/clients");
        clients.Should().NotBeNull();
        foreach (var (client, requests) in clients!) output.WriteLine($"{client}: {requests} graph requests");
        clients.Should().HaveCount(MultiPodFixture.PodCount, "every pod should have written to Salesforce");
    }

    private static bool IsIncreasing(List<long> versions) => versions.Zip(versions.Skip(1)).All(p => p.First < p.Second);

    [Fact]
    public async Task Pods_survive_their_Postgres_connections_being_dropped()
    {
        var ids = Enumerable.Range(0, 20).Select(_ => SyncFixture.NewAccountId()).ToArray();
        const int versions = 8;

        // Drop every worker and consumer connection (not the tests') every 200 ms while the events flow
        using var chaos = new CancellationTokenSource();
        var killed = 0L;
        var killer = Task.Run(async () =>
        {
            while (!chaos.IsCancellationRequested)
            {
                await using (var conn = await app.Db.OpenConnectionAsync())
                    killed += await conn.ExecuteScalarAsync<long>("""
                        select count(*) filter (where pg_terminate_backend(pid))
                        from pg_stat_activity
                        where datname = current_database() and pid <> pg_backend_pid()
                          and backend_type = 'client backend' and application_name <> @me
                        """, new { me = SyncFixture.TestsApplicationName });
                try { await Task.Delay(200, chaos.Token); } catch (OperationCanceledException) { }
            }
        });

        try
        {
            for (var v = 1; v <= versions; v++)
            {
                foreach (var id in ids)
                    app.Publish(id, v, new JsonObject { ["Name"] = $"Chaos {id}", ["Description"] = $"v{v}" },
                        new JsonObject { ["Score__c"] = v });
                app.Flush();
                await Task.Delay(300);
            }

            await Eventually.ValueAsync(() => InboxStatusesAsync(ids),
                s => s.GetValueOrDefault("done") + s.GetValueOrDefault("dead") == ids.Length * versions,
                "all events to be processed despite dropped connections", TimeSpan.FromMinutes(3));
        }
        finally
        {
            chaos.Cancel();
            await killer;
        }

        output.WriteLine($"terminated {killed} worker connections");
        killed.Should().BePositive("the chaos loop should have dropped connections");
        (await InboxStatusesAsync(ids)).Should().Equal(new Dictionary<string, long> { ["done"] = ids.Length * versions });

        await using var db = await app.Db.OpenConnectionAsync();
        (await db.ExecuteScalarAsync<long>("select count(*) from sf_claim_overlaps where entity_key = any(@ids)", new { ids }))
            .Should().Be(0, "no two claims on the same account may overlap");
        foreach (var id in ids)
        {
            var account = await app.GetRecordAsync("Account", id);
            ((string?)account?["Description"]).Should().Be($"v{versions}");
            var written = (await app.GetHistoryAsync("Account", id))
                .Where(w => w.ContainsKey("Description")).Select(w => long.Parse(((string)w["Description"]!)[1..])).ToList();
            written.Should().BeInAscendingOrder().And.OnlyHaveUniqueItems(
                "{0} must never receive an older version after a newer one", id);
        }
    }

    private async Task<Dictionary<string, long>> InboxStatusesAsync(string[] ids)
    {
        await using var conn = await app.Db.OpenConnectionAsync();
        var rows = await conn.QueryAsync<(string Status, long Count)>(
            "select status, count(*) from sf_inbox where sf_event_key(payload) = any(@ids) group by status", new { ids });
        return rows.ToDictionary(r => r.Status, r => r.Count);
    }
}
