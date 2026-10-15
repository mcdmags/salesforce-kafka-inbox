using System.Net.Http.Json;
using System.Text.Json.Nodes;
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
            {
                app.Publish(id, v,
                    new JsonObject { ["name"] = $"Concurrent {id}", ["description"] = $"v{v}" },
                    new JsonObject { ["score"] = v });
            }

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
            select format('%s: claim %s (seq %s, rows %s) while claim %s (seq %s..%s, rows %s) was open',
                          entity_id, second_token, second_claim_seq, second_row_ids,
                          first_token, first_claim_seq, coalesce(first_release_seq::text, 'never'), first_row_ids)
            from sf_claim_overlaps where entity_id = any(@ids)
            """,
            new { ids })).ToList();
        var (Claims, Unreleased, ReclaimedAccounts) = await conn.QuerySingleAsync<(long Claims, long Unreleased, long ReclaimedAccounts)>("""
            select count(*),
                   count(*) filter (where release_seq is null),
                   (select count(*) from (select 1 from sf_claim_audit where entity_id = any(@ids)
                                          group by entity_id having count(*) > 1) x)
            from sf_claim_audit where entity_id = any(@ids)
            """, new { ids });
        output.WriteLine($"claims={Claims} unreleased={Unreleased} accountsClaimedMoreThanOnce={ReclaimedAccounts} overlaps={overlaps.Count}");
        overlaps.Should().BeEmpty("no two claims on the same account may overlap, e.g. {0}", string.Join(" | ", overlaps.Take(3)));

        // 2. Nothing in Salesforce prevents an out-of-order write, so it would show up in the write history:
        //    every account's Description and every profile's Score__c must only ever move forward
        var stats = await app.GetStatsAsync();
        output.WriteLine($"fake org: {stats}");
        stats.GetProperty("recordsFailed").GetInt64().Should().Be(0);
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
            {
                regressions.Add($"{id}: account writes v[{string.Join(",", accountVersions)}], profile writes v[{string.Join(",", profileVersions)}]");
            }
        }
        regressions.Should().BeEmpty("no account may receive an older version after a newer one, e.g. {0}",
            string.Join(" | ", regressions.Take(3)));

        // 3. Everything processed: every event was written, or superseded because a newer event of its account was
        //    claimed along with it. Every claim released, and real contention happened
        statuses.Keys.Should().BeSubsetOf(["done", "superseded"], "nothing may be left pending or dead-lettered");
        statuses.Values.Sum().Should().Be(Accounts * Versions);
        statuses["done"].Should().BeGreaterThanOrEqualTo(Accounts, "each account's newest event is written");
        Unreleased.Should().Be(0, "every claim must be released");
        ReclaimedAccounts.Should().BeGreaterThan(Accounts / 2,
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
        foreach (var (client, requests) in clients!)
        {
            output.WriteLine($"{client}: {requests} composite requests and upserts");
        }

        clients.Should().HaveCount(MultiPodFixture.PodCount, "every pod should have written to Salesforce");
    }

    [Fact]
    public async Task Events_out_of_time_order_over_any_partition_and_pod_end_at_the_newest_time()
    {
        const int accounts = 30, versions = 12;
        var ids = Enumerable.Range(0, accounts).Select(_ => SyncFixture.NewAccountId()).ToArray();
        var random = new Random(7);

        // Every account's versions in a shuffled order, each under a random Kafka key or none: the events of one
        // account are spread over the 6 partitions, consumed by 3 pods, and arrive in no particular order
        var events = ids.SelectMany(id => Enumerable.Range(1, versions).Select(v => (Id: id, Version: v)))
            .OrderBy(_ => random.Next()).ToList();
        foreach (var wave in events.Chunk(accounts))
        {
            foreach (var (id, v) in wave)
            {
                await app.PublishRawAsync(random.Next(3) == 0 ? null : Any.String("k-"),
                    SyncFixture.AccountEvent(id, v, new JsonObject { ["name"] = $"Shuffled {id}", ["description"] = $"v{v}" },
                        new JsonObject { ["score"] = v }).ToJsonString());
            }

            await Task.Delay(50);
        }

        var statuses = await Eventually.ValueAsync(() => InboxStatusesAsync(ids),
            s => s.Values.Sum() == accounts * versions && !s.ContainsKey("pending"),
            "all shuffled events to be processed", TimeSpan.FromMinutes(2));
        statuses.Keys.Should().BeSubsetOf(["done", "superseded"]);

        await using var conn = await app.Db.OpenConnectionAsync();
        (await conn.ExecuteScalarAsync<long>("select count(*) from sf_claim_overlaps where entity_id = any(@ids)", new { ids }))
            .Should().Be(0, "no two claims on the same account may overlap");
        foreach (var id in ids)
        {
            ((string?)(await app.GetRecordAsync("Account", id))!["Description"]).Should().Be($"v{versions}", "the newest meta.time wins");
            ((decimal?)(await app.GetRecordAsync("Profile__c", SyncFixture.ProfileIdFor(id)))!["Score__c"]).Should().Be(versions);
            var written = (await app.GetHistoryAsync("Account", id)).Select(w => long.Parse(((string)w["Description"]!)[1..])).ToList();
            written.Should().BeInAscendingOrder().And.OnlyHaveUniqueItems("{0} must never receive an older record after a newer one", id);
        }
    }

    private static bool IsIncreasing(List<long> versions)
    {
        return versions.Zip(versions.Skip(1)).All(p => p.First < p.Second);
    }

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
                {
                    killed += await conn.ExecuteScalarAsync<long>("""
                        select count(*) filter (where pg_terminate_backend(pid))
                        from pg_stat_activity
                        where datname = current_database() and pid <> pg_backend_pid()
                          and backend_type = 'client backend' and application_name <> @me
                        """, new { me = SyncFixture.TestsApplicationName });
                }

                try { await Task.Delay(200, chaos.Token); } catch (OperationCanceledException) { }
            }
        });

        try
        {
            for (var v = 1; v <= versions; v++)
            {
                foreach (var id in ids)
                {
                    app.Publish(id, v, new JsonObject { ["name"] = $"Chaos {id}", ["description"] = $"v{v}" },
                        new JsonObject { ["score"] = v });
                }

                app.Flush();
                await Task.Delay(300);
            }

            await Eventually.ValueAsync(() => InboxStatusesAsync(ids),
                s => s.Values.Sum() == ids.Length * versions && !s.ContainsKey("pending"),
                "all events to be processed despite dropped connections", TimeSpan.FromMinutes(3));
        }
        finally
        {
            chaos.Cancel();
            await killer;
        }

        output.WriteLine($"terminated {killed} worker connections");
        killed.Should().BePositive("the chaos loop should have dropped connections");
        var statuses = await InboxStatusesAsync(ids);
        statuses.Keys.Should().BeSubsetOf(["done", "superseded"], "no event may be lost or dead-lettered");
        statuses.Values.Sum().Should().Be(ids.Length * versions);

        await using var db = await app.Db.OpenConnectionAsync();
        (await db.ExecuteScalarAsync<long>("select count(*) from sf_claim_overlaps where entity_id = any(@ids)", new { ids }))
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
            "select status, count(*) from sf_account_inbox where entity_id = any(@ids) group by status", new { ids });
        return rows.ToDictionary(r => r.Status, r => r.Count);
    }
}