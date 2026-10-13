using System.Net.Http.Json;
using System.Text.Json.Nodes;
using Dapper;
using Xunit.Abstractions;

namespace SalesforceSync.IntegrationTests;

/// <summary>
/// Proves per-account ordering under real concurrency: <see cref="MultiPodFixture.Pods"/> worker processes
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
        var ids = Enumerable.Range(0, Accounts).Select(_ => AppHostFixture.NewAccountId()).ToArray();

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
                          account_ext_id, second_token, second_claim_seq, second_versions,
                          first_token, first_claim_seq, coalesce(first_release_seq::text, 'never'), first_versions)
            from sf_claim_overlaps where account_ext_id = any(@ids)
            """,
            new { ids })).ToList();
        var audit = await conn.QuerySingleAsync<(long Claims, long Unreleased, long ReclaimedAccounts)>("""
            select count(*),
                   count(*) filter (where release_seq is null),
                   (select count(*) from (select 1 from sf_claim_audit where account_ext_id = any(@ids)
                                          group by account_ext_id having count(*) > 1) x)
            from sf_claim_audit where account_ext_id = any(@ids)
            """, new { ids });
        output.WriteLine($"claims={audit.Claims} unreleased={audit.Unreleased} accountsClaimedMoreThanOnce={audit.ReclaimedAccounts} overlaps={overlaps.Count}");
        Assert.True(overlaps.Count == 0,
            $"{overlaps.Count} overlapping claims on the same account, e.g. {string.Join(" | ", overlaps.Take(3))}");

        // 2. Nothing in Salesforce prevents an out-of-order write, so it would show up in the write history:
        //    every account's Description and every profile's Score__c must only ever move forward
        var stats = await app.GetStatsAsync();
        output.WriteLine($"fake org: {stats}");
        Assert.Equal(0, stats.GetProperty("graphsFailed").GetInt64());
        var regressions = new List<string>();
        foreach (var id in ids)
        {
            var accountVersions = (await app.GetHistoryAsync("Account", id))
                .Where(w => w.ContainsKey("Description"))
                .Select(w => long.Parse(((string)w["Description"]!)[1..])).ToList();
            var profileVersions = (await app.GetHistoryAsync("Profile__c", AppHostFixture.ProfileIdFor(id)))
                .Where(w => w.ContainsKey("Score__c"))
                .Select(w => (long)w["Score__c"]!).ToList();
            if (!IsIncreasing(accountVersions) || !IsIncreasing(profileVersions))
                regressions.Add($"{id}: account writes v[{string.Join(",", accountVersions)}], profile writes v[{string.Join(",", profileVersions)}]");
        }
        Assert.True(regressions.Count == 0,
            $"{regressions.Count} accounts received an older version after a newer one, e.g. {string.Join(" | ", regressions.Take(3))}");

        // 3. Everything processed, nothing superseded (events arrive in order, so superseded would mean an
        //    account's watermark ran ahead of its events), every claim released, and real contention happened
        Assert.Equal(new Dictionary<string, long> { ["done"] = Accounts * Versions }, statuses);
        Assert.Equal(0, audit.Unreleased);
        Assert.True(audit.ReclaimedAccounts > Accounts / 2,
            $"expected most accounts to be claimed repeatedly (real contention), got {audit.ReclaimedAccounts}/{Accounts}");

        // 4. Every account and profile holds the newest values
        foreach (var id in ids)
        {
            var account = await app.GetRecordAsync("Account", id);
            var profile = await app.GetRecordAsync("Profile__c", AppHostFixture.ProfileIdFor(id));
            Assert.NotNull(account);
            Assert.NotNull(profile);
            Assert.Equal($"v{Versions}", (string?)account["Description"]);
            Assert.Equal((decimal)Versions, (decimal?)profile["Score__c"]);
        }

        // 5. The work really was spread across pods
        var clients = await app.Salesforce.GetFromJsonAsync<Dictionary<string, long>>("/__fake/clients");
        Assert.NotNull(clients);
        foreach (var (client, requests) in clients) output.WriteLine($"{client}: {requests} graph requests");
        Assert.Equal(MultiPodFixture.Pods, clients.Count);
    }

    private static bool IsIncreasing(List<long> versions) => versions.Zip(versions.Skip(1)).All(p => p.First < p.Second);

    private async Task<Dictionary<string, long>> InboxStatusesAsync(string[] ids)
    {
        await using var conn = await app.Db.OpenConnectionAsync();
        var rows = await conn.QueryAsync<(string Status, long Count)>(
            "select status, count(*) from sf_inbox where account_ext_id = any(@ids) group by status", new { ids });
        return rows.ToDictionary(r => r.Status, r => r.Count);
    }
}
