using System.Net.Http.Json;
using System.Text.Json.Nodes;

namespace SalesforceSync.IntegrationTests;

/// <summary>
/// A write whose answer never arrives: the mock org applies it and then drops the connection. The fixture's
/// Sync:UnknownOutcomeHold is 4 seconds (10 minutes in production).
/// </summary>
[Collection(SyncCollection.Name)]
public sealed class UnknownOutcomeTests(SyncFixture app)
{
    private static readonly TimeSpan Hold = TimeSpan.FromSeconds(4);

    [Fact]
    public async Task Entity_is_held_after_a_write_with_an_unknown_outcome_and_ends_at_the_newest_event()
    {
        var acct = SyncFixture.NewAccountId();
        using (var lose = await app.Salesforce.PostAsJsonAsync("/__fake/lost-responses",
                   new JsonObject { ["sObject"] = "Account", ["externalId"] = acct, ["times"] = 1 }))
        {
            lose.EnsureSuccessStatusCode();
        }

        await app.PublishAsync(acct, 1, new JsonObject { ["name"] = "v1", ["description"] = "v1" }, new JsonObject { ["score"] = 1 });

        // The org applied v1, but the worker never heard back: the row is held, not retried and not failed
        var held = (await Eventually.ValueAsync(() => app.GetInboxRowsAsync(acct),
            rows => rows.Count == 1 && rows[0].ErrorCode == "UNKNOWN_OUTCOME", "the write's outcome to be recorded as unknown"))[0];
        var heldAt = DateTime.UtcNow;
        held.Status.Should().Be("pending");
        held.RetryCount.Should().Be(0, "an unknown outcome doesn't spend a retry");
        held.LockToken.Should().BeNull("the old worker can't record anything on it any more");
        held.LockedUntil.Should().BeAfter(heldAt.AddSeconds(1)).And.BeBefore(heldAt + Hold + TimeSpan.FromSeconds(1));
        ((string?)(await app.GetRecordAsync("Account", acct))?["Description"]).Should().Be("v1", "Salesforce did apply the write");

        // A newer event arrives during the hold and has to wait, although nobody is working on the account
        await app.PublishAsync(acct, 2, new JsonObject { ["name"] = "v2", ["description"] = "v2" }, new JsonObject { ["score"] = 2 });
        await Eventually.ValueAsync(() => app.GetInboxRowsAsync(acct), rows => rows.Count == 2, "v2 to be stored");
        await Task.Delay(TimeSpan.FromSeconds(1.5));
        var during = await app.GetInboxRowsAsync(acct);
        DateTime.UtcNow.Should().BeBefore(held.LockedUntil!.Value, "the check must fall inside the hold");
        during.Should().OnlyContain(r => r.Status == "pending" && r.RetryCount == 0, "nothing of a held entity may be claimed");
        during[1].LockToken.Should().BeNull();
        ((string?)(await app.GetRecordAsync("Account", acct))!["Description"]).Should().Be("v1");

        // After the hold, normal processing converges: read, diff, write the newest
        var settled = await app.WaitForInboxSettledAsync(acct, expectedRows: 2);
        settled.Should().Equal((1L, "superseded"), (2L, "done"));
        DateTime.UtcNow.Should().BeOnOrAfter(held.LockedUntil!.Value, "nothing was processed before the hold ended");

        ((string?)(await app.GetRecordAsync("Account", acct))!["Description"]).Should().Be("v2");
        ((decimal?)(await app.GetRecordAsync("Profile__c", SyncFixture.ProfileIdFor(acct)))!["Score__c"]).Should().Be(2m);
        var written = (await app.GetHistoryAsync("Account", acct)).Select(w => (string)w["Description"]!).ToList();
        written.Should().Equal(["v1", "v2"], "the older record is never written after the newer one");
    }

    [Fact]
    public async Task Held_entity_whose_lost_write_was_the_newest_event_completes_without_another_write()
    {
        var acct = SyncFixture.NewAccountId();
        using (var lose = await app.Salesforce.PostAsJsonAsync("/__fake/lost-responses",
                   new JsonObject { ["sObject"] = "Account", ["externalId"] = acct, ["times"] = 1 }))
        {
            lose.EnsureSuccessStatusCode();
        }

        await app.PublishAsync(acct, 1, new JsonObject { ["name"] = "only" }, new JsonObject { ["tier"] = "Gold" });

        // Held, then picked up again: Salesforce already has everything, so the diff finds nothing to write
        await Eventually.ValueAsync(() => app.GetInboxRowsAsync(acct),
            rows => rows.Count == 1 && rows[0].ErrorCode == "UNKNOWN_OUTCOME", "the hold");
        (await app.WaitForInboxSettledAsync(acct, expectedRows: 1)).Should().Equal((1L, "done"));
        (await app.GetInboxRowsAsync(acct))[0].ErrorCode.Should().BeNull("the error is cleared once the event is done");
        (await app.GetHistoryAsync("Account", acct)).Should().ContainSingle("the lost write had landed; nothing is written twice");
        (await app.GetHistoryAsync("Profile__c", SyncFixture.ProfileIdFor(acct))).Should().ContainSingle();
    }
}