using System.Text.Json.Nodes;

namespace SalesforceSync.IntegrationTests;

/// <summary>
/// A write whose answer never arrives: the mock org applies it and then drops the connection. The fixture's
/// Sync:UnknownOutcomeHold is <see cref="SyncFixture.UnknownOutcomeHold"/> (10 minutes in production).
/// </summary>
[Collection(SyncCollection.Name)]
public sealed class UnknownOutcomeTests(SyncFixture app)
{
    [Fact]
    public async Task Entity_is_held_after_a_write_with_an_unknown_outcome_and_ends_at_the_newest_event()
    {
        var acct = SyncFixture.NewAccountId();
        await app.LoseResponsesAsync("Account", acct);

        await app.PublishAsync(acct, 1, new JsonObject { ["name"] = "v1", ["description"] = "v1" }, new JsonObject { ["score"] = 1 });

        // The org applied v1, but the worker never heard back: the row is held, not retried and not failed
        var held = (await Eventually.ValueAsync(() => app.GetInboxRowAsync(acct),
            row => row?.Error?.Contains("UNKNOWN_OUTCOME") == true, "the write's outcome to be recorded as unknown"))!;
        var heldAt = DateTime.UtcNow;
        held.Status.Should().Be("pending");
        held.AttemptCount.Should().Be(0, "an unknown outcome doesn't spend an attempt");
        held.NextAttemptAt.Should().BeAfter(heldAt.AddSeconds(1)).And.BeBefore(heldAt + SyncFixture.UnknownOutcomeHold + TimeSpan.FromSeconds(1));
        ((string?)(await app.GetRecordAsync("Account", acct))?["Description"]).Should().Be("v1", "Salesforce did apply the write");

        // A newer event arrives during the hold, overwrites the row and waits, though nobody is working on the account
        await app.PublishAsync(acct, 2, new JsonObject { ["name"] = "v2", ["description"] = "v2" }, new JsonObject { ["score"] = 2 });
        await Eventually.ValueAsync(() => app.GetInboxRowAsync(acct), row => row?.Version == 2, "v2 to be stored");
        await Task.Delay(TimeSpan.FromSeconds(1.5));
        var during = (await app.GetInboxRowAsync(acct))!;
        DateTime.UtcNow.Should().BeBefore(held.NextAttemptAt!.Value, "the check must fall inside the hold");
        during.Should().Match<InboxRowState>(r => r.Version == 2 && r.Status == "pending" && r.AttemptCount == 0,
            "nothing of a held entity may be claimed");
        ((string?)(await app.GetRecordAsync("Account", acct))!["Description"]).Should().Be("v1");

        // After the hold, normal processing converges: read, diff, write the newest
        (await app.WaitForInboxSettledAsync(acct, version: 2)).Should().Be((2L, "done"));
        DateTime.UtcNow.Should().BeOnOrAfter(held.NextAttemptAt!.Value, "nothing was processed before the hold ended");

        ((string?)(await app.GetRecordAsync("Account", acct))!["Description"]).Should().Be("v2");
        ((decimal?)(await app.GetRecordAsync("Profile__c", SyncFixture.ProfileIdFor(acct)))!["Score__c"]).Should().Be(2m);
        var written = (await app.GetHistoryAsync("Account", acct)).Select(w => (string)w["Description"]!).ToList();
        written.Should().Equal(["v1", "v2"], "the older record is never written after the newer one");
    }

    [Fact]
    public async Task Held_entity_whose_lost_write_was_the_newest_event_completes_without_writing_anything_twice()
    {
        var acct = SyncFixture.NewAccountId();
        await app.LoseResponsesAsync("Account", acct);

        await app.PublishAsync(acct, 1, new JsonObject { ["name"] = "only" }, new JsonObject { ["tier"] = "Gold" });

        // Held, then picked up again: the lost upserts had saved the Account and its Profile, so the retry's
        // read finds nothing to change and writes nothing
        await Eventually.ValueAsync(() => app.GetInboxRowAsync(acct),
            row => row?.Error?.Contains("UNKNOWN_OUTCOME") == true, "the hold");
        (await app.WaitForInboxSettledAsync(acct, version: 1)).Should().Be((1L, "done"));
        (await app.GetInboxRowAsync(acct))!.Error.Should().BeNull("the error is cleared once the event is done");
        (await app.GetHistoryAsync("Account", acct)).Should().ContainSingle("the lost write had landed; nothing is written twice");
        (await app.GetHistoryAsync("Profile__c", SyncFixture.ProfileIdFor(acct))).Should().ContainSingle();
    }
}