using System.Net;
using SalesforceSync.Data.Inbox;
using SalesforceSync.Infrastructure.Salesforce;
using SalesforceSync.Sync;
using SalesforceSync.ToSalesforce;

namespace SalesforceSync.Tests;

/// <summary>
/// Batch flow (claim, parse, send, record) and what it does when the type's send step fails as a whole.
/// Send step is a stand-in: what types do is tested with them.
/// </summary>
public sealed class SalesforceBatchSyncServiceTests
{
    private static readonly CancellationToken Ct = CancellationToken.None;

    private readonly InMemoryInbox<AccountInboxRow> _inbox = new();
    private readonly List<string> _sent = [];

    private SalesforceBatchSyncService Flow => SalesforceStep.Flow();

    private Task<int> RunAsync(SendStep<AccountEvent>? send = null)
    {
        return Flow.SyncBatchAsync(AccountEvent.Reader, _inbox, send ?? Done, Ct);
    }

    private Task<Failures> Done(IReadOnlyList<ReadEvent<AccountEvent>> events, CancellationToken ct)
    {
        _sent.AddRange(events.Select(e => e.EntityId));
        return Task.FromResult(new Failures());
    }

    private Task AddAsync(string key, int second = 1, string? json = null)
    {
        json ??= Events.Account(key, second);
        return _inbox.AddAsync(key, Events.TimeOf(json), json, Ct);
    }

    // ---------- Before Salesforce ----------

    [Fact]
    public async Task Nothing_due_is_nothing_to_do()
    {
        (await RunAsync()).Should().Be(0);

        _sent.Should().BeEmpty("the send step is not called");
        _inbox.Synced.Should().BeEmpty();
    }

    [Fact]
    public async Task The_claimed_events_are_parsed_and_handed_to_the_send_step_and_each_entity_without_a_failure_is_done()
    {
        await AddAsync("ACC-1");
        await AddAsync("ACC-2", second: 2);

        (await RunAsync()).Should().Be(2);

        _sent.Should().BeEquivalentTo("ACC-1", "ACC-2");
        _inbox.Synced.Should().BeEquivalentTo("ACC-1", "ACC-2");
        _inbox.Dead.Should().BeEmpty();
        _inbox.Failed.Should().BeEmpty();
    }

    [Fact]
    public async Task An_unreadable_row_is_dead_on_its_own_and_the_others_still_sync()
    {
        await AddAsync("ACC-1");
        await _inbox.AddAsync("ACC-2", DateTimeOffset.UnixEpoch, "not json", Ct);

        await RunAsync();

        _sent.Should().Equal("ACC-1");
        _inbox.Synced.Should().Equal("ACC-1");
        _inbox.Dead.Should().ContainSingle().Which.Should().Match<FailedEvent>(d => d.EntityId == "ACC-2" && d.Error.StartsWith("INVALID_EVENT"));
    }

    [Fact]
    public async Task An_event_stored_under_another_entity_than_it_is_about_is_never_sent()
    {
        await AddAsync("ACC-1", json: Events.Account("ACC-OTHER", 1));

        await RunAsync();

        _sent.Should().BeEmpty("the lease is on the row's entity; writing another entity under it would bypass that entity's own");
        _inbox.Dead.Should().ContainSingle().Which.Error.Should().Contain("ACC-OTHER");
    }

    // ---------- What the send step reports ----------

    [Fact]
    public async Task An_entity_with_a_failure_is_retried_and_one_with_a_permanent_failure_is_dead()
    {
        await AddAsync("ACC-OK");
        await AddAsync("ACC-RETRY", second: 2);
        await AddAsync("ACC-DEAD", second: 3);

        var failures = new Failures
        {
            { "ACC-RETRY", "UNABLE_TO_LOCK_ROW", "locked" },
            { "ACC-DEAD", "UNABLE_TO_LOCK_ROW", "locked" },
            { "ACC-DEAD", "STRING_TOO_LONG", "long", true }
        };

        await RunAsync((_, _) => Task.FromResult(failures));

        _inbox.Synced.Should().Equal("ACC-OK");
        _inbox.Failed.Should().ContainSingle().Which.EntityId.Should().Be("ACC-RETRY");
        var dead = _inbox.Dead.Should().ContainSingle().Subject;
        dead.EntityId.Should().Be("ACC-DEAD");
        dead.Error.Should().StartWith("STRING_TOO_LONG", "the permanent error first");
    }

    // ---------- When the whole step fails ----------

    [Fact]
    public async Task A_write_whose_outcome_is_unknown_holds_every_claimed_entity()
    {
        await AddAsync("ACC-1");
        await AddAsync("ACC-2", second: 2);

        (await RunAsync((_, _) => throw new UnknownOutcomeException(new TimeoutException("lost")))).Should().Be(2);

        var release = _inbox.Releases.Should().ContainSingle().Subject;
        release.Delay.Should().Be(new SyncOptions().UnknownOutcomeHold);
        release.Error.Should().StartWith("UNKNOWN_OUTCOME");
        _inbox.Synced.Should().BeEmpty();
        _inbox.Pending.Should().BeEquivalentTo("ACC-1", "ACC-2");
    }

    [Fact]
    public async Task A_mapping_error_stops_the_inbox_with_the_error_on_the_rows_to_fix()
    {
        await AddAsync("ACC-1");

        await RunAsync((_, _) => throw new StopInboxException(StopReason.MappingError, [("ACC-1", new SalesforceError(null, "INVALID_FIELD", "No such column 'X'"))]));

        var release = _inbox.Releases.Should().ContainSingle().Subject;
        release.StopInbox.Should().BeTrue();
        release.Delay.Should().Be(new SyncOptions().MappingErrorHold);
        release.Error.Should().StartWith("MAPPING_ERROR");
        release.RowErrors.Should().ContainSingle().Which.Should().Match<FailedEvent>(e => e.EntityId == "ACC-1" && e.Error.Contains("INVALID_FIELD"));
        _inbox.Dead.Should().BeEmpty("a wrong mapping dead-letters nothing");
    }

    [Fact]
    public async Task Full_data_storage_stops_the_inbox_for_the_hold_without_spending_attempts()
    {
        await AddAsync("ACC-1");
        await AddAsync("ACC-2", second: 2);

        await RunAsync((_, _) => throw new StopInboxException(StopReason.StorageFull,
            [("ACC-1", new SalesforceError(null, "STORAGE_LIMIT_EXCEEDED", "storage limit exceeded"))]));

        var release = _inbox.Releases.Should().ContainSingle().Subject;
        release.StopInbox.Should().BeTrue("every create fails until space is freed");
        release.Delay.Should().Be(new SyncOptions().StorageFullHold);
        release.Error.Should().StartWith("STORAGE_LIMIT_EXCEEDED");
        _inbox.Dead.Should().BeEmpty("full storage dead-letters nothing");
        _inbox.Pending.Should().BeEquivalentTo("ACC-1", "ACC-2");
    }

    [Theory]
    [InlineData(HttpStatusCode.Forbidden, /*lang=json,strict*/ """[{"message":"TotalRequests Limit exceeded.","errorCode":"REQUEST_LIMIT_EXCEEDED"}]""")]
    [InlineData(HttpStatusCode.Forbidden, /*lang=json,strict*/ """[{"message":"ConcurrentPerOrgLongTxn Limit exceeded.","errorCode":"REQUEST_LIMIT_EXCEEDED"}]""")]
    [InlineData(HttpStatusCode.TooManyRequests, "")]
    [InlineData(HttpStatusCode.ServiceUnavailable, /*lang=json,strict*/ """[{"errorCode":"SERVER_UNAVAILABLE","message":"down for maintenance"}]""")]
    public async Task Salesforce_asking_to_back_off_stops_the_inbox_without_spending_an_attempt(HttpStatusCode status, string body)
    {
        await AddAsync("ACC-1");
        await AddAsync("ACC-2", second: 2);

        (await RunAsync((_, _) => throw new SalesforceResponseException(status, body))).Should().Be(2);

        var release = _inbox.Releases.Should().ContainSingle().Subject;
        release.StopInbox.Should().BeTrue("every call is refused alike until the limit clears");
        release.Delay.Should().Be(new SyncOptions().RequestLimitHold);
        release.Error.Should().StartWith("REQUEST_LIMIT_EXCEEDED");
        _inbox.Failed.Should().BeEmpty("no attempt is spent");
    }

    [Fact]
    public async Task A_403_that_is_not_a_request_limit_retries_every_claimed_entity()
    {
        await AddAsync("ACC-1");

        await RunAsync((_, _) => throw new SalesforceResponseException(HttpStatusCode.Forbidden, /*lang=json,strict*/ """[{"errorCode":"INSUFFICIENT_ACCESS","message":"no"}]"""));

        _inbox.Releases.Should().BeEmpty();
        _inbox.Failed.Should().ContainSingle();
    }

    [Fact]
    public async Task Any_other_failure_retries_every_claimed_entity()
    {
        await AddAsync("ACC-1");
        await AddAsync("ACC-2", second: 2);

        (await RunAsync((_, _) => throw new HttpRequestException("refused"))).Should().Be(2);

        _inbox.Failed.Should().BeEquivalentTo([new FailedEvent("ACC-1", "refused"), new FailedEvent("ACC-2", "refused")]);
    }

    [Fact]
    public async Task The_send_step_is_given_up_on_before_the_lease_runs_out()
    {
        await AddAsync("ACC-1");
        var givenUp = false;
        var flow = SalesforceStep.Flow(new SyncOptions { Lease = TimeSpan.FromMilliseconds(500) });

        await flow.SyncBatchAsync(AccountEvent.Reader, _inbox, async (_, ct) =>
        {
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(30), ct);
            }
            catch (OperationCanceledException)
            {
                givenUp = true;
            }

            return [];
        }, Ct);

        givenUp.Should().BeTrue("the step runs under a deadline of 80% of the lease (400 ms here), not the whole 30 s");
    }

    [Fact]
    public async Task Shutdown_during_the_send_step_is_not_a_failure_that_is_recorded()
    {
        await AddAsync("ACC-1");
        using var cts = new CancellationTokenSource();

        var run = () => Flow.SyncBatchAsync(AccountEvent.Reader, _inbox, (_, ct) =>
        {
            cts.Cancel();
            ct.ThrowIfCancellationRequested();
            return Task.FromResult(new Failures());
        }, cts.Token);

        await run.Should().ThrowAsync<OperationCanceledException>();
        _inbox.Failed.Should().BeEmpty("the lease expires and the row is claimed again");
    }
}