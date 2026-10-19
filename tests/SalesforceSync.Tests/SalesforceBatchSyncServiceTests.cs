using SalesforceSync.Data.Inbox;
using SalesforceSync.Infrastructure.RateLimiting;
using SalesforceSync.Infrastructure.Salesforce;
using SalesforceSync.Sync;
using SalesforceSync.ToSalesforce;

namespace SalesforceSync.Tests;

/// <summary>
/// The flow of a batch (pause, claim, parse, send, record) and what it does when the type's send step fails as a whole.
/// The send step is a stand-in here: what the types do is tested with them.
/// </summary>
public sealed class SalesforceBatchSyncServiceTests
{
    private static readonly CancellationToken Ct = CancellationToken.None;

    private readonly InMemoryInbox<AccountInboxRow> _inbox = new();
    private readonly SalesforceStep.NotPaused _limiter = new();
    private readonly List<string> _sent = [];

    private SalesforceBatchSyncService Flow => SalesforceStep.Flow(_limiter);

    private Task<int> RunAsync(SendStep<AccountEvent>? send = null)
    {
        return Flow.SyncBatchAsync(AccountEvent.Reader, _inbox, send ?? Done, Ct);
    }

    private Task<Dictionary<string, List<Failure>>> Done(IReadOnlyList<ReadEvent<AccountEvent>> events, CancellationToken ct)
    {
        _sent.AddRange(events.Select(e => e.EntityId));
        return Task.FromResult(new Dictionary<string, List<Failure>>());
    }

    private Task AddAsync(string key, int second = 1, string? json = null)
    {
        json ??= Events.Account(key, second);
        return _inbox.AddAsync(key, Events.TimeOf(json), json, Ct);
    }

    // ---------- Before Salesforce ----------

    [Fact]
    public async Task Nothing_is_claimed_while_the_process_is_paused()
    {
        _limiter.IsPaused = true;
        await AddAsync("ACC-1");

        (await RunAsync()).Should().Be(0);

        _inbox.Claims.Should().Be(0);
        _sent.Should().BeEmpty();
    }

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

        await RunAsync((events, _) => Task.FromResult(new Dictionary<string, List<Failure>>
        {
            ["ACC-RETRY"] = [new Failure("UNABLE_TO_LOCK_ROW: locked", false)],
            ["ACC-DEAD"] = [new Failure("UNABLE_TO_LOCK_ROW: locked", false), new Failure("STRING_TOO_LONG: long", true)]
        }));

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

        await RunAsync((_, _) => throw new MappingErrorException([("ACC-1", new SalesforceError(null, "INVALID_FIELD", "No such column 'X'"))]));

        var release = _inbox.Releases.Should().ContainSingle().Subject;
        release.StopInbox.Should().BeTrue();
        release.Delay.Should().Be(new SyncOptions().MappingErrorHold);
        release.Error.Should().StartWith("MAPPING_ERROR");
        release.OwnErrors.Should().ContainSingle().Which.Should().Match<FailedEvent>(e => e.EntityId == "ACC-1" && e.Error.Contains("INVALID_FIELD"));
        _inbox.Dead.Should().BeEmpty("a wrong mapping dead-letters nothing");
    }

    [Fact]
    public async Task A_rate_limited_batch_is_handed_back_to_be_claimed_after_the_wait_and_counts_as_no_work()
    {
        await AddAsync("ACC-1");

        (await RunAsync((_, _) => throw new RateLimitedException(TimeSpan.FromSeconds(7)))).Should().Be(0);

        _inbox.Releases.Should().ContainSingle().Which.Should().Match<Release>(r => r.Delay == TimeSpan.FromSeconds(7) && r.Error == null);
    }

    [Fact]
    public async Task Any_other_failure_retries_every_claimed_entity()
    {
        await AddAsync("ACC-1");
        await AddAsync("ACC-2", second: 2);

        (await RunAsync((_, _) => throw new HttpRequestException("refused"))).Should().Be(2);

        _inbox.FailedAll.Should().Equal("refused");
        _inbox.Failed.Select(f => f.EntityId).Should().BeEquivalentTo("ACC-1", "ACC-2");
    }

    [Fact]
    public async Task The_send_step_is_given_up_on_before_the_lease_runs_out()
    {
        await AddAsync("ACC-1");
        var givenUp = false;
        var flow = SalesforceStep.Flow(_limiter, new SyncOptions { Lease = TimeSpan.FromMilliseconds(500) });

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
            return Task.FromResult(new Dictionary<string, List<Failure>>());
        }, cts.Token);

        await run.Should().ThrowAsync<OperationCanceledException>();
        _inbox.Failed.Should().BeEmpty("the lease expires and the row is claimed again");
        _inbox.FailedAll.Should().BeEmpty();
    }
}