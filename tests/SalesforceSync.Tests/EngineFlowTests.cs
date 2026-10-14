using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using SalesforceSync.FakeSalesforce;
using SalesforceSync.Inbox;
using SalesforceSync.Inbox.Payloads;
using SalesforceSync.RateLimiting;
using SalesforceSync.Salesforce;
using SalesforceSync.Sync;
using SalesforceSync.Sync.Mapping;

namespace SalesforceSync.Tests;

/// <summary>
/// The engine's flow end to end without a database: a claim handed to it, the in-memory org behind the real client,
/// and a recording inbox. What is asserted is which inbox operation each kind of result leads to.
/// </summary>
public sealed class EngineFlowTests
{
    private static readonly UpsertStrategy<AccountEvent> Accounts =
        AccountSyncService.CreateStrategy(new AccountMapper(), new ProfileMapper());

    private readonly FakeSalesforceStore _org = new(new FakeSalesforceOptions { EnforceDailyLimit = false });
    private readonly RecordingInbox _inbox = new();
    private readonly Gate _gate = new();
    private readonly Interceptor _http = new();

    [Fact]
    public async Task Nothing_is_claimed_while_there_is_no_capacity()
    {
        _gate.Open = false;
        _inbox.Rows.Add(Row(1, "ACC-1", "2026-10-14T01:00:00Z"));

        (await Engine().UpsertBatchAsync(Accounts, CancellationToken.None)).Should().Be(0);

        _inbox.Calls.Should().BeEmpty();
    }

    [Fact]
    public async Task An_empty_claim_is_nothing_to_do()
    {
        (await Engine().UpsertBatchAsync(Accounts, CancellationToken.None)).Should().Be(0);

        _inbox.Calls.Should().Equal("claim sf_account_inbox account");
        _gate.Released.Should().Be(1, "the slot is given back");
    }

    [Fact]
    public async Task The_newest_event_is_written_and_every_row_gets_its_outcome_in_one_recording()
    {
        _inbox.Rows.AddRange(
        [
            Row(1, "ACC-1", "2026-10-14T01:00:00Z", "old"),
            Row(2, "ACC-1", "2026-10-14T02:00:00Z", "new"),
            new InboxEvent(3, "ACC-2", "not json", 1),
            Row(4, "ACC-3", "2026-10-14T01:00:00Z"),
        ]);
        _inbox.Synced["ACC-3"] = BatchPreparer.ToMicroseconds(DateTimeOffset.Parse("2026-10-14T01:00:00Z"));

        (await Engine().UpsertBatchAsync(Accounts, CancellationToken.None)).Should().Be(4);

        ((string?)_org.FindByExternalId("Account", "ACC-1")!["Name"]).Should().Be("new");
        _inbox.Calls.Should().Equal("claim sf_account_inbox account", "record");
        var outcomes = _inbox.Recorded!;
        outcomes.Synced.Should().ContainSingle().Which.Should().Be(
            new SyncedEntity("ACC-1", BatchPreparer.ToMicroseconds(DateTimeOffset.Parse("2026-10-14T02:00:00Z")), 2));
        outcomes.Superseded.Should().BeEquivalentTo([1L, 4L], "an older event, and one that is not newer than the watermark");
        outcomes.Dead.Should().ContainSingle().Which.Should().Match<FailedEvent>(f => f.EventId == 3 && f.Code == SalesforceErrors.InvalidEvent);
        outcomes.Released.Should().BeEmpty();
        outcomes.Failed.Should().BeEmpty();
        _gate.Released.Should().Be(1);
    }

    [Fact]
    public async Task A_write_whose_answer_is_lost_holds_every_claimed_row()
    {
        _inbox.Rows.AddRange([Row(1, "ACC-1", "2026-10-14T01:00:00Z"), Row(2, "ACC-1", "2026-10-14T02:00:00Z")]);
        _org.LoseResponsesFor("Account", "ACC-1", 1);

        (await Engine().UpsertBatchAsync(Accounts, CancellationToken.None)).Should().Be(2);

        _inbox.Calls.Should().HaveCount(2).And.HaveElementAt(1, "hold");
    }

    [Fact]
    public async Task A_rate_limited_batch_is_deferred_and_counts_as_no_progress()
    {
        _inbox.Rows.Add(Row(1, "ACC-1", "2026-10-14T01:00:00Z"));
        _http.Throw = new RateLimitedException(TimeSpan.FromSeconds(7));

        (await Engine().UpsertBatchAsync(Accounts, CancellationToken.None)).Should().Be(0);

        _inbox.Calls.Should().HaveCount(2).And.HaveElementAt(1, "defer 00:00:07");
    }

    [Fact]
    public async Task A_batch_that_fails_before_anything_is_written_is_retried_as_a_whole()
    {
        _inbox.Rows.Add(Row(1, "ACC-1", "2026-10-14T01:00:00Z"));
        _http.Throw = new HttpRequestException(HttpRequestError.ConnectionError, "refused");

        (await Engine().UpsertBatchAsync(Accounts, CancellationToken.None)).Should().Be(1);

        _inbox.Calls.Should().HaveCount(2).And.HaveElementAt(1, "fail all: refused ()");
    }

    [Fact]
    public async Task Rows_with_nothing_to_write_are_recorded_without_calling_Salesforce()
    {
        _inbox.Rows.Add(new InboxEvent(1, "ACC-1", "not json", 1));
        _http.Throw = new InvalidOperationException("Salesforce must not be called");

        (await Engine().UpsertBatchAsync(Accounts, CancellationToken.None)).Should().Be(1);

        _inbox.Recorded!.Dead.Should().ContainSingle();
    }

    [Fact]
    public void Outcomes_are_grouped_by_what_the_inbox_does_with_them()
    {
        Work Entity(string id, long eventId) => new(id, new UpsertUnit([]), eventId * 10, eventId);
        var batch = new PreparedBatch([9], [Entity("A", 1), Entity("B", 2), Entity("C", 3), Entity("D", 4), Entity("E", 5)], [(8, "bad")]);
        SalesforceError[] error = [new(null, "PROCESSING_HALTED", "x"), new("acct", "SOME_CODE", "why")];

        var outcomes = batch.Outcomes(new Dictionary<string, UpsertResult>
        {
            ["A"] = new("A", SyncOutcome.Saved, []),
            ["B"] = new("B", SyncOutcome.Unchanged, []),
            ["C"] = new("C", SyncOutcome.NotAttempted, error),
            ["D"] = new("D", SyncOutcome.PermanentFailure, error),
            ["E"] = new("E", SyncOutcome.TransientFailure, error)
        });

        outcomes.Synced.Should().Equal(new SyncedEntity("A", 10, 1), new SyncedEntity("B", 20, 2));
        outcomes.Superseded.Should().Equal(9L);
        outcomes.Released.Should().Equal(3L);
        outcomes.Dead.Should().Equal(new FailedEvent(8, "bad", SalesforceErrors.InvalidEvent),
            new FailedEvent(4, "PROCESSING_HALTED x; acct: SOME_CODE why", "SOME_CODE"));
        outcomes.Failed.Should().Equal(new FailedEvent(5, "PROCESSING_HALTED x; acct: SOME_CODE why", "SOME_CODE"));
    }

    // ---------- Helpers ----------

    private SalesforceBatchUpsertService Engine()
    {
        var options = Options.Create(new SalesforceOptions());
        _http.InnerHandler = new FakeSalesforceHandler(_org);
        var http = new HttpClient(_http) { BaseAddress = new Uri("https://fake.my.salesforce.com") };
        return new SalesforceBatchUpsertService(_gate, _inbox, new SalesforceRestClient(http), new SalesforceSchemaCache(TimeProvider.System),
            new SalesforceErrors(options), new WriteEchoTracker(), options, Options.Create(new SyncOptions()),
            NullLogger<SalesforceBatchUpsertService>.Instance);
    }

    private static InboxEvent Row(long id, string account, string time, string name = "Acme") => new(id, account,
        $$"""{ "meta": { "time": "{{time}}", "type": "inserted" }, "account": { "externalId": "{{account}}", "name": "{{name}}" } }""", 1);

    private sealed class Interceptor : DelegatingHandler
    {
        public Exception? Throw { get; set; }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
            => Throw is null ? base.SendAsync(request, ct) : throw Throw;
    }

    private sealed class Gate : ISalesforceBatchGate, IAsyncDisposable
    {
        public bool Open { get; set; } = true;
        public int Released { get; private set; }

        public Task<IAsyncDisposable?> TryEnterAsync(CancellationToken ct) => Task.FromResult<IAsyncDisposable?>(Open ? this : null);

        public ValueTask DisposeAsync()
        {
            Released++;
            return ValueTask.CompletedTask;
        }
    }

    private sealed class RecordingInbox : IInboxRepository
    {
        public List<InboxEvent> Rows { get; } = [];
        public Dictionary<string, long> Synced { get; } = [];
        public List<string> Calls { get; } = [];
        public BatchOutcomes? Recorded { get; private set; }

        public Task<InboxClaim> ClaimAsync(string inboxTable, string entityType, CancellationToken ct)
        {
            Calls.Add($"claim {inboxTable} {entityType}");
            return Task.FromResult(new InboxClaim(inboxTable, entityType, Guid.NewGuid(), Rows, Synced));
        }

        public Task RecordAsync(InboxClaim claim, BatchOutcomes outcomes, CancellationToken ct)
        {
            Calls.Add("record");
            Recorded = outcomes;
            return Task.CompletedTask;
        }

        public Task HoldAsync(InboxClaim claim, string error, CancellationToken ct) => Call("hold");
        public Task DeferAsync(InboxClaim claim, TimeSpan delay, CancellationToken ct) => Call($"defer {delay}");
        public Task FailAllAsync(InboxClaim claim, string error, string? code, CancellationToken ct) => Call($"fail all: {error} ({code})");

        private Task Call(string what)
        {
            Calls.Add(what);
            return Task.CompletedTask;
        }
    }
}
