using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using SalesforceSync.FakeSalesforce;
using SalesforceSync.Inbox;
using SalesforceSync.RateLimiting;
using SalesforceSync.Salesforce;
using SalesforceSync.Sync;

namespace SalesforceSync.Tests;

/// <summary>
/// The engine's flow end to end without a database: a claim handed to it, the in-memory org behind the real client,
/// and a recording inbox. What is asserted is which inbox operation each kind of result leads to.
/// </summary>
public sealed class EngineFlowTests
{
    private static readonly UpsertStrategy<AccountEvent> Accounts =
        AccountSync.Strategy;

    private readonly FakeSalesforceStore _org = new(new FakeSalesforceOptions { EnforceDailyLimit = false });
    private readonly RecordingInbox _inbox = new();
    private readonly Gate _gate = new();
    private readonly Interceptor _http = new();

    [Fact]
    public async Task Nothing_is_claimed_while_there_is_no_capacity()
    {
        _gate.Open = false;
        _inbox.Rows.Add(Row(1, "ACC-1", "2026-10-14T01:00:00Z"));

        (await Engine().UpsertBatchAsync(Accounts, _inbox, CancellationToken.None)).Should().Be(0);

        _inbox.Calls.Should().BeEmpty();
    }

    [Fact]
    public async Task An_empty_claim_is_nothing_to_do()
    {
        (await Engine().UpsertBatchAsync(Accounts, _inbox, CancellationToken.None)).Should().Be(0);

        _inbox.Calls.Should().Equal("claim");
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
        _inbox.Synced["ACC-3"] = PreparedBatch.ToMicroseconds(DateTimeOffset.Parse("2026-10-14T01:00:00Z"));

        (await Engine().UpsertBatchAsync(Accounts, _inbox, CancellationToken.None)).Should().Be(4);

        ((string?)_org.FindByExternalId("Account", "ACC-1")!["Name"]).Should().Be("new");
        _inbox.Calls.Should().Equal("claim", "record");
        var outcomes = _inbox.Recorded!;
        outcomes.Synced.Should().ContainSingle().Which.Should().Be(
            new SyncedEntity("ACC-1", PreparedBatch.ToMicroseconds(DateTimeOffset.Parse("2026-10-14T02:00:00Z")), 2));
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

        (await Engine().UpsertBatchAsync(Accounts, _inbox, CancellationToken.None)).Should().Be(2);

        _inbox.Calls.Should().HaveCount(2).And.HaveElementAt(1, "hold");
    }

    [Fact]
    public async Task A_rate_limited_batch_is_deferred_and_counts_as_no_progress()
    {
        _inbox.Rows.Add(Row(1, "ACC-1", "2026-10-14T01:00:00Z"));
        _http.Throw = new RateLimitedException(TimeSpan.FromSeconds(7));

        (await Engine().UpsertBatchAsync(Accounts, _inbox, CancellationToken.None)).Should().Be(0);

        _inbox.Calls.Should().HaveCount(2).And.HaveElementAt(1, "defer 00:00:07");
    }

    [Fact]
    public async Task A_batch_that_fails_before_anything_is_written_is_retried_as_a_whole()
    {
        _inbox.Rows.Add(Row(1, "ACC-1", "2026-10-14T01:00:00Z"));
        _http.Throw = new HttpRequestException(HttpRequestError.ConnectionError, "refused");

        (await Engine().UpsertBatchAsync(Accounts, _inbox, CancellationToken.None)).Should().Be(1);

        _inbox.Calls.Should().HaveCount(2).And.HaveElementAt(1, "fail all: refused ()");
    }

    [Fact]
    public async Task Rows_with_nothing_to_write_are_recorded_without_calling_Salesforce()
    {
        _inbox.Rows.Add(new InboxEvent(1, "ACC-1", "not json", 1));
        _http.Throw = new InvalidOperationException("Salesforce must not be called");

        (await Engine().UpsertBatchAsync(Accounts, _inbox, CancellationToken.None)).Should().Be(1);

        _inbox.Recorded!.Dead.Should().ContainSingle();
    }

    // ---------- Contacts: read and diff, then one upsert ----------

    [Fact]
    public async Task A_contact_batch_is_read_then_written_in_one_upsert_and_every_outcome_is_recorded()
    {
        await CreateAccountAsync("ACC-9");
        _org.InjectFault("Contact", "C-2", "FIELD_CUSTOM_VALIDATION_EXCEPTION", "rejected", 1);
        _inbox.Rows.AddRange([Contact(1, "C-1", "ACC-9"), Contact(2, "C-2", "ACC-9")]);
        var before = _org.GetStats();

        (await Engine().UpsertBatchAsync(ContactSync.Strategy, _inbox, CancellationToken.None)).Should().Be(2);

        _inbox.Calls.Should().Equal("claim", "record");
        _inbox.Recorded!.Synced.Should().ContainSingle().Which.EntityId.Should().Be("C-1");
        _inbox.Recorded.Dead.Should().ContainSingle().Which.Code.Should().Be("FIELD_CUSTOM_VALIDATION_EXCEPTION");
        _http.Writes.Should().Be(1, "both contacts in one upsert call");
        (_org.GetStats().UpsertRequests - before.UpsertRequests).Should().Be(1, "a new Contact on an existing Account needs no reference");
    }

    [Fact]
    public async Task A_contact_upsert_whose_answer_is_lost_holds_every_claimed_row()
    {
        await CreateAccountAsync("ACC-9");
        _inbox.Rows.AddRange([Contact(1, "C-1", "ACC-9"), Contact(2, "C-2", "ACC-9")]);
        _org.LoseResponsesFor("Contact", "C-1", 1);

        (await Engine().UpsertBatchAsync(ContactSync.Strategy, _inbox, CancellationToken.None)).Should().Be(2);

        _inbox.Calls.Should().Equal("claim", "hold");
    }

    /// <summary>The contacts' Account, synced through the account strategy.</summary>
    private async Task CreateAccountAsync(string account)
    {
        _inbox.Rows.Add(Row(9, account, "2026-10-14T01:00:00Z"));
        (await Engine().UpsertBatchAsync(Accounts, _inbox, CancellationToken.None)).Should().Be(1);
        _inbox.Rows.Clear();
        _inbox.Calls.Clear();
        _http.Writes = 0;
    }

    private static InboxEvent Contact(long id, string contact, string account)
    {
        return new(id, contact,
        $$"""{ "meta": { "time": "2026-10-14T01:00:00Z", "type": "inserted" }, "contact": { "externalId": "{{contact}}", "accountExternalId": "{{account}}", "lastName": "Doe" } }""", 1);
    }

    // ---------- A batch written in more than one call ----------

    [Fact]
    public async Task A_later_call_that_provably_failed_retries_the_whole_batch_and_marks_nothing_done()
    {
        await CreateOldAsync();
        _http.FailWrite = (2, new HttpRequestException(HttpRequestError.ConnectionError, "refused"));

        await TwoCallBatchAsync();

        _inbox.Calls.Should().Equal(["claim", "fail all: refused ()"],
            "every event is retried and none is marked done, not even NEW's, whose call succeeded");
    }

    [Fact]
    public async Task The_retry_of_a_batch_whose_later_call_failed_finds_what_the_earlier_call_saved()
    {
        await CreateOldAsync();
        _http.FailWrite = (2, new HttpRequestException(HttpRequestError.ConnectionError, "refused"));
        await TwoCallBatchAsync();
        _inbox.Calls.Clear();
        _http.FailWrite = null;
        _http.Writes = 0;

        (await Engine().UpsertBatchAsync(ThreeObjects, _inbox, CancellationToken.None)).Should().Be(2);

        _http.Writes.Should().Be(3, "NEW is unchanged now: only OLD's change is written, one upsert per object");
        _inbox.Recorded!.Synced.Select(s => s.EntityId).Should().BeEquivalentTo(["NEW", "OLD"]);
        _org.History("Account", "NEW").Should().ContainSingle("NEW was written once, not twice");
    }

    [Fact]
    public async Task A_later_call_whose_answer_is_lost_holds_the_whole_batch()
    {
        await CreateOldAsync();
        _org.LoseResponsesFor("Account", "OLD", 1);

        await TwoCallBatchAsync();

        _inbox.Calls.Should().Equal("claim", "hold");
    }

    [Fact]
    public async Task A_later_call_that_is_rate_limited_defers_the_whole_batch()
    {
        await CreateOldAsync();
        _http.FailWrite = (2, new RateLimitedException(TimeSpan.FromSeconds(7)));

        await TwoCallBatchAsync(processed: 0);

        _inbox.Calls.Should().Equal("claim", "defer 00:00:07");
    }

    [Fact]
    public async Task A_Profile_upsert_whose_answer_is_lost_after_the_Account_upsert_was_saved_holds_the_whole_batch()
    {
        _inbox.Rows.AddRange([AccountRow(1, "ACC-1", "2026-10-14T01:00:00Z", "One", "Gold"), AccountRow(2, "ACC-2", "2026-10-14T01:00:00Z", "Two", "Gold")]);
        (await Engine().UpsertBatchAsync(Accounts, _inbox, CancellationToken.None)).Should().Be(2);
        _inbox.Rows.Clear();
        _inbox.Calls.Clear();
        _http.Writes = 0;
        _inbox.Rows.AddRange([AccountRow(3, "ACC-1", "2026-10-14T02:00:00Z", "One!", "Gold"), AccountRow(4, "ACC-2", "2026-10-14T02:00:00Z", "Two", "Silver")]);
        _org.LoseResponsesFor("Profile__c", "ACC-2", 1);

        (await Engine().UpsertBatchAsync(Accounts, _inbox, CancellationToken.None)).Should().Be(2);

        _http.Writes.Should().Be(2, "ACC-1's Account, then ACC-2's Profile");
        _inbox.Calls.Should().Equal(["claim", "hold"], "ACC-1's call succeeded, but the batch's outcome is unknown until the held rows are read again");
        ((string?)_org.FindByExternalId("Account", "ACC-1")!["Name"]).Should().Be("One!");

        _inbox.Calls.Clear();
        _http.Writes = 0;
        (await Engine().UpsertBatchAsync(Accounts, _inbox, CancellationToken.None)).Should().Be(2);

        _http.Writes.Should().Be(0, "the read finds both writes applied, the lost one included");
        _inbox.Recorded!.Synced.Select(s => s.EntityId).Should().BeEquivalentTo(["ACC-1", "ACC-2"]);
    }

    // ---------- Failures of the read ----------

    [Fact]
    public async Task A_read_rejecting_a_field_fails_the_batch_and_forces_a_fresh_describe()
    {
        var engine = Engine();
        _inbox.Rows.Add(Row(1, "ACC-1", "2026-10-14T01:00:00Z"));
        await engine.UpsertBatchAsync(Accounts, _inbox, CancellationToken.None); // describes are cached now
        _inbox.Calls.Clear();
        _http.OnRead = request => new HttpResponseMessage(System.Net.HttpStatusCode.OK)
        {
            RequestMessage = request,
            Content = new StringContent(/*lang=json,strict*/ """{"compositeResponse":[{"referenceId":"q0","httpStatusCode":400,"httpHeaders":{},"body":[{"errorCode":"INVALID_FIELD","message":"No such column 'Rating'"}]}]}""")
        };

        await engine.UpsertBatchAsync(Accounts, _inbox, CancellationToken.None);

        _inbox.Calls.Should().HaveElementAt(1, "fail all: Query q0 failed: q0: INVALID_FIELD No such column 'Rating' (INVALID_FIELD)");
        var before = _org.GetStats();
        _http.OnRead = null;
        await engine.UpsertBatchAsync(Accounts, _inbox, CancellationToken.None);
        var after = _org.GetStats();
        (after.Describes - before.Describes).Should().BeGreaterThan(0, "the cached describe was invalidated");
        (after.DescribesNotModified - before.DescribesNotModified).Should().Be(0, "an invalidated describe is unconditional");
    }

    [Fact]
    public async Task A_read_cut_off_by_the_lease_is_retried_not_held_because_nothing_was_written()
    {
        _inbox.Rows.Add(Row(1, "ACC-1", "2026-10-14T01:00:00Z"));
        _http.OnRead = _ => throw new TaskCanceledException("The lease cutoff");

        await Engine().UpsertBatchAsync(Accounts, _inbox, CancellationToken.None);

        _inbox.Calls.Should().HaveCount(2).And.HaveElementAt(1, "fail all: The lease cutoff ()");
    }

    // ---------- Grouping ----------

    [Fact]
    public void Outcomes_are_grouped_by_what_the_inbox_does_with_them()
    {
        static Work Entity(string id, long eventId)
        {
            return new(id, new UpsertUnit([]), eventId * 10, eventId);
        }

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

    /// <summary>
    /// Account, Profile and relationship per entity: a new entity needs three collections (creates), an existing one
    /// three others (updates), so the two together take two write requests (5 collections each at most): the new one's
    /// a Composite call (its Profile and relationship refer to its Account), the existing one's three upserts, one per
    /// object.
    /// </summary>
    private static readonly UpsertStrategy<AccountEvent> ThreeObjects = new("account",
        e => e.Account?.ExternalId, e => e.Meta,
        (key, e) => new UpsertUnit(
        [
            new SObjectRecord("acct", "Account", "External_Id__c", key, new Dictionary<string, object?> { ["Name"] = e.Account!.Name }),
            new SObjectRecord("prof", "Profile__c", "External_Id__c", $"P-{key}", new Dictionary<string, object?> { ["Tier__c"] = e.Account.Name },
                [new SObjectLookup("Account__c", "Account", "External_Id__c", key)]),
            new SObjectRecord("rel", "Account_Relationship__c", "External_Id__c", $"R-{key}", new Dictionary<string, object?> { ["Type__c"] = e.Account.Name },
                [new SObjectLookup("Account__c", "Account", "External_Id__c", key), new SObjectLookup("Related_Account__c", "Account", "External_Id__c", key)])
        ]),
        units =>
        [
            new SoqlQuery("Account", "External_Id__c", SoqlQuery.ExternalIdsOf(units, "Account")),
            new SoqlQuery("Profile__c", "External_Id__c", SoqlQuery.ExternalIdsOf(units, "Profile__c")),
            new SoqlQuery("Account_Relationship__c", "External_Id__c", SoqlQuery.ExternalIdsOf(units, "Account_Relationship__c"))
        ]);

    /// <summary>Creates the entity OLD, so that changing it takes the second write call.</summary>
    private async Task CreateOldAsync()
    {
        _inbox.Rows.Add(Row(9, "OLD", "2026-10-14T01:00:00Z", "Gold"));
        (await Engine().UpsertBatchAsync(ThreeObjects, _inbox, CancellationToken.None)).Should().Be(1);
        _inbox.Recorded!.Synced.Should().ContainSingle();
        _inbox.Rows.Clear();
        _inbox.Calls.Clear();
        _http.Writes = 0;
    }

    /// <summary>NEW (row 1, first call) and a change of OLD (row 2, from the second call on: its Account first).</summary>
    /// <param name="processed">What the engine returns: 0 when the batch is deferred (no progress).</param>
    private async Task TwoCallBatchAsync(int processed = 2)
    {
        _inbox.Rows.AddRange([Row(1, "NEW", "2026-10-14T02:00:00Z", "Gold"), Row(2, "OLD", "2026-10-14T02:00:00Z", "Silver")]);
        var before = _org.GetStats().WriteRequests;

        (await Engine().UpsertBatchAsync(ThreeObjects, _inbox, CancellationToken.None)).Should().Be(processed);

        _http.Writes.Should().Be(2, "the batch stops at its failed second call");
        _org.FindByExternalId("Account", "NEW").Should().NotBeNull("the first call's records are in Salesforce");
        (_org.GetStats().WriteRequests - before).Should().BeGreaterThanOrEqualTo(1, "the first call reached the org");
    }

    private SalesforceBatchUpsertService Engine()
    {
        var options = Options.Create(new SalesforceOptions());
        _http.InnerHandler ??= new FakeSalesforceHandler(_org);
        var http = new HttpClient(_http) { BaseAddress = new Uri("https://fake.my.salesforce.com") };
        return new SalesforceBatchUpsertService(_gate, new SalesforceRestClient(http), SalesforceStep.SchemaCache(),
            new SalesforceErrors(options), options, Options.Create(new SyncOptions()),
            NullLogger<SalesforceBatchUpsertService>.Instance);
    }

    /// <summary>An account event with its Profile (keyed like the account).</summary>
    private static InboxEvent AccountRow(long id, string account, string time, string name, string tier)
    {
        return new(id, account,
        $$"""{ "meta": { "time": "{{time}}", "type": "updated" }, "account": { "externalId": "{{account}}", "name": "{{name}}", "profile": { "tier": "{{tier}}" } } }""", 1);
    }

    private static InboxEvent Row(long id, string account, string time, string name = "Acme")
    {
        return new(id, account,
        $$"""{ "meta": { "time": "{{time}}", "type": "inserted" }, "account": { "externalId": "{{account}}", "name": "{{name}}" } }""", 1);
    }

    /// <summary>Lets requests through to the org, or fails all of them, the read, or the Nth write.</summary>
    private sealed class Interceptor : DelegatingHandler
    {
        public Exception? Throw { get; set; }
        public Func<HttpRequestMessage, HttpResponseMessage>? OnRead { get; set; }
        public (int Number, Exception Failure)? FailWrite { get; set; }
        public int Writes { get; set; }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            if (Throw is not null)
            {
                throw Throw;
            }

            var path = request.RequestUri!.AbsolutePath;
            var isUpsert = path.Contains("/composite/sobjects/", StringComparison.Ordinal);
            if (isUpsert || path.EndsWith("/composite", StringComparison.Ordinal))
            {
                var isWrite = isUpsert || (await request.Content!.ReadAsStringAsync(ct)).Contains("/composite/sobjects", StringComparison.Ordinal);
                if (!isWrite && OnRead is not null)
                {
                    return OnRead(request);
                }

                if (isWrite && ++Writes == FailWrite?.Number)
                {
                    throw FailWrite.Value.Failure;
                }
            }
            return await base.SendAsync(request, ct);
        }
    }

    private sealed class Gate : ISalesforceBatchGate, IAsyncDisposable
    {
        public bool Open { get; set; } = true;
        public int Released { get; private set; }

        public Task<IAsyncDisposable?> TryEnterAsync(CancellationToken ct)
        {
            return Task.FromResult<IAsyncDisposable?>(Open ? this : null);
        }

        public ValueTask DisposeAsync()
        {
            Released++;
            return ValueTask.CompletedTask;
        }
    }

    private sealed class RecordingInbox : IRepository<AccountInboxRow>
    {
        public List<InboxEvent> Rows { get; } = [];
        public Dictionary<string, long> Synced { get; } = [];
        public List<string> Calls { get; } = [];
        public BatchOutcomes? Recorded { get; private set; }

        public Task AddAsync(string entityId, string payload, CancellationToken ct)
        {
            return Call($"add {entityId}");
        }

        public Task<InboxClaim> ClaimAsync(CancellationToken ct)
        {
            Calls.Add("claim");
            return Task.FromResult(new InboxClaim(Guid.NewGuid(), Rows, Synced));
        }

        public Task RecordAsync(InboxClaim claim, BatchOutcomes outcomes, CancellationToken ct)
        {
            Calls.Add("record");
            Recorded = outcomes;
            return Task.CompletedTask;
        }

        public Task HoldAsync(InboxClaim claim, string error, CancellationToken ct)
        {
            return Call("hold");
        }

        public Task DeferAsync(InboxClaim claim, TimeSpan delay, CancellationToken ct)
        {
            return Call($"defer {delay}");
        }

        public Task FailAllAsync(InboxClaim claim, string error, string? code, CancellationToken ct)
        {
            return Call($"fail all: {error} ({code})");
        }

        private Task Call(string what)
        {
            Calls.Add(what);
            return Task.CompletedTask;
        }
    }
}