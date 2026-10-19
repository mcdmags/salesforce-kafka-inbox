using System.Net;
using Microsoft.Extensions.Options;
using SalesforceSync.Data.Inbox;
using SalesforceSync.FakeSalesforce;
using SalesforceSync.Infrastructure.RateLimiting;
using SalesforceSync.Infrastructure.Salesforce;
using SalesforceSync.Sync;

namespace SalesforceSync.Tests;

/// <summary>
/// The engine's flow end to end without a database: a claim handed to it, the in-memory org behind the real client,
/// and a recording inbox. Asserts which inbox operation each kind of result leads to.
/// </summary>
public sealed class EngineFlowTests
{
    private static readonly SyncStrategy<AccountEvent> Accounts = Syncs.Account.Strategy;

    private readonly FakeSalesforceStore _org = new(new FakeSalesforceOptions { EnforceDailyLimit = false });
    private readonly RecordingInbox _inbox = new();
    private readonly SalesforceStep.NotPaused _limiter = new();
    private readonly Interceptor _http = new();

    [Fact]
    public async Task The_consumer_stores_an_event_with_the_time_it_read_from_meta_time()
    {
        var sync = new EntitySyncService<AccountEvent, AccountInboxRow>(Engine(), _inbox, Accounts);
        var json = Row("ACC-1", "2026-10-14T03:02:03.1234567+02:00").Payload;

        sync.TryReadEvent(json, out var entityId, out var eventTime, out var problem).Should().BeTrue("the event is valid, but: {0}", problem);
        await sync.StoreEventAsync(entityId!, eventTime, json, CancellationToken.None);

        _inbox.Calls.Should().Equal("add ACC-1 at 2026-10-14T03:02:03.1234567+02:00");
    }

    [Fact]
    public async Task Nothing_is_claimed_while_the_process_is_paused()
    {
        _limiter.IsPaused = true;
        _inbox.Rows.Add(Row("ACC-1", "2026-10-14T01:00:00Z"));

        (await Engine().SyncBatchAsync(Accounts, _inbox, CancellationToken.None)).Should().Be(0);

        _inbox.Calls.Should().BeEmpty();
    }

    [Fact]
    public async Task An_empty_claim_is_nothing_to_do()
    {
        (await Engine().SyncBatchAsync(Accounts, _inbox, CancellationToken.None)).Should().Be(0);

        _inbox.Calls.Should().Equal("claim");
    }

    [Fact]
    public async Task Each_entitys_event_is_written_and_every_row_gets_its_outcome_in_one_recording()
    {
        _inbox.Rows.AddRange(
        [
            Row("ACC-1", "2026-10-14T02:00:00Z", "new"),
            Invalid("ACC-2"),
        ]);

        (await Engine().SyncBatchAsync(Accounts, _inbox, CancellationToken.None)).Should().Be(2);

        ((string?)_org.FindByExternalId("Account", "ACC-1")!["Name"]).Should().Be("new");
        _inbox.Calls.Should().Equal("claim", "record");
        var outcomes = _inbox.Recorded!;
        outcomes.Synced.Should().Equal(["ACC-1"]);
        outcomes.Dead.Should().ContainSingle().Which.Should().Match<FailedEvent>(f => f.EntityId == "ACC-2" && f.Error.StartsWith(SalesforceErrors.InvalidEvent));
        outcomes.Failed.Should().BeEmpty();
    }

    [Fact]
    public async Task A_write_whose_answer_is_lost_holds_every_claimed_row()
    {
        _inbox.Rows.AddRange([Row("ACC-1", "2026-10-14T01:00:00Z"), Row("ACC-2", "2026-10-14T02:00:00Z")]);
        _org.LoseResponsesFor("Account", "ACC-1", 1);

        (await Engine().SyncBatchAsync(Accounts, _inbox, CancellationToken.None)).Should().Be(2);

        _inbox.Calls.Should().HaveCount(2).And.HaveElementAt(1, "hold");
    }

    [Fact]
    public async Task A_rate_limited_batch_is_deferred_and_counts_as_no_progress()
    {
        _inbox.Rows.Add(Row("ACC-1", "2026-10-14T01:00:00Z"));
        _http.Throw = new RateLimitedException(TimeSpan.FromSeconds(7));

        (await Engine().SyncBatchAsync(Accounts, _inbox, CancellationToken.None)).Should().Be(0);

        _inbox.Calls.Should().HaveCount(2).And.HaveElementAt(1, "defer 00:00:07");
    }

    [Fact]
    public async Task A_batch_that_fails_before_anything_is_written_is_retried_as_a_whole()
    {
        _inbox.Rows.Add(Row("ACC-1", "2026-10-14T01:00:00Z"));
        _http.Throw = new HttpRequestException(HttpRequestError.ConnectionError, "refused");

        (await Engine().SyncBatchAsync(Accounts, _inbox, CancellationToken.None)).Should().Be(1);

        _inbox.Calls.Should().HaveCount(2).And.HaveElementAt(1, "fail all: refused");
    }

    [Fact]
    public async Task Rows_with_nothing_to_write_are_recorded_without_calling_Salesforce()
    {
        _inbox.Rows.Add(Invalid("ACC-1"));
        _http.Throw = new InvalidOperationException("Salesforce must not be called");

        (await Engine().SyncBatchAsync(Accounts, _inbox, CancellationToken.None)).Should().Be(1);

        _inbox.Recorded!.Dead.Should().ContainSingle();
    }

    // ---------- Contacts: read and diff, then one upsert ----------

    [Fact]
    public async Task A_contact_batch_is_read_then_written_in_one_upsert_and_every_outcome_is_recorded()
    {
        await CreateAccountAsync("ACC-9");
        _org.InjectFault("Contact", "C-2", "FIELD_CUSTOM_VALIDATION_EXCEPTION", "rejected", 1);
        _inbox.Rows.AddRange([Contact("C-1", "ACC-9"), Contact("C-2", "ACC-9")]);
        var before = _org.GetStats();

        (await Engine().SyncBatchAsync(Syncs.Contact.Strategy, _inbox, CancellationToken.None)).Should().Be(2);

        _inbox.Calls.Should().Equal("claim", "record");
        _inbox.Recorded!.Synced.Should().Equal(["C-1"], "C-1's row");
        _inbox.Recorded.Dead.Should().ContainSingle().Which.Error.Should().Contain("FIELD_CUSTOM_VALIDATION_EXCEPTION");
        _http.Writes.Should().Be(1, "both contacts in one upsert call");
        (_org.GetStats().UpsertRequests - before.UpsertRequests).Should().Be(1, "a new Contact on an existing Account needs no reference");
    }

    [Fact]
    public async Task A_contact_upsert_whose_answer_is_lost_holds_every_claimed_row()
    {
        await CreateAccountAsync("ACC-9");
        _inbox.Rows.AddRange([Contact("C-1", "ACC-9"), Contact("C-2", "ACC-9")]);
        _org.LoseResponsesFor("Contact", "C-1", 1);

        (await Engine().SyncBatchAsync(Syncs.Contact.Strategy, _inbox, CancellationToken.None)).Should().Be(2);

        _inbox.Calls.Should().Equal("claim", "hold");
    }

    /// <summary>The contacts' Account, synced through the account strategy.</summary>
    private async Task CreateAccountAsync(string account)
    {
        _inbox.Rows.Add(Row(account, "2026-10-14T01:00:00Z"));
        (await Engine().SyncBatchAsync(Accounts, _inbox, CancellationToken.None)).Should().Be(1);
        _inbox.Rows.Clear();
        _inbox.Calls.Clear();
        _http.Writes = 0;
    }

    private static InboxEvent Contact(string contact, string account)
    {
        return new(contact,
            $$"""{ "meta": { "time": "2026-10-14T01:00:00Z", "type": "inserted" }, "contact": { "externalId": "{{contact}}", "accountExternalId": "{{account}}", "lastName": "Doe" } }""",
            DateTimeOffset.Parse("2026-10-14T01:00:00Z"));
    }

    // ---------- A batch written in more than one call ----------

    [Fact]
    public async Task A_later_call_that_provably_failed_retries_the_whole_batch_and_marks_nothing_done()
    {
        await CreateOldAsync();
        _http.FailWrite = (2, new HttpRequestException(HttpRequestError.ConnectionError, "refused"));

        await TwoCallBatchAsync();

        _inbox.Calls.Should().Equal(["claim", "fail all: refused"],
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

        (await Engine().SyncBatchAsync(ThreeObjects, _inbox, CancellationToken.None)).Should().Be(3);

        _http.Writes.Should().Be(2, "the Accounts are in now: only the Contacts and the Profiles are written");
        _inbox.Recorded!.Synced.Should().BeEquivalentTo(["NEW", "OLD", "KIDS"], "NEW's, OLD's and KIDS's rows");
        _org.History("Account", "NEW").Should().ContainSingle("NEW was written once, not twice");
    }

    [Fact]
    public async Task A_later_call_whose_answer_is_lost_holds_the_whole_batch()
    {
        await CreateOldAsync();
        _org.LoseResponsesFor("Contact", "C-KIDS", 1);

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
    public async Task An_Account_and_Profile_write_whose_answer_is_lost_holds_the_whole_batch()
    {
        _inbox.Rows.AddRange([AccountRow("ACC-1", "2026-10-14T01:00:00Z", "One", "Gold"), AccountRow("ACC-2", "2026-10-14T01:00:00Z", "Two", "Gold")]);
        (await Engine().SyncBatchAsync(Accounts, _inbox, CancellationToken.None)).Should().Be(2);
        _inbox.Rows.Clear();
        _inbox.Calls.Clear();
        _http.Writes = 0;
        _inbox.Rows.AddRange([AccountRow("ACC-1", "2026-10-14T02:00:00Z", "One!", "Gold"), AccountRow("ACC-2", "2026-10-14T02:00:00Z", "Two", "Silver")]);
        _org.LoseResponsesFor("Profile__c", "ACC-2", 1);

        (await Engine().SyncBatchAsync(Accounts, _inbox, CancellationToken.None)).Should().Be(2);

        _http.Writes.Should().Be(2, "ACC-1's Account upsert, then ACC-2's Profile upsert");
        _inbox.Calls.Should().Equal(["claim", "hold"], "the org applied it, but the batch's outcome is unknown until the held rows are read again");
        ((string?)_org.FindByExternalId("Account", "ACC-1")!["Name"]).Should().Be("One!");

        _inbox.Calls.Clear();
        _http.Writes = 0;
        (await Engine().SyncBatchAsync(Accounts, _inbox, CancellationToken.None)).Should().Be(2);

        _http.Writes.Should().Be(0, "the read finds both writes applied, the lost one included");
        _inbox.Recorded!.Synced.Should().BeEquivalentTo(["ACC-1", "ACC-2"], "ACC-1's and ACC-2's rows");
    }

    // ---------- Failures of the read ----------

    [Fact]
    public async Task A_read_rejecting_a_field_halts_the_inbox_as_a_mapping_error()
    {
        var engine = Engine();
        _inbox.Rows.Add(Row("ACC-1", "2026-10-14T01:00:00Z"));
        await engine.SyncBatchAsync(Accounts, _inbox, CancellationToken.None); // record types are cached now
        _inbox.Calls.Clear();
        const string body = /*lang=json,strict*/ """[{"errorCode":"INVALID_FIELD","message":"No such column 'Rating' on entity 'Account'"}]""";
        _http.OnRead = request => new HttpResponseMessage(HttpStatusCode.BadRequest) { RequestMessage = request, Content = new StringContent(body) };

        await engine.SyncBatchAsync(Accounts, _inbox, CancellationToken.None);

        _inbox.Calls.Should().Equal("claim", "halt");
        _http.Writes.Should().Be(1, "nothing was written after the first batch");
    }

    [Fact]
    public async Task A_read_cut_off_by_the_lease_is_retried_not_held_because_nothing_was_written()
    {
        _inbox.Rows.Add(Row("ACC-1", "2026-10-14T01:00:00Z"));
        _http.OnRead = _ => throw new TaskCanceledException("The lease cutoff");

        await Engine().SyncBatchAsync(Accounts, _inbox, CancellationToken.None);

        _inbox.Calls.Should().HaveCount(2).And.HaveElementAt(1, "fail all: The lease cutoff");
    }

    // ---------- Grouping ----------

    [Fact]
    public void Outcomes_are_grouped_by_what_the_inbox_does_with_them()
    {
        static Work Entity(string id)
        {
            return new(id, new SyncUnit([]));
        }

        var batch = new PreparedBatch([Entity("A"), Entity("B"), Entity("D"), Entity("E")],
            [new FailedEvent("BAD", "bad")]);
        SalesforceError[] error = [new("acct", "SOME_CODE", "why")];

        var outcomes = batch.Outcomes(new Dictionary<string, SyncResult>
        {
            ["A"] = new("A", SyncOutcome.Saved, []),
            ["B"] = new("B", SyncOutcome.Unchanged, []),
            ["D"] = new("D", SyncOutcome.PermanentFailure, error),
            ["E"] = new("E", SyncOutcome.TransientFailure, error)
        });

        outcomes.Synced.Should().Equal("A", "B");
        outcomes.Dead.Should().Equal(new FailedEvent("BAD", "bad"),
            new FailedEvent("D", "acct: SOME_CODE why"));
        outcomes.Failed.Should().Equal(new FailedEvent("E", "acct: SOME_CODE why"));
    }

    // ---------- Helpers ----------

    /// <summary>
    /// Account, Contact and Profile per entity, except a KIDS entity, whose Account is not written (only its Contact
    /// and Profile). A batch of them is three upserts, one per object: Accounts, then Contacts, then Profiles.
    /// </summary>
    private static readonly SyncStrategy<AccountEvent> ThreeObjects = new("account",
        e => e.Account?.ExternalId, e => e.Meta,
        (key, e) => new SyncUnit(
        [
            .. key == "KIDS" ? [] : new[] { new SObjectRecord("acct", "Account", "ExternalId__c", key, new Dictionary<string, object?> { ["Name"] = e.Account!.Name }) },
            new SObjectRecord("contact", "Contact", "ExternalId__c", $"C-{key}", new Dictionary<string, object?> { ["LastName"] = e.Account!.Name },
                ForeignKeys: [new ForeignKey("AccountId__c", "Account", "ExternalId__c", key)]),
            new SObjectRecord("prof", "Profile__c", "ExternalId__c", $"P-{key}",
                new Dictionary<string, object?> { ["Tier__c"] = e.Account.Name, ["AccountId__c"] = key },
                ForeignKeys: [new ForeignKey("AccountId__c", "Account", "ExternalId__c", key)])
        ]),
        units =>
        [
            new SoqlQuery("Account", "ExternalId__c", SoqlQuery.ParentIdsOf(units, "Account")),
            new SoqlQuery("Contact", "ExternalId__c", SoqlQuery.ExternalIdsOf(units, "Contact")),
            new SoqlQuery("Profile__c", "ExternalId__c", SoqlQuery.ExternalIdsOf(units, "Profile__c"))
        ],
        _ => []);

    /// <summary>Creates the entities OLD and KIDS (its Account first, through the account strategy), so that changing KIDS takes the second write call.</summary>
    private async Task CreateOldAsync()
    {
        await CreateAccountAsync("KIDS");
        _inbox.Rows.AddRange([Row("OLD", "2026-10-14T01:00:00Z", "Gold"), Row("KIDS", "2026-10-14T01:00:00Z", "Gold")]);
        (await Engine().SyncBatchAsync(ThreeObjects, _inbox, CancellationToken.None)).Should().Be(2);
        _inbox.Recorded!.Synced.Should().HaveCount(2);
        _inbox.Rows.Clear();
        _inbox.Calls.Clear();
        _http.Writes = 0;
    }

    /// <summary>
    /// NEW, a change of OLD and a change of KIDS's Contact and Profile. The Accounts' upsert is the first write call,
    /// the Contacts' the second (the one the tests make fail), the Profiles' the third.
    /// </summary>
    /// <param name="processed">What the engine returns: 0 when the batch is deferred (no progress).</param>
    private async Task TwoCallBatchAsync(int processed = 3)
    {
        _inbox.Rows.AddRange([Row("NEW", "2026-10-14T02:00:00Z", "Gold"), Row("OLD", "2026-10-14T02:00:00Z", "Silver"),
            Row("KIDS", "2026-10-14T02:00:00Z", "Silver")]);
        var before = _org.GetStats().WriteRequests;

        (await Engine().SyncBatchAsync(ThreeObjects, _inbox, CancellationToken.None)).Should().Be(processed);

        _http.Writes.Should().Be(2, "the batch stops at its failed second call");
        _org.FindByExternalId("Account", "NEW").Should().NotBeNull("the first call's records are in Salesforce");
        (_org.GetStats().WriteRequests - before).Should().BeGreaterThanOrEqualTo(1, "the first call reached the org");
    }

    private SalesforceBatchSyncService Engine()
    {
        _http.InnerHandler ??= new FakeSalesforceHandler(_org);
        return SalesforceStep.Service(SalesforceStep.Http(_http), Options.Create(new SalesforceOptions()), _limiter);
    }

    /// <summary>An account event with its Profile (keyed like the account).</summary>
    private static InboxEvent AccountRow(string account, string time, string name, string tier)
    {
        return new(account,
        $$"""{ "meta": { "time": "{{time}}", "type": "updated" }, "account": { "externalId": "{{account}}", "name": "{{name}}", "profile": { "tier": "{{tier}}" } } }""",
            DateTimeOffset.Parse(time));
    }

    private static InboxEvent Row(string account, string time, string name = "Acme")
    {
        return new(account,
        $$"""{ "meta": { "time": "{{time}}", "type": "inserted" }, "account": { "externalId": "{{account}}", "name": "{{name}}" } }""",
            DateTimeOffset.Parse(time));
    }

    /// <summary>A row whose payload can't be read.</summary>
    private static InboxEvent Invalid(string account)
    {
        return new(account, "not json", DateTimeOffset.UnixEpoch);
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

            // A write is a collections call (upsert or delete); a read is a query
            var path = request.RequestUri!.AbsolutePath;
            var isWrite = path.Contains("/composite/sobjects", StringComparison.Ordinal);
            return !isWrite && OnRead is not null && path.EndsWith("/query", StringComparison.Ordinal) ? OnRead(request)
                : isWrite && ++Writes == FailWrite?.Number ? throw FailWrite.Value.Failure
                : await base.SendAsync(request, ct);
        }
    }

    private sealed class RecordingInbox : IRepository<AccountInboxRow>
    {
        public List<InboxEvent> Rows { get; } = [];
        public List<string> Calls { get; } = [];
        public BatchOutcomes? Recorded { get; private set; }

        public Task AddAsync(string entityId, DateTimeOffset eventTime, string payload, CancellationToken ct)
        {
            return Call($"add {entityId} at {eventTime:O}");
        }

        public Task<InboxClaim> ClaimAsync(CancellationToken ct)
        {
            Calls.Add("claim");
            return Task.FromResult(new InboxClaim(Rows));
        }

        public Task RecordAsync(InboxClaim claim, BatchOutcomes outcomes, CancellationToken ct)
        {
            Calls.Add("record");
            Recorded = outcomes;
            return Task.CompletedTask;
        }

        public Task ReleaseAsync(InboxClaim claim, Release release, CancellationToken ct)
        {
            return Call(release.StopInbox ? "halt" : release.Error is not null ? "hold" : $"defer {release.Delay}");
        }

        public Task FailAllAsync(InboxClaim claim, string error, CancellationToken ct)
        {
            return Call($"fail all: {error}");
        }

        private Task Call(string what)
        {
            Calls.Add(what);
            return Task.CompletedTask;
        }
    }
}