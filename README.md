# salesforce-kafka-inbox

Kafka change events → Postgres inbox → Salesforce, with per-entity ordering, atomic multi-record upserts,
type-aware change-only updates and a shared, limit-aware rate limiter. One generic engine syncs every object;
each object only defines its mapping and queries. Written in C# / .NET 8.

## The problem

An upstream system publishes account changes and account-to-account relationships to Kafka. Each account
change has to land in Salesforce as an update to an **Account** and its related **Profile**; each relationship
as an **Account_Relationship__c** linking two Accounts, which may not be synced yet when the relationship arrives. Several things make this harder than
"consume and PATCH":

1. **Out-of-order writes across parallel batches.** The same account can change twice within a few
   seconds. If those two events land in different batches running in parallel, the newer one can
   reach Salesforce first and then be overwritten by the older one, so Salesforce ends up with stale
   data.
2. **Account and Profile must change together.** A failure must never leave an updated Account with an
   old Profile (or the reverse), but one bad pair must not roll back everyone else in the batch.
3. **Only send what changed.** Blind updates fire triggers and flows, pollute field history and burn
   API calls. Each record is read, diffed and only the changed fields are sent; new records are created.
   Nothing extra may be stored in Salesforce for the sync's own bookkeeping (no custom version field).
4. **Batching.** Salesforce limits are per call, so updates have to be grouped efficiently.
5. **Rate limits shared by every worker.** The org has a daily API allocation (also used by other
   integrations), a burst allowance and a cap on concurrent long-running requests. Many workers in
   many pods must respect them together, and back off when Salesforce says so.
6. **At-least-once delivery.** Kafka redelivers, workers crash, leases expire, and none of that may
   duplicate or regress data.

## How it's solved

| Problem | Solution |
|---|---|
| Durable, replayable intake | Kafka consumer writes each event to a Postgres **inbox** (`sf_inbox`) before committing the offset. Each topic is one entity type (`Kafka:Topics`); `unique (entity_type, entity_key, version)` makes redelivery harmless. |
| Ordering across parallel batches | Workers claim **entities, not events**: `SELECT … FOR UPDATE SKIP LOCKED` on each entity's *oldest pending* row, only for entities with nothing in flight. All pending events for a claimed entity are then leased (`locked_until`, `lock_token`, `retry_count`, `last_updated`). Two workers can never hold the same entity. |
| Bursts of updates to one entity | Pending events for an entity are **coalesced** (merged in version order; absent field = unchanged, `null` = clear), so five updates in five seconds become one Salesforce write with the newest data. |
| Late or replayed older events | **Sync watermark in Postgres**: `sf_sync_state` holds the newest version synced per entity, advanced in the same transaction that completes the events and only while the worker still holds the lease. A claimed event at or below the watermark is marked `superseded` without touching Salesforce. Nothing is stored in Salesforce for this. |
| Expired leases | The Salesforce call is cancelled at 80% of the lease, so a slow worker gives up before another worker can claim the entity. |
| Records that must change together | **Composite Graph API**: one graph per unit (Account + Profile, or a relationship). A graph is all-or-nothing; other graphs in the same request are unaffected. New records are referenced with `@{ref.id}`; existing ones (e.g. a relationship's Accounts) by the Id read from Salesforce. |
| Change-only updates | **Type-aware diff against Salesforce**: the current records are read by external id with every field the coalesced inbox state sets. Values are normalized with the fields' cached Describe metadata (number scale, checkbox `null` → `false`, 15 → 18-character ids, multi-select as a set, date/time to the millisecond, `""` = `null`) and only fields that differ are sent, in their normalized form, so a value Salesforce stores differently never rewrites itself on every event (a warning flags any field that still keeps differing). The comparison is with what is in Salesforce *now*, so edits made directly in Salesforce are corrected. |
| Batching and API calls | **2 calls per batch** of up to 75 units: one Composite request with all reads (≤5 queries), one Composite Graph with all writes (≤75 graphs / 500 nodes). Describe is cached and revalidated with `If-Modified-Since`. Request bodies are gzipped, responses accepted compressed. |
| Bad records | A value Salesforce would reject (unknown or read-only field, too long, wrong type, required field emptied) fails only its own unit, before any call; a malformed event is dead-lettered on its own (`INVALID_EVENT`). |
| Failures | Each graph's result decides per entity: **not attempted** (`PROCESSING_HALTED` after 14 failures, `ALL_OR_NONE_OPERATION_ROLLED_BACK`) → back to the inbox without spending a retry; **transient** (`UNABLE_TO_LOCK_ROW`, a lookup target not synced yet, unknown codes) → exponential backoff, dead-lettered after `MaxRetries`; **permanent** (validation rules, invalid fields, ...; configurable) → dead-lettered immediately. The code is stored in `sf_inbox.last_error_code`. |
| Postgres failures | Every unit of work (claim, outcomes, ingest, rate limiter, slots, schema) runs through **`PostgresExecutionStrategy`**: a dropped connection, failover, timeout, deadlock or serialization failure re-runs the whole unit (new connection, new transaction) with backoff. Every unit is safe to run twice. |
| Rate limits | Configured from Salesforce's published limits (see [Salesforce API limits](#salesforce-api-limits)): Postgres-backed **token bucket** shared by all processes; this integration's **share of the org's daily allocation** enforced as a hard cap over a rolling 24 hours; global **concurrency cap** (advisory locks) validated against the org's concurrent long-running limit; the `Sforce-Limit-Info` header (`api-usage`, `api-bursts`, `per-app-api-usage`) and own usage slow everyone down at 80% and pause at 95%; 429/503 and both kinds of `REQUEST_LIMIT_EXCEEDED` pause all workers. Rate-limited batches are handed back without spending a retry. |

```
Kafka (keyed by entity) ──► KafkaInboxConsumer ──► sf_inbox (Postgres, entity_type + entity_key)
  account-changes                                      │  claim entities: FOR UPDATE SKIP LOCKED on head row
  account-relationship-changes                         ▼
                              InboxWorkerService ×N loops (any number of pods), round-robin over:
                                AccountSyncService ─┐            (strategy: map + queries)
                                AccountRelationshipSyncService ─┤
                                                                ▼
                              SalesforceBatchUpsertService.UpsertBatchAsync<TEvent>(strategy)
                                claim → supersede stale → coalesce → map → SalesforceRecordUpserter
                                (describe → normalize → 1 composite read → diff + lookups → 1 composite graph)
                                → record outcomes            all Postgres work via PostgresExecutionStrategy
                                                       │  token bucket + daily budget + concurrency slots
                                                       ▼
                                                  Salesforce  (standard + existing fields only)
```

## Adding a Salesforce object

The engine (`SalesforceBatchUpsertService.UpsertBatchAsync<TEvent>`) owns claiming, ordering, coalescing, rate
limits, batching, normalization, diffing, lookups, retries and dead-lettering. An object only supplies an
`UpsertStrategy<TEvent>` (or the same three things as delegates):

```csharp
public sealed class AccountRelationshipSyncService(SalesforceBatchUpsertService upsert) : ISalesforceEntitySync
{
    public static readonly UpsertStrategy<AccountRelationshipChangedEvent> Strategy = new("account-relationship", Map, Queries);

    public string EntityType => "account-relationship";
    public Task<int> SyncBatchAsync(CancellationToken ct) => upsert.UpsertBatchAsync(Strategy, ct);

    // Inbox event -> the records to write together (one graph)
    public static UpsertUnit Map(string key, AccountRelationshipChangedEvent e) => new(
    [
        new SObjectRecord("rel", "Account_Relationship__c", "External_Id__c", key, SObjectFields.From(e.Fields),
        [
            new SObjectLookup("Account__c", "Account", "External_Id__c", e.AccountExternalId!),
            new SObjectLookup("Related_Account__c", "Account", "External_Id__c", e.RelatedAccountExternalId!)
        ])
    ]);

    // What to read: the relationships themselves (to diff) and the Accounts they point at (for their Ids)
    public static IEnumerable<SoqlQuery> Queries(IReadOnlyCollection<UpsertUnit> units) =>
    [
        new SoqlQuery("Account_Relationship__c", "External_Id__c", SoqlQuery.ExternalIdsOf(units, "Account_Relationship__c")),
        new SoqlQuery("Account", "External_Id__c", SoqlQuery.LookupIdsOf(units, "Account"))
    ];
}
```

Register it (`AddTransient<ISalesforceEntitySync, ...>()`) and map its Kafka topic to its entity type under
`Kafka:Topics`. A lookup whose target isn't in Salesforce yet (`DEPENDENCY_NOT_SYNCED`) is retried with backoff,
so related objects can arrive in any order.

**Why `SemaphoreSlim` isn't used for parallel batches:** it limits concurrency within one process, while
Salesforce's limits are org-wide. Parallelism comes from competing worker loops in every pod pulling from the
shared inbox, capped globally by Postgres advisory-lock slots and a shared token bucket. (The Describe cache
does use a `SemaphoreSlim`, as an in-process single-flight so concurrent workers don't all describe at once.)

## Repository layout

```
src/
  SalesforceSync/                 Worker: Kafka consumer, sync engine + strategies (Sync/), Salesforce client,
                                  rate limiting, Postgres execution strategy (Data/)
  SalesforceSync.FakeSalesforce/  In-memory Salesforce (describe, query, composite, composite graph, Salesforce-like
                                  value storage, error injection, write history, Sforce-Limit-Info)
  SalesforceSync.FakeSalesforce.Server/  The in-memory org as an HTTP service (+ /__fake inspection API)
  SalesforceSync.LoadGenerator/   Produces account change (with bursts) and relationship events to Kafka
  SalesforceSync.AppHost/         Aspire: Postgres + pgAdmin, Kafka + Kafka UI, mock Salesforce, worker, generator, benchmarks
tests/
  SalesforceSync.Tests/           Unit tests: the Salesforce half of the engine with both strategies against the
                                  in-memory org, type normalization, error classification, execution strategy, limits
  SalesforceSync.IntegrationTests/  End-to-end tests: Aspire app, Kafka in, assertions on the mock org + inbox
benchmarks/
  SalesforceSync.Benchmarks/      BenchmarkDotNet: real Postgres inbox, fake Salesforce HTTP
sql/
  schema.sql                      Inbox, sync watermark, rate-limit and API usage tables (embedded, applied at worker startup)
  claim-audit.sql                 Test/benchmark-only trigger that records every entity claim
```

## Run everything with Aspire

Requires Docker (or OrbStack/Colima) and the .NET 8+ SDK.

```bash
dotnet run --project src/SalesforceSync.AppHost
```

The dashboard shows:

- **postgres / inboxdb / benchdb** (+ pgAdmin)
- **kafka** (+ Kafka UI)
- **fake-salesforce**: mock Salesforce org over HTTP (OAuth token, describe, query, composite, composite graph,
  gzip). Logs its counters every 15s; inspect it at `/__fake/stats`, `/__fake/transport`,
  `/__fake/records/{sobject}[/{externalId}]` and `/__fake/history/{sobject}/{externalId}`; inject errors with
  `POST /__fake/faults`
- **salesforce-sync**: consumes `account-changes` and `account-relationship-changes` into `sf_inbox` and syncs
  to `fake-salesforce` through the real OAuth + REST client
- **load-generator**: 20k account events over 500 accounts at 200/s, including bursts to the same account, plus
  relationship events between accounts
- **benchmarks**: explicit start (click ▶), runs the inbox drain benchmark against `benchdb`

## Integration tests

End-to-end tests start the Aspire app (`--AppHost:TestMode=true`: throwaway Postgres + Kafka, the worker
and the mock Salesforce; no load generator, admin UIs or data volume), publish events to Kafka and assert
on the records in the mock org and the `sf_inbox` status. Requires Docker.

```bash
dotnet test tests/SalesforceSync.IntegrationTests
```

Covered: create Account + linked Profile, partial update (only the differing field is sent), the diff
being taken against the record in Salesforce (a direct edit in Salesforce is corrected by the next event),
bursts coalesced to the newest state, stale versions superseded without a write, unchanged events skip the
write, exactly 2 API calls per batch, and the schema being reapplied on restart. Relationships
(`RelationshipTests`): linked to two synced Accounts with only changed fields sent later; a relationship that
arrives before its Accounts waits (`DEPENDENCY_NOT_SYNCED`, retried) and syncs once they exist; a malformed
relationship event is dead-lettered alone. Writes are checked with the mock's per-record write history
(`/__fake/history/{sobject}/{externalId}`).

Rate limits (`RateLimitTests`):

- **Daily budget across pods**: 3 replicas share a budget of 30 calls (`DailyApiAllocation=60`,
  `DailyBudgetShare=0.5`). Exactly 30 calls reach the org, then nothing more; the remaining events are deferred
  until the budget frees up, without spending retries or being dead-lettered.
- **Concurrent long-running limit**: the mock org allows 2 concurrent long-running calls while 12 workers send
  them. The `ConcurrentPerOrgLongTxn` rejections cause a short pause (not the long daily one) and every event
  still syncs, with no retries spent.

Unit tests (`dotnet test tests/SalesforceSync.Tests`, no Docker) run the Salesforce half of the engine with both
strategies against the in-memory org: 2 calls per batch, values written once instead of flapping, a bad value
failing only its own unit before any call, validation / lock / halted outcomes, relinking, relationships and
missing dependencies, gzip. Also: every normalization rule (including the documented 15 → 18-character id
example), error classification, the Postgres execution strategy (retried, not retried, gives up), the allocation
formula (including the documented Enterprise example of 115,000), startup validation and throttle decisions.

### Concurrency: many threads, many pods, same accounts

`ConcurrencyTests` scales the worker like a Kubernetes deployment: **3 replicas x 8 worker loops**, all on one
inbox and one org, while 60 accounts each receive 25 updates in waves (new events keep arriving while those
accounts are being processed). Salesforce has nothing that rejects an out-of-order write, so a race would
land in the org and show up in the mock's write history. The rate limiter is opened up and batches are small
(10 accounts) so workers contend for the same accounts. The test asserts:

- **No overlapping claims**: `sql/claim-audit.sql` (test-only trigger) records every account claim from the
  moment a worker sets `lock_token` until that worker clears it, ordered by a sequence; no two claims on the
  same account may overlap.
- No out-of-order write: in every record's write history, the version carried in the data (`Description`
  `vN` on the Account, `Score__c` on the Profile) only moves forward. No event was superseded.
- Every Account and Profile ends with the version 25 values.
- Every account was claimed repeatedly (real contention) and all 3 pods wrote to Salesforce (`/__fake/clients`).

The test is known to catch races: with the `FOR UPDATE SKIP LOCKED` / in-flight check removed from
`InboxSql.LockHeads`, it fails with over ten thousand overlapping claims.

`Pods_survive_their_Postgres_connections_being_dropped` repeats this while every worker and consumer connection
is terminated (`pg_terminate_backend`) every 200 ms: every event still ends `done`, with no overlapping claims and
no out-of-order writes.

## Benchmarks

Only the Salesforce HTTP endpoint is mocked; the Postgres inbox, workers, client and rate limiter are real.

```bash
# use a dedicated database: the benchmark truncates sf_inbox and resets sf_rate_limit
export ConnectionStrings__benchdb="Host=localhost;Port=5432;Database=benchdb;Username=postgres;Password=postgres"

dotnet run -c Release --project benchmarks/SalesforceSync.Benchmarks -- --filter '*InboxDrain*'
dotnet run -c Release --project benchmarks/SalesforceSync.Benchmarks -- --filter '*Micro*'
```

Both drain benchmarks run P **pods** (independent service providers: own connection pool, rate limiter and
HTTP client, like separate processes) x W worker loops against one Postgres inbox and one org. Each iteration
**fails** if any account doesn't end at its newest version, any record's write history shows an older version
after a newer one, any inbox row isn't `done` (events are ingested in order, so nothing should be superseded),
or the claim audit shows two overlapping claims on the same account. Each iteration prints a `//` line with
fake-org counters, claims, overlaps and regressions.

- `InboxDrainBenchmarks`: the whole inbox is loaded, then drained. Params: `Pods` (1/3), `Workers` (1/4/8),
  `Accounts` (100/1000), `LatencyMs` (0/25), `Events` (5000).
- `StreamingIngestBenchmarks`: events keep arriving while the pods drain (batches of 10 accounts), so the
  same account is claimed, released and re-claimed by different workers in different pods. Params: `Pods`
  (1/3), `Workers` (4/8), 50 accounts, 5000 events, 5 ms latency.

```bash
dotnet run -c Release --project benchmarks/SalesforceSync.Benchmarks -- --filter '*StreamingIngest*'
```

## Salesforce API limits

Configured from "API Request Limits and Allocations" in the
[Salesforce Developer Limits and Allocations Quick Reference](https://developer.salesforce.com/docs/platform/salesforce-app-limits-cheatsheet/guide/salesforce-app-limits-platform-api.html).
Describe the org and the limits are derived; the worker logs what it derived at startup and refuses to start on
a configuration that can't work.

```json
"SalesforceLimits": {
  "OrgType": "Production",          // Production | Sandbox | FullSandbox | DeveloperEdition | Trial
  "Edition": "Enterprise",          // Enterprise | Professional | Unlimited | Performance
  "Licenses": { "Salesforce": 15, "Partner Community": 200 },
  "CallsPerLicense": {},            // add license types / override the built-in table
  "PurchasedApiCalls": 0,           // API Call Add-Ons
  "DailyApiAllocation": null,       // or copy the number from Setup (needed for template-based sandboxes)
  "ConcurrentLongRunningLimit": null,
  "LongRunningThreshold": "00:00:20",
  "RequestTimeout": "00:01:30"
},
"RateLimit": {
  "MaxRequestsPerSecond": 2,        // token bucket refill, all pods together
  "BurstCapacity": 5,               // token bucket size
  "DailyBudgetShare": 0.5,          // this integration's share of the org's daily allocation
  "DailySoftRatio": 0.8, "DailySoftSpeed": 0.25,
  "DailyHardRatio": 0.95, "DailyHardPause": "00:15:00",
  "BurstSoftRatio": 0.8, "BurstSoftSpeed": 0.5, "BurstPause": "00:00:10",
  "ConcurrentLimitPause": "00:00:30",
  "MaxWait": "00:00:20"
}
```

| Salesforce limit | How it's applied |
|---|---|
| **Total API requests per 24 hours**: Developer Edition 15,000; Full Sandbox 5,000,000; otherwise 100,000 + licenses × calls per license type (Enterprise/Professional: Salesforce 1,000, Platform 1,000, One App 200, Customer Community Plus 200, Partner Community 200, ...; Unlimited/Performance: Salesforce 5,000, Platform 5,000, ...) + purchased add-ons | Computed from `SalesforceLimits` (or `DailyApiAllocation`). The allocation is shared by every user and integration of the org, so this integration gets `DailyBudgetShare` of it. Every call is counted in `sf_api_usage` (10-minute buckets) and checked under the shared row lock, so all pods together never exceed the budget over any rolling 24 hours. When it's used up, events are deferred until the oldest calls leave the window. Usage is rolling, not reset at midnight. |
| **Org-wide usage** (also other integrations) | `Sforce-Limit-Info: api-usage` and `per-app-api-usage`: slow down at `DailySoftRatio`, pause at `DailyHardRatio`. A warning is logged if Salesforce reports a different allocation than configured. |
| **Concurrent long-running requests** (20 s or longer): 25 for production and sandboxes, 5 for Developer Edition and trials | `Sync:MaxConcurrentRequests` (global advisory-lock slots) must not exceed it, or startup fails. Calls that take `LongRunningThreshold` or longer are logged. `REQUEST_LIMIT_EXCEEDED` with `ConcurrentPerOrgLongTxn` pauses all workers for `ConcurrentLimitPause`. |
| **Total requests exceeded** (`REQUEST_LIMIT_EXCEEDED`, `TotalRequests`) | Pauses all workers for `DailyHardPause`. Salesforce may allow some overage on paid orgs, but it isn't relied on. |
| **API timeout**: 10 minutes for REST calls, for a composite request as a whole | `RequestTimeout` (HTTP client timeout, including the token wait) must be at most 10 minutes, or startup fails. The worker also gives up at 80% of its lease. |

Every pod applies the bucket size, rate and budget to the shared `sf_rate_limit` row at startup, so roll out
configuration changes to all pods together. `sf_limit_status` shows the last reported usage, this integration's
own calls over the last 24 hours and its budget.

## Using a real Salesforce org

1. No fields need to be added for the sync itself. Account, `Profile__c` and `Account_Relationship__c` need
   their external id fields (marked **Unique**), the `Account__c` lookup on the Profile and the `Account__c` /
   `Related_Account__c` lookups on the relationship.
2. Set `Salesforce:UseFake=false` (the default outside Aspire) plus `InstanceUrl`, `TokenUrl`,
   `ClientId`, `ClientSecret` (OAuth client credentials).
3. Describe the org in `SalesforceLimits` and choose this integration's `RateLimit:DailyBudgetShare`
   (see [Salesforce API limits](#salesforce-api-limits)).
4. Adjust object and field names in the strategies (`AccountSyncService`, `AccountRelationshipSyncService`),
   and tune `Salesforce:PermanentErrorCodes`, `Sync:RetryBaseDelay` / `MaxRetryDelay` / `MaxRetries` and
   `Postgres:Retry` if needed.

## Status

Scaffold, not yet production-tested. Verify in a Salesforce sandbox, in particular concurrent workers on
the same account and killing a worker mid-batch.
