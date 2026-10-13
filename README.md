# salesforce-kafka-inbox

Kafka change events → Postgres inbox → Salesforce, with per-account ordering, Account + Profile
atomicity, change-only updates and a shared, limit-aware rate limiter. Written in C# / .NET 8.

## The problem

An upstream system publishes account changes to Kafka. Each change has to land in Salesforce as an
update to an **Account** and its related **Profile** record. Several things make this harder than
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
| Durable, replayable intake | Kafka consumer writes each event to a Postgres **inbox** (`sf_inbox`) before committing the offset. `unique (account_ext_id, version)` makes redelivery harmless. |
| Ordering across parallel batches | Workers claim **accounts, not events**: `SELECT … FOR UPDATE SKIP LOCKED` on each account's *oldest pending* row, only for accounts with nothing in flight. All pending events for a claimed account are then leased (`locked_until`, `lock_token`, `retry_count`, `last_updated`). Two workers can never hold the same account. |
| Bursts of updates to one account | Pending events for an account are **coalesced** (merged in version order), so five updates in five seconds become one Salesforce write with the newest data. |
| Late or replayed older events | **Sync watermark in Postgres**: `sf_sync_state` holds the newest version synced per account, advanced in the same transaction that completes the events and only while the worker still holds the lease. A claimed event at or below the watermark is marked `superseded` without touching Salesforce. Nothing is stored in Salesforce for this. |
| Expired leases | The Salesforce call is cancelled at 80% of the lease, so a slow worker gives up before another worker can claim the account. |
| Account + Profile atomicity | **Composite Graph API**: one graph per Account/Profile pair. A graph is all-or-nothing; other graphs in the same request are unaffected. New records use `@{acct.id}` references. |
| Change-only updates | **Full diff against Salesforce**: the current Account/Profile are queried by external id with every field the coalesced inbox state sets, each field is compared (type-aware: numbers, dates, `""` vs `null`), and only fields that differ are sent. The comparison is with what is in Salesforce *now*, not with the previous event, so edits made directly in Salesforce are corrected. If nothing differs, nothing is written. |
| Batching | Up to 75 graphs / 500 nodes per request (Salesforce limits); `PROCESSING_HALTED` graphs are retried. |
| Rate limits | Postgres-backed **token bucket** shared by all processes; global **concurrency cap** via advisory locks; the `Sforce-Limit-Info` header (`api-usage`, `api-bursts`, `per-app-api-usage`) slows everyone down near 80% of the daily allocation and pauses at 95%; 429/503/`REQUEST_LIMIT_EXCEEDED` pause all workers. Rate-limited batches are handed back without spending a retry. |
| Failures | Exponential backoff per account, dead-lettering after `MaxRetries`, retries are safe because each graph rolled back fully. |

```
Kafka (keyed by account) ──► KafkaInboxConsumer ──► sf_inbox (Postgres)
                                                       │  claim accounts: FOR UPDATE SKIP LOCKED on head row
                                                       ▼
                                       InboxWorker ×N (any number of pods)
                                         coalesce → query + diff → Composite Graph (1 graph / pair)
                                                       │  token bucket + concurrency slots (Postgres)
                                                       ▼
                                                  Salesforce  (standard + existing fields only)
```

## Repository layout

```
src/
  SalesforceSync/                 Worker: Kafka consumer, inbox workers, Salesforce client, rate limiting
  SalesforceSync.FakeSalesforce/  In-memory Salesforce (query, composite graph, write history, Sforce-Limit-Info)
  SalesforceSync.FakeSalesforce.Server/  The in-memory org as an HTTP service (+ /__fake inspection API)
  SalesforceSync.LoadGenerator/   Produces account change events (with bursts) to Kafka
  SalesforceSync.AppHost/         Aspire: Postgres + pgAdmin, Kafka + Kafka UI, mock Salesforce, worker, generator, benchmarks
tests/
  SalesforceSync.IntegrationTests/  End-to-end tests: Aspire app, Kafka in, assertions on the mock org + inbox
benchmarks/
  SalesforceSync.Benchmarks/      BenchmarkDotNet: real Postgres inbox, fake Salesforce HTTP
sql/
  schema.sql                      Inbox, sync watermark and rate-limit tables (embedded and applied at worker startup)
  claim-audit.sql                 Test/benchmark-only trigger that records every account claim
```

## Run everything with Aspire

Requires Docker (or OrbStack/Colima) and the .NET 8+ SDK.

```bash
dotnet run --project src/SalesforceSync.AppHost
```

The dashboard shows:

- **postgres / inboxdb / benchdb** (+ pgAdmin)
- **kafka** (+ Kafka UI)
- **fake-salesforce**: mock Salesforce org over HTTP (OAuth token, query, composite graph). Logs its
  counters every 15s; inspect it at `/__fake/stats` and `/__fake/records/{Account|Profile__c}[/{externalId}]`
- **salesforce-sync**: consumes `account-changes` into `sf_inbox` and syncs to `fake-salesforce`
  through the real OAuth + REST client
- **load-generator**: 20k events over 500 accounts at 200/s, including bursts to the same account
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
write, and the schema being reapplied on restart. Writes are checked with the mock's per-record write
history (`/__fake/history/{sobject}/{externalId}`).

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

## Using a real Salesforce org

1. No fields need to be added for the sync itself. Account and `Profile__c` need their external id fields
   (marked **Unique**) and the `Account__c` lookup on the Profile.
2. Set `Salesforce:UseFake=false` (the default outside Aspire) plus `InstanceUrl`, `TokenUrl`,
   `ClientId`, `ClientSecret` (OAuth client credentials).
3. Adjust object/field names in `AccountProfileSync` and the event mapping in `InboxWorker.MapToSyncItem`.

## Status

Scaffold, not yet production-tested. Verify in a Salesforce sandbox, in particular concurrent workers on
the same account and killing a worker mid-batch.
