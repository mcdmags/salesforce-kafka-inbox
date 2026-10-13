# salesforce-kafka-inbox

Kafka change events → Postgres inbox → Salesforce, with per-entity ordering, atomic multi-record upserts and
type-aware change-only updates. One generic engine syncs every object; each object only defines its mapping and
queries. Written in C# / .NET 8.

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
5. **At-least-once delivery.** Kafka redelivers, workers crash, leases expire, and none of that may
   duplicate or regress data.

## How it's solved

| Problem | Solution |
|---|---|
| Durable, replayable intake | Kafka consumer writes each event's **raw JSON**, unchanged, to a Postgres **inbox** (`sf_inbox.payload`) before storing its offset (committed in the background, so there is no broker round trip per message). Entity type, key and order are read from the JSON (see [Event format](#event-format)), so any subscribed topic (`Kafka:Topics`) can carry any type. A unique index on (type, key, time, `md5(payload)`) makes redelivery harmless. A message that isn't a valid event (see below) is logged with its offset and skipped, rather than blocking its partition. |
| Ordering across parallel batches | Workers claim **entities, not events**: `SELECT … FOR UPDATE SKIP LOCKED` on each entity's *oldest pending* row, only for entities with nothing in flight. All pending events for a claimed entity are then leased (`locked_until`, `lock_token`, `retry_count`, `last_updated`). Two workers can never hold the same entity. |
| Bursts of updates to one entity | Pending events for an entity are **coalesced** (whole events deep-merged in `meta.time` order, then by id; absent field = unchanged, `null` = clear), so five updates in five seconds become one Salesforce write with the newest data. |
| Late or replayed older events | **Sync watermark in Postgres**: `sf_sync_state` holds the newest version (`meta.time`) synced per entity, advanced in the same transaction that completes the events and only while the worker still holds the lease. A claimed event at or below the watermark is marked `superseded` without touching Salesforce. Nothing is stored in Salesforce for this. |
| Expired leases | The Salesforce call is cancelled at 80% of the lease (measured from the claim), so a slow worker gives up before another worker can claim the entity. |
| Records that must change together | **Composite Graph API**: one graph per unit (Account + Profile, or a relationship). A graph is all-or-nothing; other graphs in the same request are unaffected. New records are referenced with `@{ref.id}`; existing ones (e.g. a relationship's Accounts) by the Id read from Salesforce. |
| Change-only updates | **Type-aware diff against Salesforce**: the current records are read by external id with every field the coalesced inbox state sets. Values are normalized with the fields' cached Describe metadata (number scale, checkbox `null` → `false`, 15 → 18-character ids, multi-select as a set, date/time to the millisecond, `""` = `null`) and only fields that differ are sent, in their normalized form, so a value Salesforce stores differently never rewrites itself on every event (a warning flags any field that still keeps differing). The comparison is with what is in Salesforce *now*, so edits made directly in Salesforce are corrected. |
| Batching and API calls | **2 calls per batch** of up to 75 units: one Composite request with all reads (≤5 queries), one Composite Graph with all writes (≤75 graphs / 500 nodes). Describe is cached and revalidated with `If-Modified-Since`. Request bodies are gzipped, responses accepted compressed. |
| Bad records | A value Salesforce would reject (unknown or read-only field, too long, wrong type, required field emptied) fails only its own unit, before any call; a malformed event is dead-lettered on its own (`INVALID_EVENT`). |
| Failures | Each graph's result decides per entity: **not attempted** (`PROCESSING_HALTED` after 14 failures, `ALL_OR_NONE_OPERATION_ROLLED_BACK`) → back to the inbox without spending a retry; **transient** (`UNABLE_TO_LOCK_ROW`, a lookup target not synced yet, unknown codes) → exponential backoff, dead-lettered after `MaxRetries`; **permanent** (validation rules, invalid fields, ...; configurable) → dead-lettered immediately. The code is stored in `sf_inbox.last_error_code`. |
| Postgres failures | Every unit of work (claim, outcomes, ingest, schema) runs through **`PostgresExecutionStrategy`**: a dropped connection, failover, timeout, deadlock or serialization failure re-runs the whole unit (new connection, new transaction) with backoff. After a lost connection the pool is cleared, since its idle connections are usually dead too. Every unit is safe to run twice; a claim retried after an unknown commit first releases what the lost attempt leased. A batch's outcomes are recorded with one statement per kind of outcome, not per entity. |

```
Kafka (keyed by entity) ──► KafkaInboxConsumer ──► sf_inbox (Postgres: raw event JSON + processing state;
  account-changes            validate, store raw       │  type / key / version read by SQL functions)
  account-relationship-changes                         │  claim entities: FOR UPDATE SKIP LOCKED on head row
                                                       ▼
                              InboxWorkerService ×N loops (any number of pods), round-robin over:
                                AccountSyncService ─┐            (strategy: map + queries)
                                AccountRelationshipSyncService ─┤
                                                                ▼
                              SalesforceBatchUpsertService.UpsertBatchAsync<TEvent>(strategy)
                                claim → supersede stale → coalesce → map → SalesforceRecordUpserter
                                (describe → normalize → 1 composite read → diff + lookups → 1 composite graph)
                                → record outcomes            all Postgres work via PostgresExecutionStrategy
                                                       │
                                                       ▼
                                                  Salesforce  (standard + existing fields only)
```

## Event format

The inbox stores only the raw Kafka message value. Every event is one entity object next to `meta`:

```json
{ "meta": { "time": "2026-10-14T01:02:03.456Z" }, "account": { "externalId": "ACC-000001", "Name": "Acme", "profile": { "Tier__c": "Gold" } } }
{ "meta": { "time": "2026-10-14T01:02:04.000Z" }, "accountRelationship": { "externalId": "REL-1", "accountExternalId": "ACC-000001", "relatedAccountExternalId": "ACC-000002", "Type__c": "Parent" } }
```

- **Entity type**: the name of the single top-level object besides `meta` (`account`, `accountRelationship`). It
  selects the strategy (`ISalesforceEntitySync.EntityType`).
- **Entity key**: `<object>.externalId`, the record's Salesforce external id (`External_Id__c`).
- **Order**: `meta.time`, an ISO-8601 time with an offset or `Z`. Events of one entity are processed in time order;
  two different events with the same time are both kept and ordered by when they were ingested (`id`).

Payload mapping (assumptions, documented on `AccountEvent` and `AccountRelationshipEvent`; change them there):

- **account**: every property of `account` except `externalId` and `profile` is an Account field, by its Salesforce API
  name. The optional `account.profile` becomes the Account's `Profile__c`: its properties except `externalId` are
  fields, and `profile.externalId` defaults to the account's `externalId`. Without a `profile`, only the Account is
  written.
- **accountRelationship**: `accountExternalId` and `relatedAccountExternalId` are the two Accounts (`Account__c`,
  `Related_Account__c`, looked up by external id); every other property except `externalId` is an
  `Account_Relationship__c` field. An event without both Account ids is dead-lettered (`INVALID_EVENT`).

**Ingest.** `KafkaInboxConsumer` checks each message before storing it: valid JSON, exactly one entity object besides
`meta`, a type some registered `ISalesforceEntitySync` handles, a non-empty string `externalId`, and a `meta.time`
with an offset. Anything else is logged with its offset and skipped. The Kafka message key isn't used (producers still
key by entity so Kafka keeps each entity's events in order).

**The inbox table** (`sql/schema.sql`) has no event columns: `id`, `payload jsonb` and the processing state (`status`,
`retry_count`, `last_updated`, `last_error`, `last_error_code`, `next_attempt_at`, `locked_until`, `lock_token`,
`received_at`). Three SQL functions read the event, so the JSON paths live in one place:

| Function | Returns |
|---|---|
| `sf_event_type(payload)` | the top-level key other than `meta` |
| `sf_event_key(payload)` | `payload -> type ->> 'externalId'` |
| `sf_event_version(payload)` | `meta.time` in microseconds since the epoch (`bigint`) |

They are `IMMUTABLE`, so they back expression indexes (pending head, ready, leased, and the unique ingest index), and
every claim query is written with exactly the same expressions so the planner uses them. A text to `timestamptz` cast
is really only stable (it depends on the session's time zone for a time without an offset); declaring it immutable is
safe only because the consumer admits nothing but times with an explicit offset. Don't insert events another way
without the same check. `sf_sync_state (entity_type, entity_key, synced_version)` is processing state, so it keeps its
columns.

A database whose `sf_inbox` still has the old `entity_key` column is upgraded at startup: the old table is renamed to
`sf_inbox_legacy` (rows kept for inspection, not migrated) and the new one is created.

```sql
-- Dead letters of one type, by cause
select sf_event_key(payload) as key, payload -> 'meta' ->> 'time' as time, last_error_code, last_error
from sf_inbox where status = 'dead' and sf_event_type(payload) = 'account' order by id desc;

-- Everything the inbox holds for one entity, in processing order
select id, status, retry_count, payload from sf_inbox
where sf_event_type(payload) = 'account' and sf_event_key(payload) = 'ACC-000001'
order by sf_event_version(payload), id;
```

## Adding a Salesforce object

The engine (`SalesforceBatchUpsertService.UpsertBatchAsync<TEvent>`) owns claiming, ordering, coalescing,
batching, normalization, diffing, lookups, retries and dead-lettering. An object only supplies an
`UpsertStrategy<TEvent>` (or the same three things as delegates):

```csharp
// The event envelope: { "meta": {...}, "accountRelationship": { "externalId": ..., ... } }
public sealed record AccountRelationshipEvent(AccountRelationshipData? AccountRelationship);

public sealed record AccountRelationshipData
{
    public string? ExternalId { get; init; }
    public string? AccountExternalId { get; init; }
    public string? RelatedAccountExternalId { get; init; }
    [JsonExtensionData] public Dictionary<string, JsonElement>? Fields { get; init; }  // every other property
}

public sealed class AccountRelationshipSyncService(SalesforceBatchUpsertService upsert) : ISalesforceEntitySync
{
    public static readonly UpsertStrategy<AccountRelationshipEvent> Strategy = new("accountRelationship", Map, Queries);

    public string EntityType => "accountRelationship";  // the event's entity object name
    public Task<int> SyncBatchAsync(CancellationToken ct) => upsert.UpsertBatchAsync(Strategy, ct);

    // Inbox event (the entity's events merged) -> the records to write together (one graph).
    // Throwing dead-letters the event (INVALID_EVENT).
    public static UpsertUnit Map(string key, AccountRelationshipEvent e)
    {
        var rel = e.AccountRelationship;
        if (string.IsNullOrWhiteSpace(rel?.AccountExternalId) || string.IsNullOrWhiteSpace(rel.RelatedAccountExternalId))
            throw new ArgumentException("accountExternalId and relatedAccountExternalId are required");

        return new UpsertUnit(
        [
            new SObjectRecord("rel", "Account_Relationship__c", "External_Id__c", key, SObjectFields.From(rel.Fields),
            [
                new SObjectLookup("Account__c", "Account", "External_Id__c", rel.AccountExternalId),
                new SObjectLookup("Related_Account__c", "Account", "External_Id__c", rel.RelatedAccountExternalId)
            ])
        ]);
    }

    // What to read: the relationships themselves (to diff) and the Accounts they point at (for their Ids)
    public static IEnumerable<SoqlQuery> Queries(IReadOnlyCollection<UpsertUnit> units) =>
    [
        new SoqlQuery("Account_Relationship__c", "External_Id__c", SoqlQuery.ExternalIdsOf(units, "Account_Relationship__c")),
        new SoqlQuery("Account", "External_Id__c", SoqlQuery.LookupIdsOf(units, "Account"))
    ];
}
```

Register it (`AddTransient<ISalesforceEntitySync, ...>()` in `AddInboxSync`); from then on the consumer admits events
whose entity object has that name. If they come on a new topic, add it to `Kafka:Topics`. A lookup whose target isn't in Salesforce yet (`DEPENDENCY_NOT_SYNCED`) is retried with backoff,
so related objects can arrive in any order.

**Why `SemaphoreSlim` isn't used for parallel batches:** it only coordinates within one process. Parallelism
comes from competing worker loops (`Sync:Workers`) in every pod pulling from the shared inbox, kept off each
other's entities by the `FOR UPDATE SKIP LOCKED` claim. (The Describe cache does use a `SemaphoreSlim`, as an
in-process single-flight so concurrent workers don't all describe at once.)

## Repository layout

```
src/
  SalesforceSync/                 Worker: Kafka consumer, sync engine + strategies (Sync/), Salesforce client,
                                  Postgres execution strategy (Data/)
  SalesforceSync.FakeSalesforce/  In-memory Salesforce (describe, query, composite, composite graph, Salesforce-like
                                  value storage, error injection, write history, Sforce-Limit-Info)
  SalesforceSync.FakeSalesforce.Server/  The in-memory org as an HTTP service (+ /__fake inspection API)
  SalesforceSync.LoadGenerator/   Produces account change (with bursts) and relationship events to Kafka
  SalesforceSync.AppHost/         Aspire: Postgres + pgAdmin, Kafka + Kafka UI, mock Salesforce, worker, generator, benchmarks
tests/
  SalesforceSync.Tests/           Unit tests: the Salesforce half of the engine with both strategies against the
                                  in-memory org, type normalization, error classification, execution strategy
  SalesforceSync.IntegrationTests/  End-to-end tests: Aspire app, Kafka in, assertions on the mock org + inbox
benchmarks/
  SalesforceSync.Benchmarks/      BenchmarkDotNet: real Postgres inbox, fake Salesforce HTTP
sql/
  schema.sql                      Inbox table, event functions + indexes, sync watermark (embedded, applied at startup)
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
write, exactly 2 API calls per batch, a redelivered event stored once (and a time without an offset skipped), the
schema being reapplied on restart, and an old-format inbox kept aside as `sf_inbox_legacy`. Tests publish version N
as `meta.time` = a fixed base time + N ms, and read it back the same way. Relationships
(`RelationshipTests`): linked to two synced Accounts with only changed fields sent later; a relationship that
arrives before its Accounts waits (`DEPENDENCY_NOT_SYNCED`, retried) and syncs once they exist; a malformed
relationship event is dead-lettered alone. Writes are checked with the mock's per-record write history
(`/__fake/history/{sobject}/{externalId}`).

Unit tests (`dotnet test tests/SalesforceSync.Tests`, no Docker) run the Salesforce half of the engine with both
strategies against the in-memory org: 2 calls per batch, values written once instead of flapping, a bad value
failing only its own unit before any call, validation / lock / halted outcomes, relinking, relationships and
missing dependencies, gzip. Also: the event mapping (account with and without a profile, relationships, missing
Account ids), merging whole events, what the consumer admits (`InboxEventEnvelope`), every normalization rule (including the documented 15 → 18-character id
example), error classification and the Postgres execution strategy (retried, not retried, gives up).

### Concurrency: many threads, many pods, same accounts

`ConcurrencyTests` scales the worker like a Kubernetes deployment: **3 replicas x 8 worker loops**, all on one
inbox and one org, while 60 accounts each receive 25 updates in waves (new events keep arriving while those
accounts are being processed). Salesforce has nothing that rejects an out-of-order write, so a race would
land in the org and show up in the mock's write history. Batches are small (10 accounts) so workers contend for
the same accounts. The test asserts:

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

Only the Salesforce HTTP endpoint is mocked; the Postgres inbox, workers and client are real.

```bash
# use a dedicated database: the benchmark truncates sf_inbox
export ConnectionStrings__benchdb="Host=localhost;Port=5432;Database=benchdb;Username=postgres;Password=postgres"

dotnet run -c Release --project benchmarks/SalesforceSync.Benchmarks -- --filter '*InboxDrain*'
dotnet run -c Release --project benchmarks/SalesforceSync.Benchmarks -- --filter '*Micro*'
```

Both drain benchmarks run P **pods** (independent service providers: own connection pool and HTTP client,
like separate processes) x W worker loops against one Postgres inbox and one org. Each iteration
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

1. No fields need to be added for the sync itself. Account, `Profile__c` and `Account_Relationship__c` need
   their external id fields (marked **Unique**), the `Account__c` lookup on the Profile and the `Account__c` /
   `Related_Account__c` lookups on the relationship.
2. Set `Salesforce:UseFake=false` (the default outside Aspire) plus `InstanceUrl`, `TokenUrl`,
   `ClientId`, `ClientSecret` (OAuth client credentials).
3. Adjust object and field names in the strategies (`AccountSyncService`, `AccountRelationshipSyncService`),
   and tune `Salesforce:PermanentErrorCodes`, `Sync:RetryBaseDelay` / `MaxRetryDelay` / `MaxRetries` and
   `Postgres:Retry` if needed. `Sync` values that can't work (no workers, `BatchSize` outside 1-75, ...)
   and a missing `Salesforce:InstanceUrl` fail startup.

## Status

Scaffold, not yet production-tested. Verify in a Salesforce sandbox, in particular concurrent workers on
the same account and killing a worker mid-batch.

## License

[MIT](LICENSE)
