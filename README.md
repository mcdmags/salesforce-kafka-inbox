# salesforce-kafka-inbox

Kafka change events → Postgres inbox → Salesforce, with per-entity ordering, atomic multi-record upserts,
type-aware change-only updates and a shared, limit-aware rate limiter. One generic engine syncs every object;
each object only defines its typed payload, its Salesforce sObject class, the mapper between them and its queries.
Written in C# / .NET 8.

## The problem

An upstream system publishes account changes and account-to-account relationships to Kafka. Each account
change has to land in Salesforce as an update to an **Account** and its related **Profile**; each relationship
as an **Account_Relationship__c** linking two Accounts, which may not be synced yet when the relationship arrives. Several things make this harder than
"consume and PATCH":

1. **Out-of-order writes across parallel batches.** The same account can change twice within a few
   seconds. If those two events land in different batches running in parallel, the newer one can
   reach Salesforce first and then be overwritten by the older one, so Salesforce ends up with stale
   data. Events may also simply arrive out of order: the producers' Kafka key can't be relied on.
2. **Account and Profile belong together, and one bad record must not hold back the batch.** They are
   written in the same request, each record saved or rejected on its own; an account counts as synced
   only when both are in, and is retried until they are (see [Writing to Salesforce](#writing-to-salesforce)).
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
| Durable, replayable intake | Kafka consumer writes each event's **raw JSON**, unchanged, to that object type's Postgres **inbox table** (`sf_account_inbox`, `sf_account_relationship_inbox`) with the entity's id in a plain `entity_id` column, before storing its offset (committed in the background, so there is no broker round trip per message). The id is read from the typed event in C#; **no SQL reads inside an event**. A redelivery is simply stored again and ends `superseded`. A message that isn't a valid event of its topic's type (see below) is logged with its offset and skipped, rather than blocking its partition. |
| Ordering across parallel batches | Workers claim **entities, not events**: `SELECT … FOR UPDATE SKIP LOCKED` on each entity's *head* (oldest pending) row plus a per-entity advisory **claim lock** for the claim transaction, only for entities with nothing in flight, then lease the entity's rows (`locked_until`, `lock_token`, `retry_count`, `last_updated`). Two workers can never hold the same entity. Which event is the newest is decided by **`meta.time`**, not by arrival: the Kafka key and partition order are not used. |
| Bursts of updates to one entity | Pending events for an entity are **coalesced**: every event carries the full record, so of the events claimed together only the one with the newest `meta.time` is written (ties: the one stored later) and the others are marked `superseded`. Five updates in five seconds become one Salesforce write with the newest data. At most `Sync:MaxEventsPerEntity` (20) events of an entity are taken per claim, so a deep backlog is never leased or loaded whole. |
| Late or replayed older events | **Sync watermark in Postgres**: `sf_sync_state` holds the newest `meta.time` synced per entity, advanced in the same transaction that completes the events and only while the worker still holds the lease. A claimed event at or before the watermark is marked `superseded` without touching Salesforce. Nothing is stored in Salesforce for this. |
| Expired leases | The Salesforce call is cancelled at 80% of the lease (measured from the claim), so a slow worker gives up before another worker can claim the entity. A worker whose lease did expire can't record anything: every outcome statement and the watermark are conditional on its `lock_token`. |
| Writes whose outcome is unknown | A write that timed out, was cancelled or lost its connection may still be applied by Salesforce later. Its entities are **held** (`UNKNOWN_OUTCOME`) for `Sync:UnknownOutcomeHold` (10 minutes) before anyone may write them again, so a late write can't overwrite newer data. See [Unknown outcomes](#unknown-outcomes). |
| Records that belong together | **One Composite request of sObject Collections** creates and updates: Accounts first, then their Profiles. Each record is saved or rejected on its own (`allOrNone: false`); the entity is done, and its watermark advanced, only when all of its records are in. A new Profile points at its new Account by reference (`@{Account_create[i].id}`); existing records (e.g. a relationship's Accounts) are linked by the Id read from Salesforce. See [Writing to Salesforce](#writing-to-salesforce). |
| Create or update | Decided by the read alone: a record Salesforce has is updated with what differs, one it doesn't have is created. The event's `meta.type` is not consulted. |
| Change-only updates | **Type-aware diff against Salesforce**: the current records are read by external id with every field of their sObject class (an event is the full record; an empty field is cleared if Salesforce has a value). Values are normalized with the fields' cached Describe metadata (number scale, checkbox `null` → `false`, 15 → 18-character ids, multi-select as a set, date/time to the second and time to the millisecond, int/long within their `digits`, 18-character ids checked against their case suffix, `""` = `null`) and only fields that differ are sent, in their normalized form, so a value Salesforce stores differently never rewrites itself on every event. The comparison is with what is in Salesforce *now*, so edits made directly in Salesforce are corrected. |
| Batching and API calls | **2 calls per batch** of up to `Sync:BatchSize` entities (75, max 100): one Composite request with all reads (≤5 queries), one Composite request with all writes (≤5 collections of ≤200 records; more are split over further requests). Describe is cached and revalidated with `If-Modified-Since`. Request bodies are gzipped, responses accepted compressed. |
| Record types | Every synced object has a record type, named by developer name on its sObject class (`[SObject("Account", …, RecordType = "Business")]`). Its Id comes from the `recordTypeInfos` of the object's cached Describe, so it costs no extra call, and every record is written with that `RecordTypeId` (diffed like any other field). A record type the org doesn't offer fails the entity before any call (`INVALID_RECORD_TYPE`, retried) and makes the next batch describe again. See [Record types](#record-types). |
| Bad records | A value Salesforce would reject (unknown or read-only field, too long, wrong type, required field emptied) fails only its own unit, before any call; a malformed event is dead-lettered on its own (`INVALID_EVENT`). |
| Failures | Each record's result decides per entity: **permanent** (validation rules, invalid fields, ...; configurable) → dead-lettered immediately, alone; **transient** (`UNABLE_TO_LOCK_ROW`, a lookup target not synced yet, a create that lost a race, unknown codes) → exponential backoff, dead-lettered after `MaxRetries`; **not attempted** (skipped because of another entity's record) → back to the inbox without spending a retry. The code is stored in the inbox row's `last_error_code`. See [Retries and dead letters](#retries-and-dead-letters). |
| Postgres failures | Every unit of work (claim, outcomes, ingest, rate limiter, slots, schema) runs through EF Core's execution strategy (**`PostgresRetryStrategy`**, a `NpgsqlRetryingExecutionStrategy` configured from `Postgres:Retry`, used as `db.Database.CreateExecutionStrategy().ExecuteAsync(...)` around a transaction): a dropped connection, failover, timeout, deadlock or serialization failure re-runs the whole unit (new transaction, change tracker cleared) with exponential backoff and jitter. After a lost connection the Npgsql pool is cleared, since its idle connections are usually dead too. Every unit is safe to run twice; a claim retried after an unknown commit first releases what the lost attempt leased. A batch's outcomes are recorded with one statement per kind of outcome, not per entity. |
| Rate limits | Configured from the org's daily API allocation and Salesforce's published limits (see [Salesforce API limits](#salesforce-api-limits)): Postgres-backed **token bucket** shared by all processes; this integration's **share of the org's daily allocation** enforced as a hard cap over a rolling 24 hours; global **concurrency cap** (advisory locks) validated against the org's concurrent long-running limit; the `Sforce-Limit-Info` header (`api-usage`, and `per-app-api-usage` when sent) and own usage slow everyone down at 80% and pause at 95%; 503 (and 429 from anything in front of Salesforce) and both kinds of `REQUEST_LIMIT_EXCEEDED` pause all workers. Rate-limited batches are handed back without spending a retry. |

```
Kafka (any key, any order) ─► KafkaInboxConsumer ─► one inbox table per object type (Postgres: entity_id +
  account-changes              typed read + validate,   │  raw event JSON + processing state)
  account-relationship-changes store raw + entity_id    │  sf_account_inbox, sf_account_relationship_inbox
                                                       │  claim entities: head row + per-entity claim lock
                                                       ▼
                              InboxWorkerService ×N loops (any number of pods), round-robin over:
                                AccountSyncService ─┐            (strategy: typed event → mapper →
                                AccountRelationshipSyncService ─┤          sObject → records, + queries)
                                                                ▼
                              SalesforceBatchUpsertService.UpsertBatchAsync<TEvent, TRow>(strategy, IRepository<TRow>)
                                claim → newest meta.time per entity (supersede stale) → map
                                → describe → normalize → 1 composite read → decide (diff + lookups)
                                → 1 composite write → per-record results → outcome per entity → record outcomes
                                                       │  inbox: IRepository<TRow> (EF Core + raw SQL, PostgresRetryStrategy)
                                                       │  token bucket + daily budget + concurrency slots
                                                       ▼
                                                  Salesforce  (standard + existing fields only)
```

### How the code is layered

The sync is three layers, each with one job:

| Layer | Classes | Does |
|---|---|---|
| **Thin I/O** | `SalesforceRestClient` (the only class that talks to Salesforce: `DescribeAsync`, `QueryAsync`, `SendCollectionsAsync`); `InboxRepository<TRow>` (`IRepository<TRow>`: store, claim, record, hold, defer, fail, over `SyncDbContext`, `InboxSql` and `PostgresRetryonStrategy`); `SalesforceBatchGate` (concurrency slot + shared pause) | Sends requests and returns raw results. Decides nothing. |
| **Business flow** | `SalesforceBatchUpsertService` | `UpsertBatchAsync` is the sync as a list of steps: claim → keep the newest event per entity → map → `WriteToSalesforceAsync` (describe → normalize → read → decide → send → interpret) → record outcomes, with the three ways a batch can fail (unknown outcome → hold, rate limited → defer, anything else → retry all). |
| **Rules, no I/O** | `BatchPreparer` (coalesce, watermark, map), `PreparedBatch.Outcomes` (entity results → inbox outcomes), and in `Sync/Upsert/`: `RecordNormalizer` (values against Describe, via `FieldValues`), `ReadQueryBuilder` (the read's SOQL), `CurrentRecords` (what the read found), `RecordDiffer` (changed fields), `WritePlanner` (create / update / unchanged, lookups by Id or reference, `DEPENDENCY_NOT_SYNCED`) with `CompositeWrite.Pack` (200 records per collection, 5 collections per request), `WriteResultInterpreter` (per-record results → outcome per entity) | Pure and deterministic; unit-tested without a database or HTTP. |

**Data access** is Entity Framework Core over Npgsql (`SyncDbContext`, pooled, on the app's `NpgsqlDataSource`); the schema
stays `sql/schema.sql` (partial indexes, the table template, views, autovacuum settings: not expressible as migrations),
and `SchemaTests` checks the model against the real tables column by column. Every inbox table has the same columns, so
they all live on the abstract `InboxRow`; `AccountInboxRow` and `AccountRelationshipInboxRow` only name their tables
(two independent entity types, no shared table), and the one `InboxRepository<TRow>` (`IRepository<TRow>`, registered as
an open generic) serves both. The table name comes from the EF model. What EF can express is LINQ (`Add` + `SaveChanges`
for an event, `ExecuteUpdate` for the lease-conditional outcomes done, superseded, released, held and deferred, a query for
the watermarks); the claim, `ReleaseLostClaims`, `MarkSynced`, fail and dead-letter, and the rate limiter's statements are
the SQL texts of `InboxSql` and `RateLimitSql` run unchanged through `Database.SqlQueryRaw` / `ExecuteSqlRaw` with named
`NpgsqlParameter`s, since a recursive CTE with `FOR UPDATE SKIP LOCKED` or `UPDATE ... RETURNING` has no LINQ form.

### The Describe cache

Describe results (fields and record types) are cached by `SalesforceSchemaCache`, in two layers:

| Layer | What it holds | Shared by |
|---|---|---|
| In process | The snapshot every worker loop reads without I/O; one describe in flight per object | The pod's worker loops |
| `IDistributedCache` (Redis) | The same snapshot as JSON: fields, record types, when it was fetched and last checked | Every pod |

A pod whose own snapshot is missing or older than `Salesforce:SchemaRefreshInterval` (15 min) first looks in Redis. An
entry checked within the interval (by any pod) is used as it is, without calling Salesforce, so the org is described
about once per interval in total rather than once per pod, and a new pod starts warm. Otherwise the pod describes with
`If-Modified-Since` (the entry's date), reuses the stored schema on 304 Not Modified, and writes the result back.

- **Key**: `salesforce-sync:describe:{org host}:{API version}:{object, lower case}`, e.g.
  `salesforce-sync:describe:acme.my.salesforce.com:v62.0:account`. Different orgs and API versions never share one.
- **Expiry**: 1 day (absolute). Much longer than the refresh interval on purpose: an entry too old to trust still
  dates the conditional describe, so a 304 replaces a download.
- **Invalidation**: when Salesforce rejects a field the cached schema has (`INVALID_FIELD` on the read) or a record
  type is missing (`INVALID_RECORD_TYPE`), the object's entry is invalidated in the pod **and removed from Redis**, so
  every pod describes it again.
- **Failures**: the shared layer only saves calls. If Redis is unreachable, slower than 2 seconds, or holds an entry
  that can't be read, the pod logs a warning (at most once a minute) and describes as if the entry were missing.
  Syncing never waits for or fails because of Redis.
- **Configuration**: `ConnectionStrings:redis` (Aspire injects it; elsewhere set `ConnectionStrings__redis`, e.g.
  `redis:6379`). Without it the shared layer is an in-memory cache of the process (`AddDistributedMemoryCache`), so
  tests, benchmarks and single-process runs need no Redis.

Two pods that miss at the same moment may both describe once; that costs a call, never correctness.

**Why Redis.** Without it every pod describes every object once per interval, and a pod that starts (a deploy, a
scale-out) describes everything before its first batch. With it the pods pool their describes: one call per object
per interval for the whole deployment, and new pods start warm. It is optional, and never on the critical path: a
cache failure means a describe, not a failed batch.

### Why the sync calls Describe

Change-only writes depend on it. To decide that a value is unchanged, the sync must compare it in the form Salesforce
stores it, and only Describe says what that is:

- **Normalization**: number scale and precision, int digits, date/time to the second, checkbox `null` → `false`, text
  length, required fields.
- **Validation before any call**: an unknown or read-only field (`INVALID_FIELD`), a lookup that isn't a reference,
  a value that would be rejected. The entity fails on its own instead of costing a write.
- **The read query**: which fields to select.
- **The diff**: whether a field is createable or updateable.
- **Record types**: the developer name → `RecordTypeId` (`recordTypeInfos`).

**Is it best practice?** Caching Describe and revalidating it with `If-Modified-Since` is what Salesforce's REST API
documentation recommends, and what generic integration tools do. The cost is about one call per object per refresh
interval: with 3 objects and 15 minutes, 288 calls a day (a 304 still counts as a call). That was per pod; with the
shared Redis layer it is for the whole deployment.

| Alternative | Gains | Loses |
|---|---|---|
| Upsert everything by external id (no Describe, no read, no diff) | Two fewer steps | Every event writes every field: triggers, flows, field history and `LastModifiedDate` fire on every event, and bursts multiply writes. |
| Hard-coded field metadata | No calls | Drifts silently when an admin changes a field (scale, length, read-only). |
| Describe once at startup, refresh only on `INVALID_FIELD` | Fewer calls | Misses scale or read-only changes until the next restart. |

Recommendation: keep it. To spend fewer calls, raise `Salesforce:SchemaRefreshInterval`.

## Event format

The inbox stores only the raw Kafka message value. Every event is one entity object next to `meta`, in **camelCase**
JSON with the source system's own field names (nothing in an event is a Salesforce name):

```json
{ "meta": { "time": "2026-10-14T01:02:03.456Z", "type": "updated" },
  "account": { "externalId": "ACC-000001", "name": "Acme", "rating": "Hot", "industry": "Technology",
               "description": null, "annualRevenue": 1250000.50, "numberOfEmployees": 250, "phone": "+1 555 0100",
               "website": "https://example.com", "isActive": true, "tags": ["B2B", "Retail"],
               "customerSince": "2020-01-15", "lastContactedAt": "2026-10-12T01:02:03Z",
               "profile": { "name": "Acme profile", "tier": "Gold", "score": 87.5, "discountPercent": 12.5,
                            "notes": null, "optIn": true, "startDate": "2020-02-01" } } }
{ "meta": { "time": "2026-10-14T01:02:04.000Z", "type": "inserted" },
  "accountRelationship": { "externalId": "REL-1", "accountExternalId": "ACC-000001", "relatedAccountExternalId": "ACC-000002",
                           "name": "Acme / Globex", "type": "Parent", "startDate": "2026-01-31", "isActive": true } }
```

- **Entity type**: decided by the **topic** the event arrives on (`account-changes` → account,
  `account-relationship-changes` → accountRelationship; `Kafka:Topics:<entityType>` overrides a type's topic). Each
  type has its own typed event class, its own inbox table and its own worker pass.
- **Entity id**: `<object>.externalId`, the record's Salesforce external id (`External_Id__c`). It is read from the
  deserialized event (`UpsertStrategy.EntityId`) and stored in the inbox row's `entity_id` column.
- **Order**: `meta.time`, an ISO-8601 time with an offset or `Z`. **The Kafka message key is not used, and neither is
  arrival order**: an entity's events may come over any partition, through any consumer, in any order. The event with
  the newest `meta.time` wins; an event that is not newer than what was already synced is superseded. This relies on
  the source's clock. Two events of an entity with the same time: the one stored later wins if they are claimed
  together, otherwise the first one synced stays.
- **Type**: `meta.type`, `"inserted"` or `"updated"` (any case), is required. It is validated at ingest, where a
  missing or other value (`deleted`, ...) makes the event invalid. Deletes are out of scope. It does not decide
  whether a record is created or updated: Salesforce's current state does.
- **Casing**: camelCase is the contract. Events are bound to the typed classes with `JsonSerializerDefaults.Web`,
  which ignores case (so `"Name"` binds too), for the envelope (`meta`, `time`, `type`, `account`, `externalId`) as
  much as for the fields. A property the classes don't have is ignored.

### An event is the full record

Every event carries the entity's complete current state. There is no "field not mentioned = leave it alone":

- A property that is **`null` or absent** means *empty in the source*. If Salesforce has a value there, the field is
  cleared. (A checkbox is never null in Salesforce, so an empty one becomes `false`.)
- The diff still writes **only the fields that differ** from Salesforce, so a full record in which one field changed
  is a one-field update, and an unchanged record writes nothing. A new record is created with its non-empty fields.
- **Coalescing takes the newest event**, it doesn't merge: several pending events of an entity become one write of
  the newest full record (by `meta.time`; ties by row id). Merging would keep a field the newest event no longer has.
- A required field must therefore be in every event: an account event without a `name` is dead-lettered
  (`REQUIRED_FIELD_MISSING`), because it would empty `Account.Name`.
- The one thing that isn't cleared by absence is a whole object: an account event **without a `profile`** writes
  only the Account and leaves the Profile in Salesforce as it is.

Other rules (in the strategies, `AccountSyncService` and `AccountRelationshipSyncService`):

- **account**: `profile.externalId` defaults to the account's `externalId`, and the Profile always looks up the
  event's account.
- **accountRelationship**: `accountExternalId` and `relatedAccountExternalId` are the two Accounts (looked up by
  external id). An event without both is dead-lettered (`INVALID_EVENT`), as is an event whose value has the wrong
  type for its payload property (e.g. `"numberOfEmployees": "lots"`).

## The three typed layers

An event passes through three kinds of classes on its way to the generic engine. Each knows one thing:

| Layer | Where | Knows |
|---|---|---|
| **Inbox payload** (`AccountPayload`, `ProfilePayload`, `AccountRelationshipPayload`, in the `AccountEvent` / `AccountRelationshipEvent` envelopes) | `src/SalesforceSync/Inbox/Payloads/` | The event as the source system sends it. PascalCase CLR properties, bound from camelCase JSON; no attributes, no Salesforce names. |
| **Salesforce sObject** (`AccountSObject`, `ProfileSObject`, `AccountRelationshipSObject`) | `src/SalesforceSync/Salesforce/SObjects/` | The org. One flat class per object; `[SObject]` names the object, its external id field and its record type (developer name), `[JsonPropertyName]` names each field, `[SObjectLookup]` marks a lookup. |
| **Mapper** (`AccountMapper`, `ProfileMapper`, `AccountRelationshipMapper` : `ISObjectMapper<TPayload, TSObject>`) | `src/SalesforceSync/Sync/Mapping/` | How one becomes the other: hand-written, one line per field, converting only where the types differ (the `tags` list becomes a `;`-separated multi-select value). Registered in `AddInboxSync`. |

```
raw event JSON ─► AccountEvent { AccountPayload { …, ProfilePayload } }     System.Text.Json (Web defaults)
               ─► AccountMapper / ProfileMapper ─► AccountSObject, ProfileSObject
               ─► SObjectRecord.From(referenceId, sobject) ─► SObjectRecord ─► engine (normalize, diff, write)
```

**Salesforce API names live only on the sObject classes.** Nothing else in the sync spells a field or object name:
the strategies' queries (`SoqlQuery.RecordsOf<AccountSObject>(units)`), the lookups and the records all read them
from the attributes. Pointing the sync at an org whose fields are named differently (`LLC_BI__Active__c` instead of
`Active__c`) means editing the attribute strings and nothing else.

`SObjectRecord.From` turns any sObject instance into the engine's record: object, external id field, record type and
field names from the attributes, every property as a field (nulls included, see above), every `[SObjectLookup]`
property as a lookup by the target's external id. The attributes are read by reflection once per class
(`SObjectMetadata<TSObject>.Instance`, with compiled property getters), not per event; a class declared wrongly (a
property without `[JsonPropertyName]`, no external id property, a blank record type) fails the first time it is used.
`RecordType` is a required member of `[SObject]`, so leaving it out doesn't compile. A unit test checks
every name on every sObject class, record type included, against the mock org's schema (`FakeSchema`, whose Describe
serves each object's record types and which rejects an unknown `RecordTypeId` like Salesforce), which catches typos.

## The inbox

**Ingest.** `KafkaInboxConsumer` reads each message with the typed event of its topic's entity type
(`UpsertStrategy.TryRead`, the same read the engine uses) and stores it only if it is valid: valid JSON for that
type, the entity object with a non-empty id, a `meta.time` with an explicit offset, and a `meta.type` of `inserted`
or `updated`. Anything else is logged with its topic, partition and offset and skipped. The row gets
`entity_id` = the id from the event and `payload` = the message value, unchanged.

**One inbox table per object type** (`sql/schema.sql`), all the same shape, created at startup from one DDL template
for every registered `ISalesforceEntitySync` (`InboxSchemaInitializer`; table names are code constants, checked to be
plain identifiers before they are put into SQL):

```sql
create table if not exists sf_account_inbox (       -- and sf_account_relationship_inbox, ...
  id              bigserial   primary key,
  entity_id       text        not null,          -- the entity's id, read from the typed event by the consumer
  payload         jsonb       not null,          -- the Kafka message value, unchanged; never read by SQL
  status          text        not null default 'pending',  -- pending | done | superseded | dead
  retry_count     int         not null default 0,
  last_updated    timestamptz not null default now(),
  last_error      text,
  last_error_code text,
  next_attempt_at timestamptz not null default now(),  -- not claimable before; while leased, the end of the lease
  locked_until    timestamptz,                   -- lease while a worker processes the row
  lock_token      uuid,                          -- which worker holds the lease
  received_at     timestamptz not null default now()
);
create index if not exists sf_account_inbox_ready   on sf_account_inbox (next_attempt_at, id)     where status = 'pending';
create index if not exists sf_account_inbox_pending on sf_account_inbox (entity_id, id)           where status = 'pending';
create index if not exists sf_account_inbox_leased  on sf_account_inbox (entity_id, locked_until) where locked_until is not null;
alter table sf_account_inbox set (autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_threshold = 1000);
```

There is no entity type, version or time column and **no SQL that names a JSON field**: the event's field names are
yours to change in the payload classes. There is also no uniqueness rule: a Kafka redelivery becomes a second row,
which ends `superseded` (its time is not newer than what was synced); a rule on the payload would wrongly drop the
last event of A → B → A. `sf_sync_state (entity_type, entity_id, synced_time)` is the sync's own bookkeeping: the
newest synced `meta.time` per entity in microseconds, supplied by C#.

**Claim** (`InboxSql`, the same statements for every table). One statement picks up to `Sync:BatchSize` entities and
locks them; a second leases them:

1. Read the first 500 claimable rows (`status = 'pending'`, `next_attempt_at <= now()`) in `(next_attempt_at, id)`
   order off the ready index, and reduce them to distinct entities.
2. Per entity (not per row): find its head (lowest pending `id`), require it to be due and the entity to have no live
   lease on any row, and lock the head by primary key `FOR UPDATE SKIP LOCKED`, re-checking the conditions on the
   locked row. Then take the entity's claim lock, `pg_try_advisory_xact_lock(hashtext(table), hashtext(entity_id))`,
   held until the claim transaction ends; an entity whose claim lock another worker holds is skipped.
3. Only if those 500 rows were not enough for a batch (they can all belong to entities that are in flight or backing
   off): walk the pending entities in id order to find the ones behind them.
4. Lease the claimed entities' oldest `Sync:MaxEventsPerEntity` pending rows (`lock_token`, `locked_until`,
   `retry_count + 1`) and return them. While a row is leased its `next_attempt_at` is the end of the lease, so leased
   rows are outside the range step 1 scans and come back by themselves if the lease expires. An entity on which
   another worker holds a live lease gets no rows.

The claim lock and the lease check in step 4 are what keep two workers off one entity; the head row lock alone is
not enough. The head is whatever row a worker's snapshot shows as oldest, and an event committed out of id order
(two consumers storing events of one entity) makes it a different row for two workers, so neither row lock
conflicts. Step 4 runs while holding the claim lock, so its snapshot is taken after every earlier claim of the entity
committed, and it sees their leases (`ClaimRaceTests` replays that interleaving on four connections).

**Plans don't depend on statistics.** An inbox's statistics describe some earlier moment: never taken after a bulk
load, or taken while every row was finished. With the latter, the earlier claim chose plans that read the whole
ready index per entity: 1.5 s per statement on 100,000 pending rows, 11 s with most entities leased. The claim and
outcome transactions now start with `InboxSql.PinPlans` (`SET LOCAL` no bitmap scans, sorts, sequential scans or
JIT, sent in the same round trip as the first statement), and conditions that also fit a partial index the
statement must not use are written as a `CASE`. Every statement got one plan across 108 measured scenarios. Claim
and outcome costs are in [docs/benchmarks/README.md](docs/benchmarks/README.md).

Then, per claimed entity, in C#: deserialize its rows, keep the one with the newest `meta.time` (the rest are
`superseded`), supersede that one too if it is not newer than the entity's watermark, otherwise map → write → advance
the watermark. A row that can't be read is dead-lettered on its own (`INVALID_EVENT`). All outcome statements are
set-based and conditional on the `lock_token`.

**Cost.** A claim of 75 entities locks them in about 2 ms (median over 108 measured scenarios: 5,000 or 100,000
pending rows, 100 to 5,000 entities, fresh, analyzed or stale statistics, up to 90% of entities in flight); the
slowest scenario's median is 9.5 ms and the slowest single run 21.5 ms, on a loaded machine. Leasing costs about
11 µs per row on a quiet machine and 30 to 55 µs under load (each lease is an update that adds an entry to all four
indexes), so a claim that leases 75 events is a few milliseconds and one that leases 1,500 (75 entities with 20 or
more pending events each) is 20 to 40 ms, about 54 ms under load. Lower `Sync:MaxEventsPerEntity` to make deep-backlog claims cheaper, at the price of more
Salesforce calls per event.

**Upgrade.** An `sf_inbox` table from an earlier version (any shape) is renamed to `sf_inbox_legacy` at startup, rows
kept for inspection and not migrated; the `sf_event_*` SQL functions are dropped; `sf_sync_state` keeps its
watermarks under the new column names.

```sql
-- Dead letters of one type, by cause
select entity_id, payload -> 'meta' ->> 'time' as time, last_error_code, last_error
from sf_account_inbox where status = 'dead' order by id desc;

-- Everything an inbox holds for one entity, in arrival order
select id, status, retry_count, last_error_code, locked_until, payload
from sf_account_inbox where entity_id = 'ACC-000001' order by id;
```

(Queries like these, for people, may look inside the payload; the worker's own SQL never does.)

### How a lease works

A lease is two columns on the inbox rows: `lock_token` (which claim owns the row) and `locked_until` (until when).
It is what keeps an entity to one worker while that worker talks to Salesforce, without holding a database lock
or a transaction all that time.

1. **Take.** In the claim transaction, `LockHeads` locks each entity's head row (`FOR UPDATE SKIP LOCKED`) and takes
   the entity's claim lock (`pg_try_advisory_xact_lock(hashtext(table), hashtext(entity_id))`). `ClaimEntities` then
   checks that no other worker has a live lease on the entity and leases its oldest pending rows: a new `lock_token`,
   `locked_until = now() + Sync:Lease`, and `next_attempt_at` = the end of the lease, so leased rows drop out of the
   claim's scan. Because the claim lock serializes claims of one entity, that check sees every earlier claim's
   lease, even when two workers see different head rows (`ClaimRaceTests`). The transaction commits; the locks go,
   the lease stays.
2. **While live.** Other workers skip the entity. This worker calls Salesforce outside any transaction and stops
   (cancels the call) at 80% of the lease, measured from the claim, so it gives up before anyone else can claim the
   entity.
3. **Release.** The outcome transaction records done, superseded, retry, dead or hold. Every statement, the
   watermark (`MarkSynced`) included, only touches rows `where lock_token = @token`: a worker whose lease expired and
   was claimed by another changes nothing.
4. **Crash.** Nothing to clean up. The lease expires, `next_attempt_at` has already passed, and the next claim takes
   the rows. Its read of Salesforce shows what the crashed worker did or didn't save. (A write whose outcome is
   unknown is held instead; see [Unknown outcomes](#unknown-outcomes).)

### Why the Salesforce calls run outside a transaction

A batch uses two short transactions: the claim (`InboxRepository.ClaimAsync`) and the outcomes
(`RecordAsync`), each through `RunInTransactionAsync` (the execution strategy around `BeginTransactionAsync`). The
Salesforce read and write happen between them, in no transaction. One transaction around everything would be worse:

| One transaction around the batch would | Because |
|---|---|
| only look atomic | Salesforce can't take part in a Postgres transaction. A rollback can't undo a write Salesforce saved. |
| send the HTTP calls again | The execution strategy re-runs the whole delegate on a transient Postgres error (dropped connection, failover). |
| hold row locks for seconds, up to the request timeout (90 s) | Other workers and the consumer wait on them. |
| pin a pooled connection per worker | Workers × pods connections busy doing nothing. |
| block autovacuum | An open transaction holds back the horizon, and the inbox tables churn: bloat builds up. |

The lease protects the rows between the two transactions. The trade-off: a crash after Salesforce saved but before
the outcomes committed leaves the rows pending. That is safe, because every attempt reads Salesforce first and writes
only what differs, so the retry finds the records saved and ends `done` without writing them again. A write whose
outcome is unknown is held rather than retried at once.

### Retries and dead letters

A transient failure (`UNABLE_TO_LOCK_ROW`, `DEPENDENCY_NOT_SYNCED`, `INVALID_RECORD_TYPE`, a create answered with `DUPLICATE_VALUE` because
someone else created the record first, an unknown error code, a failed read) is retried
with exponential backoff: `Sync:RetryBaseDelay` (30 s) doubling per attempt up to `Sync:MaxRetryDelay` (1 h), and
dead-lettered after `Sync:MaxRetries` (8) attempts. With the defaults the attempts come about 30 s, 1 min, 2 min,
4 min, 8 min, 16 min and 32 min apart: roughly an hour from the first failure to the dead letter. Permanent errors
are dead-lettered at once. The error code that caused it stays in `last_error_code`. Rate limits, records that were
not attempted and unknown outcomes don't spend a retry.

**A relationship that arrives before its Accounts** waits this way: the read doesn't find the Account, the event
fails with `DEPENDENCY_NOT_SYNCED` before anything is sent, and it is retried on the schedule above until the
account sync has created the Account; no manual step is needed. If Salesforce itself rejects the lookup at write time
(`INVALID_CROSS_REFERENCE_KEY` or `ENTITY_IS_DELETED` on the lookup field), that is recorded as the same `DEPENDENCY_NOT_SYNCED` and retried the same way, not dead-lettered. Only an
Account that never arrives within `MaxRetries` ends as a dead letter, with `DEPENDENCY_NOT_SYNCED` kept as the cause.

To re-queue dead letters once the cause is fixed (e.g. the missing Account now exists):

```sql
update sf_account_inbox
set status = 'pending', retry_count = 0, next_attempt_at = now(), locked_until = null, lock_token = null
where status = 'dead' and last_error_code = 'DEPENDENCY_NOT_SYNCED';

update sf_account_relationship_inbox
set status = 'pending', retry_count = 0, next_attempt_at = now(), locked_until = null, lock_token = null
where status = 'dead' and last_error_code = 'DEPENDENCY_NOT_SYNCED';
```

A re-queued event still obeys the watermark: if a newer event of the entity has been synced since, it is superseded.

### Unknown outcomes

A write to Salesforce whose result never arrives may still be applied, possibly after the worker has given up. If
the entity were released and a newer event synced in the meantime, the late write would overwrite newer data.

- **What counts as unknown** (`WriteOutcome.IsUnknown`): a cancellation or timeout (the lease cutoff, the HTTP
  timeout, shutdown while the request is in flight), a connection lost once the request may have left, a 502/504, a
  503 that is not Salesforce's own, or a response that can't be read.
- **What does not**: anything Salesforce itself answered (4xx including 429, `REQUEST_LIMIT_EXCEEDED`, per-record
  errors, a 5xx with a Salesforce error body), a connection that was never established (refused, DNS, TLS), and
  failures before the request left (waiting for a rate-limit token, getting the OAuth token). Failures of the
  describe or the read are never unknown: nothing was written. These follow the normal rules at once.
- **What happens**: every entity of that batch is held. Its claimed rows stay `pending` with `locked_until = now() +
  Sync:UnknownOutcomeHold`, no `lock_token` (the old worker can't record anything on them), `last_error_code =
  'UNKNOWN_OUTCOME'`, and the attempt doesn't count as a retry. The watermark doesn't move. The claim skips an entity
  with a live lease, so events arriving for it meanwhile wait too. After the hold, normal processing (read, diff,
  write the newest event) converges whether or not the lost write landed.
- **The cost**: that entity's updates wait up to the hold.
- **Why 10 minutes**: it is Salesforce's API request timeout, after which the lost request has completed or been
  stopped. Lower `Sync:UnknownOutcomeHold` only if your org's requests are certain to end sooner.
- **Shutdown**: a worker stopping mid-write still records the hold (with its own short timeout). If it can't (the
  process is killed), the lease is what protects the entity, which is why startup requires `Sync:Lease` to be longer
  than `SalesforceLimits:RequestTimeout`. The remaining risk is a hard kill combined with a lease shorter than the
  time Salesforce takes to finish the request.

### Parallel processing: what is guaranteed

| Guarantee | How | Test |
|---|---|---|
| Two workers or pods never hold the same entity at once | head row `FOR UPDATE SKIP LOCKED`, per-entity claim lock, lease on the entity's rows checked under that lock | `ConcurrencyTests.Parallel_pods_never_process_the_same_account_concurrently_and_end_at_the_newest_version` (3 pods × 8 loops, claim audit: 0 overlaps); `ClaimRaceTests` (a row committed out of id order; fails without the claim lock and lease check) |
| An event arriving while its entity is being processed is not claimed until the lease is released or expires, then wins if newer | the claim requires the head to be due and the entity unleased | `LeaseTests.Event_arriving_while_its_entity_is_being_processed_waits_for_the_lease_then_wins_if_newer` |
| Events out of `meta.time` order, over any partition and consumer, end at the newest; an older record is never written after a newer one | newest-time coalescing + watermark | `ConcurrencyTests.Events_out_of_time_order_over_any_partition_and_pod_end_at_the_newest_time`, `SyncEndToEndTests.The_Kafka_key_is_not_used_and_arrival_order_does_not_matter` |
| A worker whose lease expired can't record outcomes or move the watermark | every statement is conditional on `lock_token` | `LeaseTests.Worker_whose_lease_expired_can_record_nothing_and_cannot_move_the_watermark` |
| Dropped Postgres connections cause no overlap, loss or regression | `PostgresRetryStrategy` (EF execution strategy), idempotent statements | `ConcurrencyTests.Pods_survive_their_Postgres_connections_being_dropped` |
| A write with an unknown outcome can't be overtaken by a newer write | the hold | `UnknownOutcomeTests.Entity_is_held_after_a_write_with_an_unknown_outcome_and_ends_at_the_newest_event` |

Windows in which a guarantee does not hold, stated plainly: (1) a worker whose **Postgres** work to record a hold
fails (database unreachable) or whose process is killed mid-write leaves only the lease; if Salesforce then takes
longer than the rest of the lease to apply the request, a newer write can be overtaken. (2) A lease that expires
while its worker is still running (clock stall, a pause longer than the 20% margin) lets a second worker claim the
entity; the first can no longer record anything, but a request it already sent is in Salesforce's hands. (3)
Ordering is only as good as `meta.time`: two different events of an entity with the same time are not ordered by
content, and a source clock that goes backwards makes a newer change look older (it is superseded).

## Writing to Salesforce

A batch is written with **one Composite request** (`POST /services/data/vXX.X/composite`) whose subrequests are
sObject Collections operations with explicit verbs, never upserts:

```jsonc
{ "allOrNone": false,
  "compositeRequest": [
    { "method": "POST",  "url": "/services/data/v62.0/composite/sobjects", "referenceId": "Account_create",
      "body": { "allOrNone": false, "records": [
        { "attributes": { "type": "Account" }, "External_Id__c": "ACC-1", "Name": "Acme" } ] } },
    { "method": "PATCH", "url": "/services/data/v62.0/composite/sobjects", "referenceId": "Account_update",
      "body": { "allOrNone": false, "records": [
        { "attributes": { "type": "Account" }, "id": "001…", "Rating": "Hot" } ] } },       // only what differs
    { "method": "POST",  "url": "/services/data/v62.0/composite/sobjects", "referenceId": "Profile__c_create_ref",
      "body": { "allOrNone": false, "records": [
        { "attributes": { "type": "Profile__c" }, "External_Id__c": "ACC-1", "Account__c": "@{Account_create[0].id}" } ] } }
  ] }
```

One collection per object and verb, parents before children: parent creates, parent updates, then child creates and
updates. Salesforce's limits are constants in `SalesforceRestClient`: 200 records per collection, 25 subrequests per
Composite request of which **5** may be collections or queries. An account batch normally needs at most 5
collections (Account create / update, Profile create by reference / create with an Id / update). Units that need
more are split over further Composite requests, an entity's records always in the same one. The read is unchanged: one
Composite request with up to 5 queries.

When a batch needs more than one write request and one of them fails as a whole (the call, not records in it), the
batch stops and the failure applies to every event of the batch, as if the first request had failed: retried with
backoff, held if the request's outcome is unknown, deferred if it was rate limited. That is safe even though earlier
requests may have saved their records: the retry reads Salesforce again and writes only what still differs, so those
entities end `done` without being written twice.

**`Sync:BatchSize`** is 1 to 100 entities (default 75). The bound comes from the read, not the write: a query takes
100 external ids and a Composite request 5 queries, and a relationship needs up to three ids per entity (itself and
two Accounts).

### Record types

Every synced object must have a record type, and each sObject class names it by **developer name**:

```csharp
[SObject("Account", ExternalIdField = "External_Id__c", RecordType = "Business")]
[SObject("Profile__c", ExternalIdField = "External_Id__c", RecordType = "Standard")]
[SObject("Account_Relationship__c", ExternalIdField = "External_Id__c", RecordType = "Standard")]
```

These are defaults; they must match record types in the org (see
[step 4](#4-create-the-objects-and-fields)). The engine turns the name into an Id with the RecordType data Salesforce
returns in the object's Describe (`recordTypeInfos`: `developerName`, `recordTypeId`, `active`, `available` for the
running user). It is the same Describe the engine already calls for the fields, so the record type Ids are cached with
it (`SalesforceSchemaCache`: same refresh interval, `If-Modified-Since`, one fetch in flight per object): no extra call
and no second cache.

`RecordTypeId` is then an ordinary field of the record's desired state: set on create, read back by the batch's read,
compared after normalization (15 → 18-character id), and written on update only when Salesforce has another one
(for example after a user changed it). A lookup's target needs no record type: it is found by its external id only.

| Problem | Result |
|---|---|
| The developer name isn't in the object's Describe, isn't active, or isn't available to the run-as user | The entity fails before any call with `INVALID_RECORD_TYPE` (the message names the object and developer name). The object's cached Describe is invalidated, so the next batch describes it again. |
| Salesforce rejects the `RecordTypeId` at write time (`INVALID_CROSS_REFERENCE_KEY` on `RecordTypeId`: deactivated or unassigned after the describe) | Recorded as the same `INVALID_RECORD_TYPE`, Describe invalidated. |

**Why from Describe, not a query on `RecordType`.** The record type Ids come from the Describe the engine already
makes, so there is no extra call and no second cache: they are refreshed together with the fields, and Describe
already says whether a record type is active and available to the run-as user, which a SOQL query on `RecordType`
would not.

`INVALID_RECORD_TYPE` is transient (it is not in `Salesforce:PermanentErrorCodes`): it is retried with backoff, so
events go through once an admin has created, activated or assigned the record type, and dead-lettered only after
`Sync:MaxRetries`.

### Create or update: decided by what Salesforce has

Which verb a record gets is decided by the read alone; the event's `meta.type` is not consulted:

| In Salesforce | What happens |
|---|---|
| no | **create** from the full record |
| yes | **update** with the fields that differ; nothing differs → no write (`done`) |

A redelivered insert therefore writes nothing and creates no duplicate, and an `updated` event for a record that
isn't in Salesforce (its insert was lost or dead-lettered, or the record was deleted there) creates it. An
`inserted` and an `updated` event pending together are coalesced first, so they become one write of the newest
data. A create that Salesforce answers with `DUPLICATE_VALUE` (someone created the record between the read and the
write) is retried, and the retry's read turns it into an update.

### `allOrNone: false`

`allOrNone: false` is sent explicitly on the Composite request and on every collection in it: every record is saved
or rejected on its own. A record with a validation error fails alone; the other 99 entities of the batch are saved
in the same call. If records fail but none of them names a cause (each reports `PROCESSING_HALTED`, or has no
answer), the entities spend a retry and back off, so a request can't be repeated forever.

**Not atomic.** An Account can be saved while its Profile is rejected (or the reverse). The entity is then not
done: it is retried (or dead-lettered, by the failing record's error), the retry's read finds the record that is
already in and writes only what is missing, and **the watermark advances only when every record of the entity has
succeeded**. Until then Salesforce can show the new Account with the old or no Profile.

### Per-record results → what happens to the entity

| Records of the entity | Entity |
|---|---|
| all saved, or nothing to change | `done`, watermark advanced |
| any record with a permanent error (`Salesforce:PermanentErrorCodes`) | dead-lettered at once, alone |
| any record with another error (row lock, `DUPLICATE_VALUE` on create, lookup target missing, unknown code) | retried with backoff |
| a child not saved only because its parent in the same entity wasn't | whatever the parent's error says; the child's own "parent missing" error is ignored |
| its whole subrequest went down because of another entity's record (`PROCESSING_HALTED` on a reference without a value) | released, no retry spent |
| a subrequest that failed as a whole for any other reason | retried with backoff, never dead-lettered (it says nothing about one record's data) |
| the call itself failed (any write request of the batch) | every event of the batch: rate-limited → deferred; provably not applied → retried; otherwise [unknown outcome](#unknown-outcomes) → held |

### Linking a child to a parent created in the same request

A new Profile points at an Account that is created in the same request by a Composite reference to the parent's
create result, `"Account__c": "@{Account_create[3].id}"`: the parent's position in its collection's response.
Children that link this way get a collection of their own (`Profile__c_create_ref`). If a parent fails, its
reference has no value, which fails the **whole subrequest** of children that use references (`PROCESSING_HALTED`).
The other entities' parents are in, so their children go back to the inbox without spending a retry and are written
by the next claim, with a literal Id. Children of existing parents are in a separate subrequest and are not affected.

A child whose parent **already exists** gets the parent's 18-character Id from the read, and a parent that exists
and has nothing to change is not written at all. A parent that is neither in Salesforce nor in the same entity
fails the entity with `DEPENDENCY_NOT_SYNCED` before anything is sent.

A **relationship** is its own event, inbox and request, separate from the account sync, so its two Accounts are
always literal Ids from the read.

> **Verify in a sandbox.** Salesforce documents `@{referenceId.field}` references between Composite subrequests,
> with `[]` indexing into arrays, and that sObject Collections can be Composite subrequests. That a reference may
> index into a collection's *response array* (`@{Account_create[0].id}`) is not spelled out, and neither is what
> Composite answers for a reference without a value. The mock org implements both the way this client expects. If a
> real org rejects such a subrequest, nothing is lost: a subrequest-level failure is retried, never dead-lettered,
> and the retry links the child by the Id it now reads; but every new account would then take two rounds.

## Adding a Salesforce object

The engine (`SalesforceBatchUpsertService.UpsertBatchAsync<TEvent, TRow>`) owns claiming, ordering, coalescing, rate
limits, batching, normalization, diffing, lookups, retries and dead-lettering. Nothing in the engine, the consumer or
the worker loops names an object. An object supplies four small things: a payload class, an sObject class, a mapper,
an inbox row class (which selects the object's inbox table) and a strategy class that names the object's topic and says
how an event becomes records. The strategy class derives from `EntitySyncService<TEvent, TRow>`, which does the rest of
`ISalesforceEntitySync` through the shared `IRepository<TRow>`.

```csharp
// 1. Payload: the event as the source sends it  { "meta": {...}, "accountRelationship": { "externalId": ..., ... } }
public sealed record AccountRelationshipEvent(EventMeta? Meta, AccountRelationshipPayload? AccountRelationship);

public sealed class AccountRelationshipPayload
{
    public string? ExternalId { get; set; }
    public string? AccountExternalId { get; set; }
    public string? RelatedAccountExternalId { get; set; }
    public string? Type { get; set; }
    public bool? IsActive { get; set; }
}

// 2. sObject: the only place with Salesforce API names. RecordType is the developer name of the record type every
//    record gets (required; it must exist in the org, be active and be assigned to the run-as user)
[SObject("Account_Relationship__c", ExternalIdField = "External_Id__c", RecordType = "Standard")]
public sealed class AccountRelationshipSObject
{
    [JsonPropertyName("External_Id__c")] public string? ExternalId { get; set; }

    // A lookup holds the external id of the record it points at; the target class knows its object and id field
    [JsonPropertyName("Account__c"), SObjectLookup(typeof(AccountSObject))]
    public string? AccountExternalId { get; set; }

    [JsonPropertyName("Related_Account__c"), SObjectLookup(typeof(AccountSObject))]
    public string? RelatedAccountExternalId { get; set; }

    [JsonPropertyName("Type__c")] public string? Type { get; set; }
    [JsonPropertyName("Active__c")] public bool? Active { get; set; }
}

// 3. Mapper: explicit, one line per field
public sealed class AccountRelationshipMapper : ISObjectMapper<AccountRelationshipPayload, AccountRelationshipSObject>
{
    public AccountRelationshipSObject Map(AccountRelationshipPayload payload) => new()
    {
        ExternalId = payload.ExternalId,
        AccountExternalId = payload.AccountExternalId,
        RelatedAccountExternalId = payload.RelatedAccountExternalId,
        Type = payload.Type,
        Active = payload.IsActive
    };
}

// 4. Inbox row: the object's inbox table. Every column is on InboxRow, so every object's inbox has the same fields
public sealed class AccountRelationshipInboxRow : InboxRow;
//    ... and in SyncDbContext: a DbSet<AccountRelationshipInboxRow>, and in OnModelCreating
//    ConfigureInbox(modelBuilder.Entity<AccountRelationshipInboxRow>(), "sf_account_relationship_inbox");
//    The table is created at startup from the schema template (sql/schema.sql); SchemaTests checks the model against it.

// 5. Strategy: how to read an event (id, time, type), event -> records, and what to read
public sealed class AccountRelationshipSyncService(
    SalesforceBatchUpsertService engine,
    IRepository<AccountRelationshipInboxRow> inbox,   // the open generic InboxRepository<TRow>, registered once
    ISObjectMapper<AccountRelationshipPayload, AccountRelationshipSObject> relationships)
    : EntitySyncService<AccountRelationshipEvent, AccountRelationshipInboxRow>(engine, inbox, CreateStrategy(relationships), DefaultTopic)
{
    public const string Type = "accountRelationship";
    public const string DefaultTopic = "account-relationship-changes";  // Kafka:Topics:accountRelationship overrides

    public static UpsertStrategy<AccountRelationshipEvent> CreateStrategy(
        ISObjectMapper<AccountRelationshipPayload, AccountRelationshipSObject> relationships)
        => new(Type,
            EntityId: e => e.AccountRelationship?.ExternalId,   // the only places that know where the id
            Meta: e => e.Meta,                                  // and the meta (time and type) are in the event
            Map: (key, e) => Map(key, e, relationships),
            Queries: Queries);

    // Throwing dead-letters the event (INVALID_EVENT)
    public static UpsertUnit Map(string key, AccountRelationshipEvent e,
        ISObjectMapper<AccountRelationshipPayload, AccountRelationshipSObject> relationships)
    {
        var payload = e.AccountRelationship;
        if (string.IsNullOrWhiteSpace(payload?.AccountExternalId) || string.IsNullOrWhiteSpace(payload.RelatedAccountExternalId))
            throw new ArgumentException("accountExternalId and relatedAccountExternalId are required");

        return new UpsertUnit([SObjectRecord.From("rel", relationships.Map(payload))]);
    }

    // The relationships themselves (to diff) and the Accounts they point at (for their Ids)
    public static IEnumerable<SoqlQuery> Queries(IReadOnlyCollection<UpsertUnit> units) =>
    [
        SoqlQuery.RecordsOf<AccountRelationshipSObject>(units),
        SoqlQuery.LookupTargetsOf<AccountSObject>(units)
    ];
}
```

Register the mapper and the strategy in `AddInboxSync`:

```csharp
services.AddSingleton<ISObjectMapper<AccountRelationshipPayload, AccountRelationshipSObject>, AccountRelationshipMapper>();
services.AddTransient<ISalesforceEntitySync, AccountRelationshipSyncService>();
```

From then on the object's inbox table is created at startup, the consumer subscribes to its topic and stores its
events there, and the worker loops sync it. A lookup whose target isn't in Salesforce yet (`DEPENDENCY_NOT_SYNCED`) is retried with backoff, so
related objects can arrive in any order. A strategy may build `SObjectRecord`s by hand; the typed layers are how the built-in objects do it, not something the
engine requires.

**Why `SemaphoreSlim` isn't used for parallel batches:** it limits concurrency within one process, while
Salesforce's limits are org-wide. Parallelism comes from competing worker loops in every pod pulling from the
shared inbox, capped globally by Postgres advisory-lock slots and a shared token bucket. (The Describe cache
does use a `SemaphoreSlim`, as an in-process single-flight so concurrent workers don't all describe at once.)

## Repository layout

```
src/
  SalesforceSync/                 Worker:
    Inbox/                        Kafka consumer, worker loops, inbox schema, InboxSql, InboxRow and its subclasses, InboxRepository
    Inbox/Payloads/               The typed events (payload classes) and their meta
    Sync/                         The engine (SalesforceBatchUpsertService), BatchPreparer, the strategies
                                  (EntitySyncService and one class per object) and the engine's model types
    Sync/Upsert/                  The write path's rules, no I/O: normalize, read queries, diff, plan, pack, interpret
    Sync/Mapping/                 Mappers: payload -> sObject
    Salesforce/                   The REST client and its request/result types, Describe cache, FieldValues, errors
    Salesforce/SObjects/          sObject classes and the attributes they are read by
    RateLimiting/                 Token bucket, daily budget, pause, concurrency slots, the batch gate
    Data/                         SyncDbContext (the EF model of the tables), PostgresRetryStrategy, raw-SQL helpers
  SalesforceSync.FakeSalesforce/  In-memory Salesforce (describe, query, composite, sObject collections, Salesforce-like
                                  value storage, error injection, write history, Sforce-Limit-Info): FakeSalesforceStore
                                  is the org's surface and lock; FakeRecords, FakeSoql, FakeCollectionWriter,
                                  CompositeReferences, FakeFaults and FakeCounters are its parts
  SalesforceSync.FakeSalesforce.Server/  The in-memory org as an HTTP service (+ /__fake inspection API)
  SalesforceSync.LoadGenerator/   Produces account change (with bursts) and relationship events to Kafka
  SalesforceSync.AppHost/         Aspire: Postgres + pgAdmin, Kafka + Kafka UI, mock Salesforce, worker, generator, benchmarks
tests/
  SalesforceSync.Tests/           Unit tests: the Salesforce half of the engine with both strategies against the
                                  in-memory org, the engine's flow with a stand-in inbox, the pure write-path rules,
                                  type normalization, error classification, execution strategy, limits
  SalesforceSync.IntegrationTests/  End-to-end tests: Postgres + Kafka in Testcontainers, worker pods and the mock org
                                  in-process; Kafka in, assertions on the mock org + inbox
benchmarks/
  SalesforceSync.Benchmarks/      BenchmarkDotNet: real Postgres inbox, fake Salesforce HTTP
sql/
  schema.sql                      Inbox table template (applied once per object type) + indexes, sync watermark,
                                  rate-limit and API usage tables (embedded, applied at worker startup)
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
- **redis** (+ Redis Insight): the Describe cache shared by the sync's pods ([The Describe cache](#the-describe-cache))
- **fake-salesforce**: mock Salesforce org over HTTP (OAuth token, describe, query, composite with sObject
  collections, gzip). Logs its counters every 15s; inspect it at `/__fake/stats`, `/__fake/transport`,
  `/__fake/records/{sobject}[/{externalId}]`, `/__fake/history/{sobject}/{externalId}` and `/__fake/requests` (the
  write requests as received); inject errors with
  `POST /__fake/faults`
- **salesforce-sync**: consumes `account-changes` and `account-relationship-changes` into their inbox tables and syncs
  to `fake-salesforce` through the real OAuth + REST client
- **load-generator**: 20k account events (full records) over 500 accounts at 200/s, including bursts to the same
  account, plus relationship events between accounts
- **benchmarks**: explicit start (click ▶), runs the inbox drain benchmark against `benchdb`

To sync to a Salesforce sandbox instead of the mock org, see [Run against the sandbox](#5-run-against-the-sandbox).

## Tests

Requires the .NET 8 SDK; the integration tests also need **Docker** (Docker Desktop, OrbStack or Colima), nothing
else: no Aspire, no local Postgres, Kafka or Redis, no ports to free up. Images are pulled on first run (`postgres:17`,
`apache/kafka:3.9.1`, `redis:7.4-alpine`).

```bash
dotnet test                                      # everything
dotnet test tests/SalesforceSync.Tests           # unit tests only (no Docker, under a second)
dotnet test tests/SalesforceSync.IntegrationTests
```

Libraries: **xUnit** (2.x), **FluentAssertions** (pinned to 7.x, `[7.2.2]`: version 8 needs a commercial license),
**AutoFixture** (+ `AutoFixture.Xunit2` for `[AutoData]`) for values that only need to exist or be unique (keys,
names, numbers that are written and read back; documented examples such as the 15 → 18-character id stay
literal), and **Testcontainers** (`Testcontainers.PostgreSql`, `Testcontainers.Kafka`) for the integration tests.

### Integration tests

Each xUnit collection fixture (`SyncFixture`, `MultiPodFixture`, `FewRetriesFixture`, `DailyBudgetFixture`,
`ConcurrentLimitFixture`)
starts its own environment once, shared by the tests in that collection:

- **Postgres** (`postgres:17`) and **Kafka** (KRaft) in containers, removed when the collection finishes. Both
  topics are created up front with 6 partitions, like production. `MultiPodFixture` also starts **Redis**
  (`redis:7.4-alpine`), so its pods share their Describe cache like replicas in production; the other fixtures'
  pods use the in-memory fallback.
- **The mock Salesforce org**: the `SalesforceSync.FakeSalesforce.Server` app itself (`FakeSalesforceServer.Build`,
  the same code its `Program.cs` runs) on Kestrel at a random loopback port, so the worker goes through real HTTP:
  OAuth token, gzip, the REST client.
- **Worker pods**: each pod is its own in-process `IHost` built with `AddSalesforceSyncWorker()`, the exact
  registrations `Program.cs` uses (options and validation, Npgsql, the Salesforce client with the rate limiter,
  auth handler and token provider, `KafkaInboxConsumer`, `InboxSchemaInitializer`, `RateLimitInitializer`,
  `InboxWorkerService`). A pod has its own connection pool, Kafka consumer (all pods share the consumer group),
  token cache, rate limiter (sharing the Postgres bucket, budget and slots), HTTP stack and worker loops, so
  several pods behave like replicas. Pods read the worker's `appsettings.json` plus the fixture's settings, never
  the environment. Each pod sets `Salesforce:UserAgentInstance` so the mock org can tell them apart.

A fixture shapes the deployment in its constructor: the pod count, `WorkerSettings` that every pod gets (e.g.
`Sync:Workers`, `Sync:BatchSize`, `Sync:Lease`, `Sync:RetryBaseDelay`, `RateLimit:*`) and `FakeOrgSettings` for the
org (`FakeSalesforce:Latency`, `LongRunningThreshold`, `ConcurrentLongRunningLimit`, ...). Waits for a result poll
with `Eventually`; the only fixed delay checks that nothing more happens once the daily budget is used up. Tests
publish to Kafka and assert on the records in the mock org and the inbox rows' status. `LeaseTests` play a worker by hand
with the worker's own SQL, so a lease lasts exactly as long as the test wants.

Covered: create Account + linked Profile, a full record in which one field changed (only that field is sent), a
field that becomes null or absent being cleared, an event without a profile leaving the Profile alone, the diff
being taken against the record in Salesforce (a direct edit in Salesforce is corrected by the next event),
bursts coalesced to the newest full record, stale versions superseded without a write, unchanged events skip the
write, exactly 2 API calls per batch, a redelivered event stored again and superseded, an older event claimed with a
newer one superseded in the same claim, A → B → A ending at A, one account's events under different or no Kafka keys
ending at the newest `meta.time`, invalid events (no time offset, no or an unknown `meta.type`, another type's event,
not JSON) skipped at ingest, a deep backlog taken 20 events at a time, an event waiting for its entity's lease, an
expired worker unable to record anything, the unknown-outcome hold, both inbox tables created at startup with
identical columns, the schema being reapplied, and an earlier `sf_inbox` kept aside as `sf_inbox_legacy`. Tests
publish version N as `meta.time` = a fixed base time + N ms, and read it back from the stored payload. Relationships
(`RelationshipTests`): linked to two synced Accounts with only changed fields sent later; a relationship that
arrives before its Accounts waits (`DEPENDENCY_NOT_SYNCED`, retried) and syncs once they exist; a malformed
relationship event is dead-lettered alone. Every record ends with its class's `RecordTypeId`. Pods share their
Describes through Redis, so a pod that has described nothing finds them there (`SharedDescribeCacheTests`). Writes are checked with the mock's per-record write history
(`/__fake/history/{sobject}/{externalId}`) and with the requests it received (`/__fake/requests`).

The write itself (`CollectionsWriteTests`): a new Account and Profile in one request with `allOrNone: false` and the
Profile's reference to its Account; an Account saved while its Profile failed being retried with the watermark
unmoved until both are in; a failing Account dead-lettered alone, without its Profile, while another account is
saved; `inserted` and `updated` pending together becoming one create; a later `inserted` for an existing record
updating only what differs. `DeadLetterTests` (`FewRetriesFixture`, `Sync:MaxRetries=3` from configuration): a
relationship whose Account never arrives dead-lettered after `MaxRetries` with `DEPENDENCY_NOT_SYNCED`, then synced
once the Accounts exist and the dead letter is re-queued.

Rate limits (`RateLimitTests`, each with its own fixture, i.e. its own containers, org and pods):

- **Daily budget across pods**: 3 pods share a budget of 30 calls (`DailyApiAllocation=1000`, the mock org
  reporting a limit of 60, `DailyBudgetShare=0.5`). Exactly 30 calls reach the org, then nothing more; the
  remaining events are deferred until the budget frees up, without spending retries or being dead-lettered.
- **Concurrent long-running limit**: the mock org allows 2 concurrent long-running calls while 12 workers send
  them. The `ConcurrentPerOrgLongTxn` rejections cause a short pause (not the long daily one) and every event
  still syncs, with no retries spent.

### Unit tests

Unit tests (`dotnet test tests/SalesforceSync.Tests`, no Docker) run the Salesforce half of the engine with both
strategies against the in-memory org: 2 calls per batch, the exact verbs, bodies and `allOrNone` flags sent, values
Salesforce stores differently written once rather than on every event, a bad value failing only its own unit before
any call, a validation error or a row lock costing only its own entity, an Account saved without its Profile, a
create losing a race, a failed new Account taking down the reference subrequest without dead-lettering anyone else,
more than 5 collections split over requests, failures without a cause spending a retry, create or update decided by
what Salesforce has, relinking, relationships and missing dependencies (found by the read, or rejected by
Salesforce), gzip, clearing fields that became empty. Also: each mapper (an AutoFixture payload in, the expected
sObject out, nulls passed through), sObject → record (names from the attributes, nulls included, lookups and external
id), the guard that every Salesforce name on an sObject class exists in the mock org's schema, the event mapping
(account with and without a profile, relationships, missing Account ids, camelCase and PascalCase bodies), what the
engine does with a claim (newest `meta.time` wins, ties, watermark, A → B → A, invalid rows alone), what the consumer
admits (the typed read: id, time with offset, `meta.type`), the classification of failed writes (unknown outcome or
not), inbox table names as plain identifiers, every normalization rule (including the documented 15 → 18-character id
example), error classification, the Postgres execution strategy (retried, not retried, gives up), the daily budget
(the configured allocation as the maximum, lowered by the one Salesforce reports), startup validation and throttle
decisions.

### Concurrency: many threads, many pods, same accounts

`ConcurrencyTests` (`MultiPodFixture`) scales the worker like a Kubernetes deployment: **3 pods x 8 worker loops**, all on one
inbox and one org, while 60 accounts each receive 25 updates in waves (new events keep arriving while those
accounts are being processed). Salesforce has nothing that rejects an out-of-order write, so a race would
land in the org and show up in the mock's write history. The rate limiter is opened up and batches are small
(10 accounts) so workers contend for the same accounts. The test asserts:

- **No overlapping claims**: `sql/claim-audit.sql` (test-only trigger on every inbox table) records every claim from the
  moment a worker sets `lock_token` until that worker clears it, ordered by a sequence; no two claims on the
  same account may overlap.
- No out-of-order write: in every record's write history, the version carried in the data (`Description`
  `vN` on the Account, `Score__c` on the Profile) only moves forward. Every event ends `done` or `superseded`
  (superseded: claimed together with a newer event of its account).
- Every Account and Profile ends with the version 25 values.
- Every account was claimed repeatedly (real contention) and all 3 pods wrote to Salesforce (`/__fake/clients`, per
  User-Agent).

Mutation checks on the current claim (each reverted afterwards):

- Without the in-flight checks (the lease conditions, the claim lock, the lease re-check in `ClaimEntities` and the
  head's due check): this test failed with 10,119 overlapping claims.
- Without the lease conditions, the claim lock and the lease re-check, but with the head's due check: this test
  still passed (a leased head is not due, so it is never locked). The protections overlap; `ClaimRaceTests` is what
  covers the claim lock and the lease re-check.
- Without `SKIP LOCKED` only: both passed (a second worker waits for the row lock and then sees the lease); it costs
  throughput, not correctness.

`Pods_survive_their_Postgres_connections_being_dropped` repeats this while every worker and consumer connection
is terminated (`pg_terminate_backend`) every 200 ms (the test's own sessions are spared by their
`application_name`): every event still ends `done`, with no overlapping claims and
no out-of-order writes. The fixture allows 15 short Postgres retries (`Postgres:Retry`, at most about 12 s, inside
the 20 s lease), so a unit of work outlasts the chaos instead of giving up and leaving its lease to expire.

## Benchmarks

Only the Salesforce HTTP endpoint is mocked; the Postgres inbox, workers, client and rate limiter are real.

```bash
# use a dedicated database: the benchmark truncates the inbox tables and resets sf_rate_limit
export ConnectionStrings__benchdb="Host=localhost;Port=5432;Database=benchdb;Username=postgres;Password=postgres"

dotnet run -c Release --project benchmarks/SalesforceSync.Benchmarks -- --filter '*'             # everything, about 20 minutes
dotnet run -c Release --project benchmarks/SalesforceSync.Benchmarks -- --filter '*InboxDrain*'
dotnet run -c Release --project benchmarks/SalesforceSync.Benchmarks -- --filter '*Micro*'
```

Results, the SQL statement measurements and the before/after of this tuning: [docs/benchmarks/README.md](docs/benchmarks/README.md).

The micro benchmarks need no database: header parsing, coalescing, the typed mapping (`MapAccountEvent`: event JSON →
payload → sObjects → records; `MapPayloadToSObject` and `MapSObjectToRecord` for its two halves) and the type-aware
diff.

Both drain benchmarks run P **pods** (independent service providers: own connection pool, rate limiter and
HTTP client, like separate processes) x W worker loops against one Postgres inbox and one org. Each iteration
**fails** if any account doesn't end at its newest version, any record's write history shows an older version
after a newer one, any inbox row isn't `done` or `superseded`, or the claim audit shows two overlapping claims on
the same entity. Each iteration prints a `//` line with the mock org's counters (`api`, `writeRequests`,
`recordsOk`, ...), `done` (entity syncs) and `superseded` rows, the coalescing (events per entity sync), claims,
retries, overlaps and regressions.

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
[Salesforce Developer Limits and Allocations Quick Reference](https://developer.salesforce.com/docs/platform/salesforce-app-limits-cheatsheet/guide/salesforce-app-limits-platform-api.html),
with the response header and status codes from the REST API Developer Guide
([Limit Info Header](https://developer.salesforce.com/docs/platform/api-rest/guide/headers-api-usage.html),
[Composite](https://developer.salesforce.com/docs/platform/api-rest/guide/resources-composite-composite-post.html)).
Set the org's daily API allocation (`DailyApiAllocation`, the number in Setup: System Overview, API Requests, Last
24 Hours) and its concurrent long-running request limit (`ConcurrentLongRunningLimit`); the worker logs its limits at startup and refuses to start on a configuration that can't
work, including a missing or non-positive `DailyApiAllocation`. The configured allocation is the maximum: Salesforce
reports the org's real one on every REST response (`Sforce-Limit-Info: api-usage=25/5000`, a limit of 5,000), and
when that is lower the budget follows it. A response without the header, or with one that can't be read, leaves the
budget on the configuration.

```json
"SalesforceLimits": {
  "DailyApiAllocation": 115000,     // required: the org's calls per 24 hours, from Setup
  "ConcurrentLongRunningLimit": 25, // the org's limit: 25 for production and sandboxes, 5 for Developer Edition and trials
  "LongRunningThreshold": "00:00:20",
  "RequestTimeout": "00:01:30"
},
"RateLimit": {
  "MaxRequestsPerSecond": 2,        // token bucket refill, all pods together
  "BurstCapacity": 5,               // token bucket size
  "DailyBudgetShare": 0.5,          // this integration's share of the org's daily allocation
  "DailySoftRatio": 0.8, "DailySoftSpeed": 0.25,
  "DailyHardRatio": 0.95, "DailyHardPause": "00:15:00",
  "ConcurrentLimitPause": "00:00:30",
  "MaxWait": "00:00:20"
}
```

| Salesforce limit | How it's applied |
|---|---|
| **Total API requests per 24 hours**: depends on the edition, licenses and add-ons (e.g. 115,000 for an Enterprise org with 15 Salesforce licenses); Setup shows the org's number | `SalesforceLimits:DailyApiAllocation` (required) as the maximum, lowered to the limit in `Sforce-Limit-Info: api-usage` when Salesforce reports less. The allocation is shared by every user and integration of the org, so this integration gets `DailyBudgetShare` of it. Every call is counted in `sf_api_usage` (10-minute buckets) and checked under the shared row lock, so all pods together never exceed the budget over any rolling 24 hours. When it's used up, events are deferred until the oldest calls leave the window. Usage is rolling, not reset at midnight. |
| **What a call is**: each HTTP request; a Composite request (and an sObject Collections request) counts as one call however many subrequests it holds | One token per HTTP request: a describe (cached), each query page and each Composite write. |
| **Org-wide usage** (also other integrations): `Sforce-Limit-Info: api-usage=used/limit` on every REST response | `api-usage` and, when Salesforce sends it, `per-app-api-usage` (the connected app's quota): slow down at `DailySoftRatio`, pause at `DailyHardRatio`. The allocation Salesforce reports, the configured maximum and the resulting budget are logged when first seen and whenever Salesforce's changes. `api-bursts` appears in Salesforce's example of the header but isn't defined, so it is recorded in `sf_limit_status` and not acted on. |
| **Requests per second**: Salesforce publishes no such limit, and none on concurrent requests shorter than 20 s | `MaxRequestsPerSecond` and `BurstCapacity` are this integration's own pacing, so it spreads its share of the daily allocation instead of spending it in bursts. |
| **Concurrent long-running requests** (20 s or longer): 25 for production and sandboxes, 5 for Developer Edition and trials | `SalesforceLimits:ConcurrentLongRunningLimit` (default 25). `Sync:MaxConcurrentRequests` (global advisory-lock slots) must not exceed it, or startup fails. Calls that take `LongRunningThreshold` or longer are logged. `REQUEST_LIMIT_EXCEEDED` with `ConcurrentPerOrgLongTxn` pauses all workers for `ConcurrentLimitPause`. |
| **Total requests exceeded** (`REQUEST_LIMIT_EXCEEDED`, `TotalRequests`) | Pauses all workers for `DailyHardPause`. Salesforce may allow some overage on paid orgs, but it isn't relied on. |
| **API timeout**: 10 minutes for REST calls, for a composite request as a whole (query calls follow the SOQL limits and fail with `QUERY_TIMEOUT`, retried as transient) | `RequestTimeout` (HTTP client timeout, including the token wait) must be at most 10 minutes, or startup fails. The worker also gives up at 80% of its lease. |

The limiter is registered by `AddSalesforceRateLimiting()`, which the worker (`AddSalesforceSyncWorker`), the
benchmark pods and the test pods all use. Its handler is always the outermost on the Salesforce client (rate limiter
→ gzip → auth), so every request takes a token before it is compressed and authenticated.

Every pod applies the bucket size, rate and budget to the shared `sf_rate_limit` row at startup, so roll out
configuration changes to all pods together. `sf_limit_status` shows the last reported usage, this integration's
own calls over the last 24 hours and its budget.

### Concurrency slots: Postgres advisory locks

`SalesforceConcurrencySlots` caps how many batches are calling Salesforce at once **across all pods**, so the
integration stays under the org's concurrent long-running limit (25, or 5 on Developer Edition).

**How it works.** There is no table for it. `pg_try_advisory_lock(key)` is a built-in Postgres function that
locks an application-chosen number, held in Postgres's lock manager (in memory). There are
`Sync:MaxConcurrentRequests` slots, keys `0x53460000 + 0 … N-1`. Before calling Salesforce, a worker opens a
connection and tries the keys in random order:

- it gets `true` → it holds that slot for the batch, then calls `pg_advisory_unlock(key)`;
- `false` comes back immediately if another session holds that key, so there is no waiting;
- all `false` → every slot is busy, so the worker skips this round and claims nothing.

If a pod crashes or its connection drops, Postgres ends the session and releases its locks. A slot can't leak,
so there is no expiry, heartbeat or cleanup job. (`pg_advisory_xact_lock` in `InboxSchema` does the same for
schema setup: it lets one pod at a time apply `schema.sql`, released at commit.)

**Why this rather than the alternatives**

| Option | Why not (here) |
| --- | --- |
| `SemaphoreSlim` per pod | Only limits one process. Splitting the limit statically (`25 / pods`) breaks when pods scale up or down and wastes slots when some pods are idle. |
| Slot rows in a table with a lease (`locked_until`) | Works, but a crashed holder keeps the slot until the lease expires, needs heartbeats for long calls, and every acquire/release is a write (WAL, vacuum). |
| Redis semaphore / Redlock | Another piece of infrastructure to run, and it's also TTL-based, with the same expiry trade-offs. |
| Counting in-flight rows in the inbox tables | Needs a lock or `serializable` isolation to count and claim atomically, and is easy to get subtly wrong. |
| **Advisory locks** | Postgres is already the coordinator. Acquiring is one non-blocking statement, a crashed holder is released automatically, and nothing is written to disk. |

**Is it best practice?** Yes, for a distributed semaphore or mutex when Postgres is already shared state.
It's the documented use of advisory locks ("locks with application-defined meanings"), and established
Postgres-backed job systems use them for the same purpose (e.g. Que and GoodJob for job locking, and Flyway
for its migration lock on Postgres). It's the wrong tool if no shared Postgres exists, or if slots must
outlive a connection.

**Trade-offs and how they're handled**

- **Session-level locks need a stable session.** Use Npgsql's own connection pooling (a slot is a pooled `SyncDbContext` whose connection is opened explicitly and held
  until the slot is disposed) or a pooler in *session* mode, not PgBouncer in *transaction* mode: there the unlock can
  run on a different server connection, so the lock would stay held.
- **A held slot keeps a connection open** for the whole batch, in addition to the short claim/outcome
  connections. Size `Maximum Pool Size` per pod above `Workers` (one slot connection per worker plus headroom),
  and Postgres `max_connections` for all pods together.
- **Pooled connections:** a slot is always unlocked explicitly before its connection goes back to the pool.
  As a backstop, Npgsql resets a pooled connection with `DISCARD ALL` (which also drops advisory locks) when
  it's next used, and a broken connection is discarded, which ends the session.
- **Acquire cost** is at most N quick round trips when all slots are busy. That's negligible for N ≤ 25. The
  random order spreads pods across keys so they don't all contend for slot 0.
- **Key namespace:** keys are `0x5346xxxx` ("SF"). Advisory locks are per database, so other applications in
  the same database must not use the same range.

To see the slots held right now:

```sql
select pid, objid::bigint - 1397096448 as slot, application_name, client_addr, state
from pg_locks join pg_stat_activity using (pid)
where locktype = 'advisory' and classid = 0 and objid::bigint between 1397096448 and 1397096448 + 100;
```

## Using a real Salesforce org

The steps below connect the sync to a sandbox. The same steps work for production, but use a sandbox first. Every
Salesforce screen named here is under **Setup** (the gear icon, top right). Type the name into **Quick Find** to
open it.

### 1. Find the org's API address

Lightning shows the org at an address like `https://acme--dev.sandbox.lightning.force.com`. The API uses the org's
**My Domain** address instead: replace `lightning.force.com` with `my.salesforce.com`.

| What you see in the browser | `Salesforce:InstanceUrl` |
|---|---|
| `https://acme--dev.sandbox.lightning.force.com` (sandbox) | `https://acme--dev.sandbox.my.salesforce.com` |
| `https://acme.lightning.force.com` (production) | `https://acme.my.salesforce.com` |

**Setup → My Domain** shows the exact address. Use it, not `login.salesforce.com` or `test.salesforce.com`: the
client credentials flow only works on the org's own domain. The token URL is the same address plus
`/services/oauth2/token`.

### 2. Create the user the sync runs as

With the client credentials flow there is no login. Salesforce runs every API call as one user that you choose, so the
sync can do only what that user may do.

1. **Users → New User**. User License **Salesforce Integration**, Profile **Minimum Access - API Only Integrations**.
   Give it a name like "Salesforce Sync" and a real email address you can read. The username must be unique across
   all Salesforce orgs, for example `salesforce-sync@acme.dev-sandbox`.
2. **Permission Sets → New**. Label `Salesforce Sync`, License **Salesforce API Integration**. In **Object Settings**,
   give it **Read, Create and Edit** on Account, Profile and Account Relationship (the objects created in step 4), and
   **Edit** access on every field listed in step 4. Delete isn't needed.
3. On the user: **Permission Set License Assignments → Edit Assignments → Salesforce API Integration**, then
   **Permission Set Assignments → Edit Assignments → Salesforce Sync**.

If the org has no Salesforce Integration licenses left, use a standard Salesforce user with a profile that has **API
Enabled**. For a first test in a sandbox you can also run as your own administrator user, but don't do that in
production.

### 3. Create the app and get the client id and secret

Salesforce calls the client id and secret the **Consumer Key** and **Consumer Secret**. They belong to an **External
Client App** (newer orgs) or a **Connected App** (older orgs). New orgs can't create Connected Apps any more, so
start with an External Client App.

**External Client App:**

1. **External Client App Manager → New External Client App**. Name `Salesforce Sync`, a contact email, Distribution
   State **Local**.
2. Under **API (Enable OAuth Settings)**, tick **Enable OAuth**.
   - **Callback URL**: `https://login.salesforce.com/services/oauth2/callback`. It's required but this flow never uses
     it.
   - **OAuth Scopes**: add **Manage user data via APIs (api)**. Nothing else is needed.
   - **Flow Enablement**: tick **Enable Client Credentials Flow** and confirm.
   - Save with **Create**.
3. Open the app's **Policies** tab → **Edit**. Under **OAuth Policies**, tick **Enable Client Credentials Flow** and
   set **Run As (Username)** to the user from step 2. Save.
4. Open the **Settings** tab → **OAuth Settings** → **Consumer Key and Secret**. Salesforce emails you a verification
   code. Copy the **Consumer Key** (the `ClientId`) and the **Consumer Secret** (the `ClientSecret`).

**Connected App** (only if the org still offers **App Manager → New Connected App**):

1. Name `Salesforce Sync`, a contact email, tick **Enable OAuth Settings**, the same callback URL, scope **Manage user
   data via APIs (api)**, and tick **Enable Client Credentials Flow**. Save.
2. **App Manager** → the app's row menu → **Manage → Edit Policies**: under **Client Credentials Flow**, set **Run As**
   to the user from step 2. Save.
3. **App Manager** → the app's row menu → **View → Manage Consumer Details** shows the key and secret.

A new or changed app can take up to 10 minutes to work. Check it before going on (replace the three values):

```bash
curl -s https://acme--dev.sandbox.my.salesforce.com/services/oauth2/token \
  -d grant_type=client_credentials -d client_id='<Consumer Key>' -d client_secret='<Consumer Secret>'
```

A JSON answer with an `access_token` means it works. The usual errors:

| Error | Cause |
|---|---|
| `request not supported on this domain` | The URL isn't the org's My Domain address (step 1). |
| `no client credentials user enabled` | **Run As** isn't set in the app's policies (step 3). |
| `invalid_client` / `invalid_client_id` | Wrong key or secret, or the app is less than 10 minutes old. |

Treat the secret like a password: never commit it. The next steps keep it in .NET user secrets, which live in your
home folder, outside the repository.

### 4. Create the objects and fields

The sync writes three objects. Account is standard; `Profile__c` and `Account_Relationship__c` are custom objects to
create. Create them and their fields in **Object Manager** (**Create → Custom Object**, then **Fields &
Relationships → New** on each object). The API name is what you type in **Field Name** plus `__c`: Field Name
`External_Id` gives `External_Id__c`.

Every object needs an `External_Id__c` field with **Unique** and **External ID** ticked. The sync finds and links
records by it, never by the Salesforce Id.

**Account** (standard object). These standard fields need nothing: `Name`, `Rating`, `Industry`, `Description`,
`AnnualRevenue`, `NumberOfEmployees`, `Phone`, `Website`. Add:

| Field Name → API name | Type |
|---|---|
| `External_Id__c` | Text(255), Unique, External ID |
| `Active__c` | Checkbox |
| `Tags__c` | Picklist (Multi-Select). Values the events send, e.g. `B2B`, `Retail` |
| `Customer_Since__c` | Date |
| `Last_Contacted__c` | Date/Time |

**`Profile__c`** (custom object). Label `Profile`, Object Name `Profile`, Record Name `Profile Name` of type Text.
If Salesforce refuses the name, pick another and change it on `ProfileSObject` (see [field mappings](#field-mappings)).

| Field Name → API name | Type |
|---|---|
| `External_Id__c` | Text(255), Unique, External ID |
| `Account__c` | Lookup Relationship to Account |
| `Tier__c` | Picklist: `Bronze`, `Silver`, `Gold`, `Platinum` |
| `Score__c` | Number(16, 2) |
| `Discount__c` | Percent(3, 2) |
| `Notes__c` | Text Area(255) |
| `Opt_In__c` | Checkbox |
| `Start_Date__c` | Date |

**`Account_Relationship__c`** (custom object). Label `Account Relationship`, Object Name `Account_Relationship`,
Record Name `Relationship Name` of type Text.

| Field Name → API name | Type |
|---|---|
| `External_Id__c` | Text(255), Unique, External ID |
| `Account__c` | Lookup Relationship to Account (Child Relationship Name e.g. `Relationships`) |
| `Related_Account__c` | Lookup Relationship to Account (a different Child Relationship Name, e.g. `Related_Relationships`) |
| `Type__c` | Picklist: `Parent`, `Subsidiary`, `Partner`, `Reseller` |
| `Start_Date__c` | Date |
| `Active__c` | Checkbox |

While creating each field, Salesforce asks for field-level security: make it visible (not read-only) for the
`Salesforce Sync` permission set, or grant field access in the permission set afterwards.

**Record types.** Every object needs the record type its sObject class names (`RecordType` in `[SObject]`, see
[Record types](#record-types)):

| Object | Record type developer name (Record Type Name) |
|---|---|
| Account | `Business` |
| `Profile__c` | `Standard` |
| `Account_Relationship__c` | `Standard` |

1. **Object Manager → (object) → Record Types → New**. Record Type Label e.g. `Business`; the **Record Type Name**
   (the developer name) must be exactly the name above. Tick **Active**. Use names that already exist in your org by
   changing `RecordType` on the sObject class instead.
2. Assign it to the user the sync runs as: **Permission Sets → Salesforce Sync → Object Settings → (object) → Edit**,
   and under **Record Type Assignments** tick the record type. (A user on a standard profile can have it assigned on
   the profile's **Record Type Settings** instead.)

A record type that is missing, inactive or not assigned fails the object's events with `INVALID_RECORD_TYPE`, retried
with backoff; they go through once the record type is fixed.

A picklist with **Restrict picklist to the values defined in the value set** ticked rejects any other value. That
event is then dead-lettered (`INVALID_OR_NULL_FOR_RESTRICTED_PICKLIST`). Add every value the source system sends, or
leave the picklist unrestricted.

### Field mappings

A mapping is code, in three places:

1. **Payload** (`Inbox/Payloads/`): the event's JSON fields.
2. **sObject** (`Salesforce/SObjects/`): the Salesforce object and field API names and the record type, in the
   `[SObject]` and `[JsonPropertyName]` attributes. This is the only place Salesforce names appear.
3. **Mapper** (`Sync/Mapping/`): one line per field, from the payload to the sObject.

The mappings as they are:

| Event field | Salesforce field |
|---|---|
| `account.externalId` | `Account.External_Id__c` |
| `account.name`, `rating`, `industry`, `description`, `annualRevenue`, `numberOfEmployees`, `phone`, `website` | `Account.Name`, `Rating`, `Industry`, `Description`, `AnnualRevenue`, `NumberOfEmployees`, `Phone`, `Website` |
| `account.isActive` | `Account.Active__c` |
| `account.tags` (a list) | `Account.Tags__c` (values joined with `;`) |
| `account.customerSince` | `Account.Customer_Since__c` |
| `account.lastContactedAt` | `Account.Last_Contacted__c` |
| `account.profile.externalId` (the account's own if missing) | `Profile__c.External_Id__c` |
| the account itself | `Profile__c.Account__c` |
| `account.profile.name`, `tier`, `score`, `notes`, `optIn`, `startDate` | `Profile__c.Name`, `Tier__c`, `Score__c`, `Notes__c`, `Opt_In__c`, `Start_Date__c` |
| `account.profile.discountPercent` | `Profile__c.Discount__c` |
| `accountRelationship.externalId`, `name`, `type`, `startDate`, `isActive` | `Account_Relationship__c.External_Id__c`, `Name`, `Type__c`, `Start_Date__c`, `Active__c` |
| `accountRelationship.accountExternalId` | `Account_Relationship__c.Account__c` (the Account with that external id) |
| `accountRelationship.relatedAccountExternalId` | `Account_Relationship__c.Related_Account__c` |
| (none: the sObject class's `RecordType`) | `RecordTypeId` of every object: the Id of `Business` (Account) or `Standard` (`Profile__c`, `Account_Relationship__c`), from the cached Describe |

To change a mapping:

- **A field has a different API name in your org** (e.g. `LLC_BI__Active__c`): change the string in its
  `[JsonPropertyName("...")]` on the sObject class. Same for an object name in `[SObject("...")]`, and for a record
  type in `[SObject(..., RecordType = "...")]`.
- **Drop a field**: remove its property from the sObject class and its line from the mapper. A field the sync doesn't
  know is never touched.
- **Add a field**: add the property to the payload class (the JSON name in camelCase), the property with its
  `[JsonPropertyName]` to the sObject class, and one line to the mapper. Add it to `FakeSchema`
  (`src/SalesforceSync.FakeSalesforce/FakeSchema.cs`) too: a unit test checks every sObject name against the mock org,
  and the mock org rejects fields it doesn't know.
- **A new object**: see [Adding a Salesforce object](#adding-a-salesforce-object). In `FakeSchema`, give it a
  `RecordTypeId` field and its record type in `RecordTypes`.

Every property on an sObject class is the record's full desired state: an event without a value clears that field in
Salesforce.

### 5. Run against the sandbox

Store the values from steps 1 and 3 in the AppHost's user secrets. When they are set, the AppHost skips the mock org,
points the worker at the sandbox, and makes the load generator send a small run (200 events over 20 accounts) instead
of 20,000.

```bash
cd src/SalesforceSync.AppHost
dotnet user-secrets set "Salesforce:InstanceUrl" "https://acme--dev.sandbox.my.salesforce.com"
dotnet user-secrets set "Salesforce:ClientId" "<Consumer Key>"
dotnet user-secrets set "Salesforce:ClientSecret" "<Consumer Secret>"
dotnet user-secrets set "SalesforceLimits:DailyApiAllocation" "5000000"
cd ../..
dotnet run --project src/SalesforceSync.AppHost
```

`DailyApiAllocation` is the org's number of API calls per 24 hours: **Setup → Company Information → API Requests, Last
24 Hours** shows it (sandboxes usually have 5,000,000). `dotnet user-secrets clear` (in `src/SalesforceSync.AppHost`)
goes back to the mock org.

Then check the result in the sandbox, for example in **Developer Console → Query Editor**:

```sql
SELECT External_Id__c, Name, Description, Active__c, LastModifiedDate FROM Account WHERE External_Id__c LIKE 'ACC-%'
SELECT External_Id__c, Account__r.External_Id__c, Tier__c, Score__c FROM Profile__c
SELECT External_Id__c, Account__r.External_Id__c, Related_Account__r.External_Id__c, Type__c FROM Account_Relationship__c
```

The `salesforce-sync` logs in the Aspire dashboard show each batch, and pgAdmin shows the inbox tables: every row should
end `done` or `superseded`. A `dead` row's `last_error` says what Salesforce rejected. Delete the test records
afterwards: `ACC-…`, `PRF-…` and `REL-…`.

To run the worker without Aspire (a server or Kubernetes), give it the same settings as environment variables
(`Salesforce__InstanceUrl`, `Salesforce__TokenUrl`, `Salesforce__ClientId`, `Salesforce__ClientSecret`,
`SalesforceLimits__DailyApiAllocation`), plus `ConnectionStrings__inboxdb` and `ConnectionStrings__kafka`, and
`ConnectionStrings__redis` so the replicas share their Describe results (optional: without it each process describes on
its own; see [The Describe cache](#the-describe-cache)). With Aspire, Redis needs no setup: the AppHost starts it and
passes the connection string.

### 6. Tune and check

- Choose this integration's share of the daily allocation with `RateLimit:DailyBudgetShare`, and set
  `SalesforceLimits:ConcurrentLongRunningLimit` (5 on Developer Edition and trial orgs). See
  [Salesforce API limits](#salesforce-api-limits).
- Tune `Salesforce:PermanentErrorCodes`, `Sync:RetryBaseDelay` / `MaxRetryDelay` / `MaxRetries` and `Postgres:Retry`
  if needed. `Sync` values that can't work (no workers or slots, `BatchSize` outside 1-100, ...) and a missing
  `Salesforce:InstanceUrl` fail startup.
- The mock org was built from Salesforce's documentation. These behaviours are worth confirming in the sandbox:
  - an empty checkbox (`null`) is stored as `false`, and an empty text (`""`) as empty;
  - a number with more decimals than the field allows is rounded;
  - a Date/Time keeps whole seconds;
  - updating a record whose Id no longer exists, and linking to an Account that doesn't exist, fail with a clear error
    code;
  - a required field that has a default value can be left out on create;
  - concurrent workers on the same account, and a worker killed mid-batch, still end with every account at its newest
    version.

## Status

Scaffold, not yet production-tested. Verify in a Salesforce sandbox, in particular concurrent workers on
the same account and killing a worker mid-batch.

## License

[MIT](LICENSE)
