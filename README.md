# salesforce-kafka-inbox

Kafka change events → Postgres inbox → Salesforce, with per-entity ordering, atomic multi-record upserts,
type-aware change-only updates and a shared, limit-aware rate limiter. One generic engine syncs every object;
each object only defines its typed payload, its Salesforce sObject class, the mapper between them and its queries.
Written in C# / .NET 8.

## The problem

An upstream system publishes account changes, account-to-account relationships and contacts to Kafka. Each account
change has to land in Salesforce as an update to an **Account** and its related **Profile**; each relationship
as an **Account_Relationship__c** linking two Accounts, which may not be synced yet when the relationship arrives;
each contact as a **Contact** of its Account, which may not be synced yet either. Several things make this harder than
"consume and PATCH":

1. **Out-of-order writes across parallel batches.** The same account can change twice within a few
   seconds. If those two events land in different batches running in parallel, the newer one can
   reach Salesforce first and then be overwritten by the older one, so Salesforce ends up with stale
   data. Events may also simply arrive out of order: the producers' Kafka key can't be relied on.
2. **Account and Profile belong together, and one bad record must not hold back the batch.** They are
   written in the same batch, each record saved or rejected on its own; an account counts as synced
   only when both are in, and is retried until they are (see [Writing to Salesforce](#writing-to-salesforce)).
3. **Only send what changed.** Blind updates fire triggers and flows, pollute field history and burn
   API calls. Each record is read, diffed and only the changed fields are sent; new records are created
   (why, and the alternative: [Why compare with Salesforce before writing](#why-compare-with-salesforce-before-writing)).
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
| Durable, replayable intake | Kafka consumer writes each event's **raw JSON**, unchanged, to that object type's Postgres **inbox table** (`sf_account_inbox`, `sf_account_relationship_inbox`, `sf_contact_inbox`) with the entity's id in a plain `entity_id` column, and marks the entity due on that row in the same statement, before storing its offset (committed in the background, so there is no broker round trip per message). The id is read from the typed event in C#; **no SQL reads inside an event**. An inbox keeps **one row per entity**, holding its newest event: a newer event overwrites it, an older one changes nothing. A redelivery overwrites the row with itself and ends `superseded`. A message that isn't a valid event of its topic's type (see below) is logged with its offset and skipped, rather than blocking its partition. |
| Ordering across parallel batches | Workers claim **entities, not events**: every entity has one inbox row, which holds its sync state too (due time, lease, watermark), and a claim is `SELECT … FOR UPDATE SKIP LOCKED` on the due rows, which it then leases (`lock_token`, and `next_attempt_at` moved to the lease's end, so a leased row is not due until then; `attempt_count + 1`) and reads, in one statement. Two workers can never hold the same entity: they would lock the same row. Which event is the newest is decided by **`meta.time`**, not by arrival: the Kafka key and partition order are not used. |
| Bursts of updates to one entity | Pending events for an entity are **coalesced** in the inbox: every event carries the full record, so an event with a newer `meta.time` (or the same, stored later) overwrites the entity's row, and what a claim writes is the newest event stored before it. Five updates in five seconds become one Salesforce write with the newest data. An idle entity is due `Sync:CoalesceDelay` (50 ms) after its first new event, so the rest of a burst overwrites the row before it is claimed: fewer calls, for a sync up to that much later. An event that overwrites a row while a worker writes the event it claimed is not lost: that worker's outcome is recorded only on rows that still hold the event time it claimed, and the newer event is claimed next. |
| Late or replayed older events | **Sync watermark** in the entity's inbox row (`synced_time`): when the event written to Salesforce ends `done`, its `meta.time` becomes the entity's `synced_time` (never moving back), in the same statement and only while the worker still holds the lease. A claimed event at or before the watermark is marked `superseded` without touching Salesforce. Nothing is stored in Salesforce for this. |
| Expired leases | The Salesforce call is cancelled at 80% of the lease (measured from the claim), so a slow worker gives up before another worker can claim the entity. A worker whose lease did expire can't record anything: every outcome statement and the watermark are conditional on its `lock_token` on the entity's row. |
| Writes whose outcome is unknown | A write that timed out, was cancelled or lost its connection may still be applied by Salesforce later. Its entities are **held** (`UNKNOWN_OUTCOME`) for `Sync:UnknownOutcomeHold` (10 minutes) before anyone may write them again, so a late write can't overwrite newer data. See [Unknown outcomes](#unknown-outcomes). |
| Records that belong together | Written in the same batch, **parents first**: one Composite request of the Accounts' **Collections upsert**, then the Profiles'. The org's foreign keys are **Text fields holding the parent's external id** (`Profile__c.AccountId__c` = the account key), declared with `[SObjectForeignKey]`: written as the text, never resolved to an Id, never a reference. A parent of another inbox (a relationship's or a contact's Account) must already be in Salesforce: the read checks, and a missing one makes the entity wait (`DEPENDENCY_NOT_SYNCED`) with nothing sent. Each record is saved or rejected on its own (`allOrNone: false`); the entity is done, and its watermark advanced, only when all of its records are in. A child whose new parent failed is still sent (same request): the org decides, and the parent's error decides the entity. (For orgs with real **Lookup** fields the engine still resolves them to Ids and links a new child to its new parent by `@{reference}` in the same Composite request.) See [Writing to Salesforce](#writing-to-salesforce). |
| Create or update | Decided by the read alone: a record Salesforce has is updated with what differs, one it doesn't have is created. The event's `meta.type` is not consulted. |
| Change-only updates | **Type-aware diff against Salesforce**: the current records are read by external id with every field of their sObject class (an event is the full record; an empty field is cleared if Salesforce has a value). Values are normalized with the fields' cached Describe metadata (number scale, checkbox `null` → `false`, 15 → 18-character ids, multi-select as a set, date/time to the second and time to the millisecond, int/long within their `digits`, 18-character ids checked against their case suffix, `""` = `null`) and only fields that differ are sent, in their normalized form, so a value Salesforce stores differently never rewrites itself on every event. The comparison is with what is in Salesforce *now*, so edits made directly in Salesforce are corrected. |
| Batching and API calls | **1 read call per batch** of up to `Sync:BatchSize` entities (75, max 100): one Composite request with all reads (≤5 queries of ≤100 external ids, the row type's `MaxValuesPerQuery`, each query's URL within Salesforce's 16,384-byte limit on a call's URI and headers; only very long ids, which split the queries further, take a second read call per 5 more queries). Then, after the diff, the writes: **one sObject Collections upsert per level and object** (≤180 records, the row type's `MaxRecordsPerWrite`, under Salesforce's 200), by external id with exactly the planned fields, grouped into write requests of ≤5 upserts (more are split over further requests), **one call each**: a request of one upsert (writes of one object) is that upsert on its own; a request of several (Accounts and their Profiles, or a new child linked to its new parent by a Lookup's `@{reference}`) is **one Composite request** of them, parents first. So a batch of new Accounts with their Profiles costs 2 calls (one read, one Composite write), Account-only changes (relationships, contacts likewise) 2 (one read, one upsert), and Account plus Profile changes 2. The rule is the engine's, not the example objects': any strategy, with any objects, gets it. See [One request per batch](#one-request-per-batch). Describe is cached and revalidated with `If-Modified-Since`. Request bodies are gzipped, responses accepted compressed. |
| Record types | Every synced object has a record type, named by developer name on its sObject class (`[SObject("Account", …, RecordType = "Business")]`). Its Id comes from the `recordTypeInfos` of the object's cached Describe, so it costs no extra call, and every record is written with that `RecordTypeId` (diffed like any other field). A record type the org doesn't offer fails the entity before any call (`INVALID_RECORD_TYPE`, retried) and makes the next batch describe again. See [Record types](#record-types). |
| Bad records | A value Salesforce would reject (unknown or read-only field, too long, wrong type, required field emptied) fails only its own unit, before any call; a malformed event is dead-lettered on its own (`INVALID_EVENT`). |
| Failures | Each record's result decides per entity: **permanent** (validation rules, invalid fields, ...; configurable) → dead-lettered immediately, alone; **transient** (`UNABLE_TO_LOCK_ROW`, a parent not synced yet, a create that lost a race, unknown codes) → exponential backoff, dead-lettered after `MaxRetries`; **not attempted** (skipped because of another entity's record) → back to the inbox without spending an attempt. The code is stored in the inbox row's `last_error_code`. See [Retries and dead letters](#retries-and-dead-letters). |
| Postgres failures | Every unit of work (claim, outcomes, ingest, rate limiter, slots, schema) runs through EF Core's execution strategy (**`PostgresRetryStrategy`**, a `NpgsqlRetryingExecutionStrategy` configured from `Postgres:Retry`, used as `db.Database.CreateExecutionStrategy().ExecuteAsync(...)` around a transaction): a dropped connection, failover, timeout, deadlock or serialization failure re-runs the whole unit (new transaction, change tracker cleared) with exponential backoff and jitter. After a lost connection the Npgsql pool is cleared, since its idle connections are usually dead too. Every unit is safe to run twice; a claim retried after an unknown commit first releases what the lost attempt leased. A batch's outcomes are recorded with one statement per kind of outcome, not per entity. |
| Rate limits | Configured from the org's daily API allocation and Salesforce's published limits (see [Salesforce API limits](#salesforce-api-limits)): Postgres-backed **token bucket** shared by all processes; this integration's **share of the org's daily allocation** enforced as a hard cap over a rolling window (`RateLimit:BudgetWindow`, 24 hours); global **concurrency cap** (advisory locks) validated against the org's concurrent long-running limit; the `Sforce-Limit-Info` header (`api-usage`, and `per-app-api-usage` when sent) and own usage slow everyone down at 80% and pause at 95%; 503 (and 429 from anything in front of Salesforce) and both kinds of `REQUEST_LIMIT_EXCEEDED` pause all workers. Rate-limited batches are handed back without spending an attempt. |

```
Kafka (any key, any order) ─► KafkaInboxConsumer ─► one inbox table per object type (Postgres), one row per
  account-changes              typed read + validate,   │  entity: entity_id + raw event JSON + status, and its
  account-relationship-changes store raw + entity_id    │  sync state (due time, lease, watermark)
  contact-changes              + mark the entity due    │
                                                       │  claim entities: their inbox rows, SKIP LOCKED
                                                       ▼
                              InboxWorkerService ×N loops (any number of pods), round-robin over:
                                AccountSync ─┐                   (strategy: typed event → mapping →
                                AccountRelationshipSync ─┤                 sObject → records, + queries)
                                ContactSync ─┤
                                                                ▼
                              SalesforceBatchUpsertService.UpsertBatchAsync<TEvent, TRow>(strategy, IRepository<TRow>)
                                claim → newest meta.time per entity (supersede stale) → map
                                → describe → normalize → 1 composite read → decide (diff + parents)
                                → write: 1 collections upsert per level and object, parents first:
                                  1 object → the upsert alone, 2+ → 1 composite of them
                                → per-record results → outcome per entity → record outcomes
                                                       │  inbox: IRepository<TRow> (EF Core + raw SQL, PostgresRetryStrategy)
                                                       │  token bucket + daily budget + concurrency slots
                                                       ▼
                                                  Salesforce  (standard + existing fields only)
```

### How the code is layered

The sync is three layers, each with one job:

| Layer | Classes | Does |
|---|---|---|
| **Thin I/O** | `SalesforceRestClient` (the only class that talks to Salesforce: `DescribeAsync`, `QueryAsync`, `UpsertAsync`); `InboxRepository<TRow>` (`IRepository<TRow>`: store, claim, record, hold, defer, fail, over `SyncDbContext`, `InboxSql` and `PostgresRetryStrategy`); `SalesforceBatchGate` (concurrency slot + shared pause) | Sends requests and returns raw results. Decides nothing. |
| **Business flow** | `SalesforceBatchUpsertService` | `UpsertBatchAsync` is the sync as a list of steps: claim → supersede an entity's event if it isn't newer than the watermark → map → `WriteToSalesforceAsync` (describe → normalize → read → decide → send → interpret) → record outcomes, with the three ways a batch can fail (unknown outcome → hold, rate limited → defer, anything else → retry all). |
| **Rules, no I/O** | `PreparedBatch.Prepare` (read, watermark, map) and `PreparedBatch.Outcomes` (entity results → inbox outcomes), and in `Sync/Upsert/`: `RecordNormalizer` (values against Describe, via `FieldValues`), `ReadQueryBuilder` (the read's SOQL), `CurrentRecords` (what the read found), `WritePlanner` (changed fields, create / update / unchanged, parents first, lookups by Id or reference, `DEPENDENCY_NOT_SYNCED`) with `CompositeWrite.Pack` (one Collections upsert per level and object, the row type's `MaxRecordsPerWrite` records each, 5 per request: one alone, several as one Composite request), `WriteResultInterpreter` (per-record results → outcome per entity, for Composite subrequests and upserts alike) | Pure and deterministic; unit-tested without a database or HTTP. |

**Data access** is Entity Framework Core over Npgsql (`SyncDbContext`, pooled, on the app's `NpgsqlDataSource`). The
model defines every table in `OnModelCreating` (partial indexes with `HasFilter`, autovacuum settings with
`HasStorageParameter`), and EF migrations (`src/SalesforceSync/Data/Migrations`) create them at startup (see
[Schema and migrations](#schema-and-migrations)); `SchemaTests` checks the model against the real tables column by
column, and a unit test fails when the model has changed without a migration. Every inbox table has the same columns, so
they all live on the abstract `InboxRow`; `AccountInboxRow`, `AccountRelationshipInboxRow` and `ContactInboxRow` only
name their tables (independent entity types, no shared table) and their batch's query and write sizes, and the one
`InboxRepository<TRow>` (`IRepository<TRow>`, registered as an open generic) serves them all. The table name comes
from the EF model, the batch's query and write sizes from the row type (`IInboxRow`'s static `MaxValuesPerQuery` and
`MaxRecordsPerWrite`). What EF can express is LINQ (the rate limiter's usage count and writes); storing an event, the
claim, every outcome (done, superseded, released, fail, dead-letter, hold) and the end of a lease (also of a lost
claim attempt's leases) are the SQL texts of `InboxSql` run unchanged through
`Database.SqlQueryRaw` / `ExecuteSqlRaw`, their `@name` parameters bound from an anonymous object's properties
(`new { ids, times, token }`), since `FOR UPDATE SKIP LOCKED`, `UPDATE ... RETURNING` and `INSERT ... ON CONFLICT` have
no LINQ form. The rate limiter's `RateLimitSql` only locks its row (`FOR UPDATE`) and reads the database clock.

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
- **Validation before any call**: an unknown or read-only field (`INVALID_FIELD`), a lookup that isn't a reference
  (or a foreign key that is one), a value that would be rejected. The entity fails on its own instead of costing a write.
- **The read query**: which fields to select.
- **The diff**: whether a field is createable or updateable.
- **Record types**: the developer name → `RecordTypeId` (`recordTypeInfos`).

**Is it best practice?** Caching Describe and revalidating it with `If-Modified-Since` is what Salesforce's REST API
documentation recommends, and what generic integration tools do. The cost is about one call per object per refresh
interval: with 4 objects and 15 minutes, 384 calls a day (a 304 still counts as a call). That was per pod; with the
shared Redis layer it is for the whole deployment.

| Alternative | Gains | Loses |
|---|---|---|
| Upsert everything by external id (no Describe, no read, no diff) | Two fewer steps | Every event writes every field: triggers, flows, field history and `LastModifiedDate` fire on every event, and bursts multiply writes. A bad value costs a call and comes back as a per-record error. |
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
{ "meta": { "time": "2026-10-14T01:02:05.000Z", "type": "inserted" },
  "contact": { "externalId": "CON-1", "accountExternalId": "ACC-000001", "firstName": "Ada", "lastName": "Lovelace",
               "email": "ada@example.com", "phone": "+1 555 0101" } }
```

- **Entity type**: decided by the **topic** the event arrives on (`account-changes` → account,
  `account-relationship-changes` → accountRelationship, `contact-changes` → contact; `Kafka:Topics:<entityType>` overrides a type's topic). Each
  type has its own typed event class, its own inbox table and its own worker pass.
- **Entity id**: `<object>.externalId`, the record's Salesforce external id (`ExternalId__c`). It is read from the
  deserialized event (`UpsertStrategy.EntityId`) and stored in the inbox row's `entity_id` column.
- **Order**: `meta.time`, an ISO-8601 time with an offset or `Z`. **The Kafka message key is not used, and neither is
  arrival order**: an entity's events may come over any partition, through any consumer, in any order. The event with
  the newest `meta.time` wins; an event that is not newer than what was already synced is superseded. This relies on
  the source's clock. Two events of an entity with the same time: the one stored later overwrites the other's row,
  and is written unless the first one was already synced (then it is superseded).
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
- **Coalescing takes the newest event**, it doesn't merge: an entity's inbox row keeps only its newest event, so
  several events stored before a claim become one write of the newest full record (by `meta.time`; ties: the one
  stored later). Merging would keep a field the newest event no longer has.
- A required field must therefore be in every event: an account event without a `name` is dead-lettered
  (`REQUIRED_FIELD_MISSING`), because it would empty `Account.Name`.
- The one thing that isn't cleared by absence is a whole object: an account event **without a `profile`** writes
  only the Account and leaves the Profile in Salesforce as it is.

Other rules (in the strategies, `AccountSync`, `AccountRelationshipSync` and `ContactSync`):

- **account**: `profile.externalId` defaults to the account's `externalId`, and the Profile always belongs to the
  event's account (`AccountId__c` = the account's `externalId`).
- **accountRelationship**: `accountExternalId` and `relatedAccountExternalId` are the two Accounts (by external id,
  written to `AccountId__c` and `RelatedAccountId__c`). An event without both is dead-lettered (`INVALID_EVENT`), as is an event whose value has the wrong
  type for its payload property (e.g. `"numberOfEmployees": "lots"`).
- **contact**: `accountExternalId` is the contact's Account (by external id, written to `AccountId__c`); an event without it is dead-lettered
  (`INVALID_EVENT`), and one without `lastName` (required on Contact) with `REQUIRED_FIELD_MISSING`.

## The three typed layers

An event passes through three kinds of types on its way to the generic engine. Each knows one thing; all three of an
object live in its one file (`Sync/AccountSync.cs`, `Sync/AccountRelationshipSync.cs`, `Sync/ContactSync.cs`):

| Layer | Where | Knows |
|---|---|---|
| **Inbox payload** (`AccountPayload`, `ProfilePayload`, `AccountRelationshipPayload`, `ContactPayload`, in the `AccountEvent` / `AccountRelationshipEvent` / `ContactEvent` envelopes) | `Sync/<Object>Sync.cs` | The event as the source system sends it. PascalCase CLR properties, bound from camelCase JSON; no attributes, no Salesforce names. |
| **Salesforce sObject** (`AccountSObject`, `ProfileSObject`, `AccountRelationshipSObject`, `ContactSObject`) | `Sync/<Object>Sync.cs` | The org. One flat class per object; `[SObject]` names the object, its external id field and its record type (developer name), `[JsonPropertyName]` names each field, `[SObjectForeignKey]` marks a Text field holding a parent's external id (`[SObjectLookup]` a real Lookup field, which none of the examples has). |
| **Mapping** (`AccountSync.ToAccount`, `AccountSync.ToProfile`, `AccountRelationshipSync.ToRelationship`, `ContactSync.ToContact`) | `Sync/<Object>Sync.cs` | How one becomes the other: hand-written static methods, one line per field, converting only where the types differ (the `tags` list becomes a `;`-separated multi-select value). |

```
raw event JSON ─► AccountEvent { AccountPayload { …, ProfilePayload } }     System.Text.Json (Web defaults)
               ─► AccountSync.ToAccount / ToProfile ─► AccountSObject, ProfileSObject
               ─► SObjectRecord.From(referenceId, sobject) ─► SObjectRecord ─► engine (normalize, diff, write)
```

**Salesforce API names live only on the sObject classes.** Nothing else in the sync spells a field or object name:
the strategies' queries (`SoqlQuery.RecordsOf<AccountSObject>(units)`), the foreign keys and the records all read them
from the attributes. Pointing the sync at an org whose fields are named differently (`LLC_BI__Active__c` instead of
`Active__c`) means editing the attribute strings and nothing else.

`SObjectRecord.From` turns any sObject instance into the engine's record: object, external id field, record type and
field names from the attributes, every property as a field (nulls included, see above; a `[SObjectForeignKey]`
property too, also listed as a parent to send first and to wait for), every `[SObjectLookup]` property as a lookup by
the target's external id. The attributes are read by reflection once per class
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
`entity_id` = the id from the event, `event_time` = its `meta.time` and `payload` = the message value, unchanged.

**One inbox table per object type**, all the same shape (one `ConfigureInbox` call per inbox row type in
`SyncDbContext.OnModelCreating`; table names are code constants, checked to be plain identifiers before they are put
into SQL). An inbox row also holds its entity's sync state: when it is due, the lease of the worker processing it
and its watermark; no table is shared by the types. The tables the migrations create:

```sql
create table sf_account_inbox (                 -- and sf_account_relationship_inbox, sf_contact_inbox, ...
  id              bigserial   primary key,
  entity_id       text        not null,          -- the entity's id, read from the typed event by the consumer
  payload         jsonb       not null,          -- the Kafka message value, unchanged; never read by SQL
  status          text        not null default 'pending',  -- pending | done | superseded | dead
  attempt_count   int         not null default 0,          -- times this event was claimed (a newer event starts at 0)
  last_error      text,
  last_error_code text,
  last_updated    timestamptz not null default now(),
  event_time      timestamptz not null,                    -- the event's meta.time, read by the consumer
  next_attempt_at timestamptz,            -- null: nothing to do (not pending); else claimable from then on (backoff and lease included)
  lock_token      uuid,                   -- which claim holds the lease (until next_attempt_at)
  synced_time     bigint                  -- watermark: the newest event time written to Salesforce, microseconds
);
create unique index sf_account_inbox_entity_id on sf_account_inbox (entity_id);
create index sf_account_inbox_ready on sf_account_inbox (next_attempt_at) where next_attempt_at is not null;
```

The inboxes vacuum after 2% of their rows changed (`autovacuum_vacuum_scale_factor = 0.02`, threshold 1,000), not
the default 20%: every event, every claim and every outcome updates them.

**One row per entity.** `entity_id` is unique: an inbox row is an entity's newest event and what became of it, not
one event. Every event is the full record, so keeping only the newest loses nothing the sync needs (Kafka, within its
retention, is the history). A newer event overwrites the row, whatever its status, as a new event: pending, no
attempts, no error. An older one changes nothing. One as new (a Kafka redelivery) overwrites it too; its claim then
finds it not newer than the watermark and ends it `superseded`, without a Salesforce call. A rule on the payload
instead would wrongly drop the last event of A → B → A; by time, the last A is newer than B and is written.

Of the event, an inbox has only its entity and its time as columns, both read in C# by the consumer, and there is
**no SQL that names a JSON field** (except the one-time fill of `event_time` for rows stored before it existed, in the
`InboxEventTime` migration): the event's field names are yours to change in the payload classes. The sync's own
bookkeeping is in the row's own columns: `synced_time` is set, to the written event's `event_time` in
microseconds, when that event ends `done`, and a newer event overwriting the row keeps it.

`event_time` holds `meta.time` to the microsecond the way the engine counts it: .NET reads the time to 100 ns ticks
and the engine cuts them to whole microseconds to compare an event with the watermark, so the consumer stores that
cut time. Postgres keeps microseconds too, but it would round a finer time (`.1234567` to `.123457`), and the
watermark would then be 1 µs later than the event as the engine reads it: an event of that same microsecond, newer
by less than one, would be taken as not newer and superseded.

**Store** (`InboxSql.Store`, one statement): insert the event into the entity's row (`on conflict (entity_id) do
update ... where event_time <= excluded.event_time`), keeping the row's watermark and any lease on it. A new entity,
or an idle one (`next_attempt_at` null), becomes due after `Sync:CoalesceDelay`, and one that is already due,
backing off, deferred, held or leased keeps its time (a leased row's is the lease's end). The delay is what lets a burst coalesce: the events of an entity that
arrive within it overwrite its row one after the other, and only the last is claimed and written, so a longer delay
means fewer Salesforce calls, and each entity synced up to that much later. A row overwritten during a lease gets no
delay when the lease ends: its event arrived during the batch's Salesforce calls, so it has already waited, and more
of the burst that arrive before the next claim overwrite it anyway. The lock the store takes on the existing row is
what orders it against a claim or an outcome of the same entity.

| Setting (`Sync:`) | Default | Meaning |
|---|---|---|
| `CoalesceDelay` | `00:00:00.050` | How long an idle entity waits after a new event before it is due. Zero: due at once. Must be >= 0 and less than `Lease`, or startup fails |

The trade-off is fewer Salesforce calls against a sync up to `CoalesceDelay` later. The default is the smallest delay
that got most of the reduction in the streaming benchmark (about 11% fewer calls with 4 workers x 1 pod, the same as
100 ms; [measured](docs/benchmarks/README.md#coalescedelay-sweep)); raise it if your bursts are spread wider and
some latency is acceptable, or set 0 for no added delay.

**Claim** (`InboxSql.Claim`, the same statement for every type), in a transaction of its own, so the rows are read
before the commit is sent: lock up to `Sync:BatchSize` of the inbox's rows that are due (`next_attempt_at <= now()`),
longest-waiting first, off the ready index, `FOR UPDATE SKIP LOCKED`; lease them (`lock_token`, `next_attempt_at =
now() + Sync:Lease`), `attempt_count + 1`, and return them with their payloads, their `event_time` and their
watermarks. Only pending rows are due: every outcome that finishes a row sets its `next_attempt_at` null. A leased
row is not due until its lease ends, so "due" is the only condition: no separate lease check, and a lease that
expired (its worker died) is simply due again.

Two workers can't hold one entity: they would lock the same row. Under READ COMMITTED a worker that reads a row
before another worker's lease on it commits re-checks the row when it locks it, finds it no longer due, and drops it
(`ClaimRaceTests`). Storing an event locks the row too, so a claim skips an entity whose event is being stored, and a
second event of the entity waits until the first has committed, and then overwrites it.

**Plans.** Every lookup is by key: a primary key (`= any` of an array, never a join the planner could turn into a
scan), the unique `entity_id` index, or the front of the ready index. The claim on `sf_sync_state` before this design
was checked with `EXPLAIN ANALYZE` (generic plans) against statistics taken while everything was idle and then a
100,000-entity backlog, and against statistics taken during a backlog that had since drained: the same index plans
each time, so no planner settings are pinned. The statements on the inbox row have the same shapes (the claim's due
rows off the ready index, the outcomes by primary key, the claimed times joined to them as a semi join) and were
checked with generic plans on an analyzed 200,000-row inbox, not yet against stale statistics. Claim and outcome
costs measured for the earlier claims are in [docs/benchmarks/README.md](docs/benchmarks/README.md).

Then, per claimed entity, in C#: deserialize its event, supersede it if it is not newer than the entity's watermark,
otherwise map → write → advance the watermark (one statement for the batch marks the rows `done` and moves the
watermarks). An event that can't be read is dead-lettered on its own (`INVALID_EVENT`). All outcome statements are
set-based and conditional on the `lock_token` of the entity's row, and each ends the lease of the rows it records
by setting `next_attempt_at`: done, superseded and dead-lettered rows are idle (null), retried, released and deferred
ones due again when their backoff or deferral ends, held ones when the hold ends.

**Overwritten while claimed** (explained step by step in
[A newer event while an entity is being synced](#a-newer-event-while-an-entity-is-being-synced)). The lease keeps other
workers away from an entity, not the consumer: a newer event can overwrite the row while the worker writes the event it
claimed. So every outcome statement updates only the claimed rows that still hold the `event_time` the claim read
(`InboxSql.Template.StillClaimed`, part of each statement's WHERE: the claim's row ids and times, `unnest(@ids, @times)`, each
row looked up by primary key and compared timestamptz to timestamptz, exactly: the times are the microseconds read
back from the column) and records outcomes on those only: done, superseded, released, retried, dead, held. An
overwritten row stays pending with the newer event, done doesn't move the watermark to it, and the transaction's
last statement (`EndLease`, run only if the outcomes left a claimed row under the lease) ends its lease and makes
it due now (held or deferred: when the hold or deferral ends), not when the lease would have ended, so the next
claim writes it. The consumer and the outcome update the same row, so Postgres's row lock orders
them (see below).

```sql
-- Dead letters of one type, by cause
select entity_id, payload -> 'meta' ->> 'time' as time, last_error_code, last_error
from sf_account_inbox where status = 'dead' order by id desc;

-- What an inbox holds for one entity: its newest event
select id, status, attempt_count, last_error_code, payload
from sf_account_inbox where entity_id = 'ACC-000001';

-- Its state: due when, leased by whom, synced up to
select next_attempt_at, lock_token, synced_time from sf_account_inbox where entity_id = 'ACC-000001';
```

(Queries like these, for people, may look inside the payload; the worker's own SQL never does.)

### A newer event while an entity is being synced

`sf_account_inbox` has one row per account: for ACC-1 it always holds ACC-1's latest event. A newer event for ACC-1
replaces that row's payload and `event_time` and sets it back to `pending`.

Syncing ACC-1 takes three steps, and they are not instant:

1. **Claim.** A worker reads ACC-1's row and leases ACC-1: no other worker can sync ACC-1 meanwhile.
2. **Write to Salesforce.** The worker sends what it read: a read call, then a write call, tens to hundreds of
   milliseconds.
3. **Record the result.** The worker marks the row `done` (or retried, or `dead`) and moves ACC-1's watermark
   (`synced_time`) to the time of the event it wrote.

The lease stops other *workers*, not the *Kafka consumer*: new events keep arriving and are stored at once. So between
steps 1 and 3 this can happen:

```
10:00:00.000  row ACC-1 = { name "A", event_time 09:59, pending }
10:00:00.010  worker claims ACC-1, reads name "A"
10:00:00.020  worker sends name "A" to Salesforce ...
10:00:00.050  consumer stores a new event: row ACC-1 = { name "B", event_time 10:00, pending }
10:00:00.200  ... Salesforce confirms name "A" is saved
10:00:00.210  worker records the result for row ACC-1
```

At 10:00:00.210 the row no longer holds the event the worker wrote: it holds "B", which Salesforce has never seen. If
the worker simply marked the row `done`, the row would say `done` with "B" in it, so "B" would never be claimed; the
watermark would move to 10:00, so even a redelivery of "B" would count as synced; and Salesforce would keep "A" for
good. The newest update would be lost, with no error anywhere.

**The guard.** In step 3 the worker records its result only on a row that still has the `event_time` it claimed
(09:59), checked by the statement that records it:

- **yes**: nothing arrived during the write, so it records `done` and moves the watermark as usual;
- **no** (it is now 10:00): a newer event arrived, so it records **nothing** on the row. The row stays `pending` with
  "B", the watermark stays where it was, and when the lease ends ACC-1 is due again: the next claim writes "B".

Every result the worker can record (done, superseded, released, retried, dead, held) goes through the same check, since
each would otherwise overwrite the newer event's status.

**The check and the consumer can't interleave.** The check is part of the worker's own update of ACC-1's row
(`... where lock_token = @token and event_time = 09:59`), and the consumer updates the same row, so Postgres's row
lock puts one after the other. If the consumer's write committed first, the worker's update finds 10:00 and changes
nothing. If the consumer is still writing, the worker's update waits for it and then checks its condition again on
the row the consumer left (under READ COMMITTED an UPDATE that waited for a row re-checks its WHERE on the row's
newest version): 10:00 again. If the worker's update came first, the consumer waits until the worker has recorded its
result and then replaces the row with "B", `pending` and due. Either way "B" is synced next. Tests:
`OneRowPerEntityTests.An_outcome_for_a_claimed_event_is_not_recorded_on_the_newer_event_that_overwrote_it` and
`ClaimRaceTests.An_outcome_that_waits_for_a_store_of_a_newer_event_is_not_recorded_on_it` (every kind of result) and `LeaseTests.A_newer_event_stored_while_the_claimed_one_is_written_is_written_next_whatever_the_claimed_ones_outcome`
(end to end, Salesforce ends at "B").

### How a lease works

A lease is two columns of the entity's inbox row: `lock_token` (which claim owns the entity) and `next_attempt_at`
(until when), as a queue's visibility timeout: a leased row is simply not due until its lease ends. It is what keeps
an entity to one worker while that worker talks to Salesforce, without holding a database lock or a transaction all
that time.

1. **Take.** In the claim transaction, `Claim` locks due rows (`FOR UPDATE SKIP LOCKED`) and leases them: a new
   `lock_token`, `next_attempt_at = now() + Sync:Lease`. The transaction commits; the row locks go, the lease stays.
2. **While live.** Other workers skip the entity. This worker calls Salesforce outside any transaction and stops
   (cancels the call) at 80% of the lease, measured from the claim, so it gives up before anyone else can claim the
   entity.
3. **Release.** The outcome transaction records done, superseded, retry, dead, deferral or hold; every statement,
   done (which also moves the watermark) included, only touches rows `where lock_token = @token`, so a worker whose
   lease expired and was claimed by another changes nothing, and only rows that still hold the event the claim read
   (see [Overwritten while claimed](#the-inbox)). Each statement also ends the lease of its rows and sets
   `next_attempt_at`: null for a finished row, else when it is due again (now, or the end of the backoff, deferral or
   hold). Its last statement (`EndLease`) ends the lease of the rows left, those a newer event overwrote meanwhile
   (a store keeps a leased row's `next_attempt_at`, the lease's end), and makes them due now. An event stored
   at the same moment is never left behind: the store and the outcome update the same row, so either the store came
   first (the outcome skips the overwritten row and `EndLease` ends its lease) or it waits for the outcome
   transaction and then overwrites the finished row and makes the idle entity due (`ClaimRaceTests`).
4. **Crash.** Nothing to clean up. The lease expires, which makes the entity due, and the next claim takes it. Its read
   of Salesforce shows what the crashed worker did or didn't save. (A write whose outcome is unknown is held instead;
   see [Unknown outcomes](#unknown-outcomes).)

### Why the lease needs a lock token

The row's `event_time` tells an outcome whether the row still holds the **event** the claim read (a newer event
overwrites it). `lock_token` tells it whether this claim is still the **owner**. Both are needed, because the same
event can be claimed twice: a row is claimable again as soon as its lease ends, and nothing stops the old worker from
coming back after that.

**How the same event gets claimed twice** (rare, but it can't be ruled out):

- **A worker slower than its lease.** It is paused (a long garbage collection, a suspended or CPU-starved container or
  VM), it can't reach Postgres to record its outcome (a network blip or a failover longer than the rest of the lease),
  or a call takes longer than expected. Startup requires `Sync:Lease` to be longer than the HTTP request timeout, and a
  worker cancels its Salesforce call at 80% of the lease, but a pause or an outage doesn't respect either.
- **A claim retried after a lost commit.** The claim's commit succeeds, but the connection drops before the answer
  arrives, and the claim is retried. The first attempt's leases exist, but no running code owns them; the retry
  releases them by that attempt's `lock_token`.

In both cases the stale claim and the new one hold the same `event_time`, so only a value that is new on every claim
can tell them apart: that is the lock token. (`attempt_count` can't: a release, a hold and a deferral decrement it, so
an old claim's number can come back.)

**What would go wrong without it:**

1. **Older data over newer in Salesforce, silently.**

   ```
   A claims ACC-1 (event 09:59) and freezes; its lease ends
   B claims ACC-1 (event 09:59) and starts writing
   A wakes up and records "done": nothing says A no longer owns the row
     -> ACC-1 is finished and unleased while B is still writing
   a new event C arrives; worker D claims and writes it      -> Salesforce = C
   B's write of 09:59 lands                                   -> Salesforce = 09:59 (older)
   ```

   The inbox says C is `done` and the watermark is at C, so nothing corrects it until the record changes again at the
   source. No error is raised.
2. **A hold cancelled.** A gets no answer from Salesforce (its write may still land) and records a hold; B, which
   took over, records `done` on top and clears it. The next event is written at once, and A's lost write lands after
   it: the same overwrite as in 1. The hold exists to prevent exactly this.
3. **Wrong bookkeeping.** B succeeds, then A records `failed`: the row is synced again for nothing (extra calls), or,
   if that was A's last allowed attempt, a record B just synced is dead-lettered (a false alarm). Or A's hold lands on
   B's success and pauses the record for 10 minutes for no reason.
4. **A retried claim can't clean up.** The lost attempt's rows stay invisible until their lease ends.

**What it doesn't prevent:** the old and the new worker writing the same event in the moment between a lease ending
and the old worker noticing. That is harmless: an upsert by external id writes the same values twice. The token makes
sure only the current owner's outcome counts, so a worker that no longer owns a record can't change its state, its
hold or its watermark. Tests: `LeaseTests.Worker_whose_lease_expired_can_record_nothing_and_cannot_move_the_watermark`
(a lease expires mid-batch) and `ClaimRetryTests` (a claim retried after a lost commit).

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

The lease protects the entities between the two transactions. The trade-off: a crash after Salesforce saved but before
the outcomes committed leaves the events pending. That is safe, because every attempt reads Salesforce first and writes
only what differs, so the retry finds the records saved and ends `done` without writing them again. A write whose
outcome is unknown is held rather than retried at once.

### Retries and dead letters

A transient failure (`UNABLE_TO_LOCK_ROW`, `DEPENDENCY_NOT_SYNCED`, `INVALID_RECORD_TYPE`, a create answered with `DUPLICATE_VALUE` because
someone else created the record first, an unknown error code, a failed read) is retried
with exponential backoff of the entity: `Sync:RetryBaseDelay` (30 s) doubling per attempt (the row's
`attempt_count`) up to `Sync:MaxRetryDelay` (1 h); an event is dead-lettered once it was claimed `Sync:MaxRetries` (8)
times (`attempt_count`; a newer event of the entity starts again at 0). With the defaults the attempts come about 30 s, 1 min, 2 min,
4 min, 8 min, 16 min and 32 min apart: roughly an hour from the first failure to the dead letter. Permanent errors
are dead-lettered at once. The error code that caused it stays in `last_error_code`. Rate limits, records that were
not attempted and unknown outcomes don't spend an attempt.

**A relationship that arrives before its Accounts** waits this way: the read doesn't find the Account, the event
fails with `DEPENDENCY_NOT_SYNCED` before anything is sent, and it is retried on the schedule above until the
account sync has created the Account; no manual step is needed. Only an Account that never arrives within
`MaxRetries` ends as a dead letter, with `DEPENDENCY_NOT_SYNCED` kept as the cause.

**A contact that arrives before its Account** waits the same way: the read doesn't find the Account, and nothing is
sent for the contact until it does.

**The org rejecting a child whose parent is gone at write time** (the read found the Account, but it was deleted
before the write): the org's own automation rejects a record whose Text foreign key names no record (a validation
rule, a trigger's `addError` or a flow's Custom Error: `FIELD_CUSTOM_VALIDATION_EXCEPTION` with the org's message; an
unhandled trigger exception: `CANNOT_INSERT_UPDATE_ACTIVATE_ENTITY`). Salesforce has no code of its own for it, so the
message tells it apart: list it in `Salesforce:ParentMissingMessages` (case-insensitive substrings, empty by default),
and a record error whose message contains one is recorded as `DEPENDENCY_NOT_SYNCED` and retried the same way,
whatever its code. Until it is listed, that rejection is what its code says (`FIELD_CUSTOM_VALIDATION_EXCEPTION`:
permanent, dead-lettered). For a real Lookup field, Salesforce's own `INVALID_CROSS_REFERENCE_KEY` or
`ENTITY_IS_DELETED` on the lookup field is recognized as `DEPENDENCY_NOT_SYNCED` without configuration.

To re-queue dead letters once the cause is fixed (e.g. the missing Account now exists):

```sql
update sf_account_relationship_inbox set status = 'pending', attempt_count = 0, next_attempt_at = now()
where status = 'dead' and last_error_code = 'DEPENDENCY_NOT_SYNCED';
```

(`sf_account_inbox` and `sf_contact_inbox` the same way. Setting `next_attempt_at` is what makes the rows due again;
a row is claimed only while it has one, so a row changed by hand is pending exactly when it has a `next_attempt_at`.)

A re-queued event still obeys the watermark: if an event as new has been synced since, it is superseded. A newer
event of a dead-lettered entity needs no re-queue: it overwrites the dead row, which is pending again (and the dead
letter's error is gone with the old event).

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
- **What happens**: every entity of that batch is held: its row gets `next_attempt_at = now() +
  Sync:UnknownOutcomeHold` and loses its lease (no `lock_token`: the old worker can't record anything on it); it stays
  `pending` with `last_error_code = 'UNKNOWN_OUTCOME'`, and the attempt doesn't count. The watermark doesn't move. A
  row a newer event overwrote during the write is held the same way. The claim takes a row only once its
  `next_attempt_at` has passed, and storing an event keeps a row's due time, so an event arriving meanwhile (which
  overwrites the row, error included) waits too. After the hold, normal processing (read, diff,
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
| Two workers or pods never hold the same entity at once | the entity's one inbox row: locked `FOR UPDATE SKIP LOCKED` and leased, its lease re-checked when it is locked | `ConcurrencyTests.Parallel_pods_never_process_the_same_account_concurrently_and_end_at_the_newest_version` (3 pods × 8 loops, claim audit: 0 overlaps); `ClaimRaceTests` (claims while another claim or a store of the entity is open, events stored concurrently) |
| An event stored while its entity's lease ends is not left behind | storing and ending a lease update the same row, so its lock orders them; a store keeps a due row's time and makes an idle row due, and the lease of an overwritten row ends with it due now (`EndLease`), not at the lease's end | `ClaimRaceTests.An_event_stored_while_the_outcome_transaction_has_the_row_overwrites_it_and_leaves_it_due` |
| An event arriving while its entity is being processed is not claimed until the lease is released or expires, then wins if newer | a leased row is not due until its lease ends (its `next_attempt_at`) | `LeaseTests.Event_arriving_while_its_entity_is_being_processed_waits_for_the_lease_then_wins_if_newer` |
| Events out of `meta.time` order, over any partition and consumer, end at the newest; an older record is never written after a newer one | a row is overwritten only by an event as new or newer, and the watermark | `ConcurrencyTests.Events_out_of_time_order_over_any_partition_and_pod_end_at_the_newest_time`, `SyncEndToEndTests.The_Kafka_key_is_not_used_and_arrival_order_does_not_matter` |
| An event stored while its entity's older event is being written is not lost: the older one's outcome isn't recorded on it, the watermark doesn't move to it, and it is written next | every outcome statement updates only claimed rows whose `event_time` is still the one the claim read (`InboxSql.Template.StillClaimed`, in its WHERE); storing updates the same row, so the row lock orders the two, and an outcome that waited for a store re-checks its WHERE on the row the store left (READ COMMITTED); `EndLease` ends the overwritten row's lease and leaves it due | `OneRowPerEntityTests.An_outcome_for_a_claimed_event_is_not_recorded_on_the_newer_event_that_overwrote_it` and `ClaimRaceTests.An_outcome_that_waits_for_a_store_of_a_newer_event_is_not_recorded_on_it` (done, failed, dead, superseded, released, failed all, deferred, held), `LeaseTests.A_newer_event_stored_while_the_claimed_one_is_written_is_written_next_whatever_the_claimed_ones_outcome`, `ClaimRaceTests.An_event_stored_while_the_outcome_transaction_has_the_row_overwrites_it_and_leaves_it_due` |
| A worker whose lease expired can't record outcomes or move the watermark | every statement is conditional on the `lock_token` of the entity's row | `LeaseTests.Worker_whose_lease_expired_can_record_nothing_and_cannot_move_the_watermark`, `LeaseTests.Done_completes_an_event_and_sets_the_watermark_to_its_time_only_under_its_lease_and_only_once` |
| Dropped Postgres connections cause no overlap, loss or regression | `PostgresRetryStrategy` (EF execution strategy), idempotent statements | `ConcurrencyTests.Pods_survive_their_Postgres_connections_being_dropped` |
| A write with an unknown outcome can't be overtaken by a newer write | the hold | `UnknownOutcomeTests.Entity_is_held_after_a_write_with_an_unknown_outcome_and_ends_at_the_newest_event` |

Windows in which a guarantee does not hold, stated plainly: (1) a worker whose **Postgres** work to record a hold
fails (database unreachable) or whose process is killed mid-write leaves only the lease; if Salesforce then takes
longer than the rest of the lease to apply the request, a newer write can be overtaken. (2) A lease that expires
while its worker is still running (clock stall, a pause longer than the 20% margin) lets a second worker claim the
entity; the first can no longer record anything, but a request it already sent is in Salesforce's hands. (3)
Ordering is only as good as `meta.time`: two different events of an entity with the same time are not ordered by
content (the one stored later overwrites the other; if it does so while the other is being written, the check by
time can't tell them apart, the other's outcome is recorded on it and it isn't written until a newer event comes),
and a source clock that goes backwards makes a newer change look older (it changes nothing, or is superseded). (4)
The inbox holds each entity's newest event only, not a log: an event overwritten before it was claimed, or older
than the row when it arrived, leaves no trace in Postgres; Kafka, within its retention, has it.

### Schema and migrations

`SyncDbContext.OnModelCreating` defines every table: the inboxes (one `ConfigureInbox` call per inbox row type),
`sf_rate_limit` (with its one `salesforce` row as seed data) and `sf_api_usage`, with the column names in snake_case,
the partial indexes (`HasFilter`) and the autovacuum settings (`HasStorageParameter`). EF migrations in
`src/SalesforceSync/Data/Migrations` create them; the `sf_limit_status` view, which has no model counterpart, is
created by `migrationBuilder.Sql` in `InitialSchema` (and recreated without `own_calls_24h` by
`LimitStatusWithoutOwnCalls`). Queries stay raw SQL (`InboxSql`, `RateLimitSql`) where LINQ can't express them.

The migrations, in order:

| Migration | Does |
|---|---|
| `InitialSchema` | The inboxes (one row per event), `sf_sync_state` (every entity's due time, lease and watermark), `sf_rate_limit`, `sf_api_usage` and the `sf_limit_status` view |
| `LimitStatusWithoutOwnCalls` | Recreates `sf_limit_status` without `own_calls_24h` |
| `InboxEventTime` | Adds `event_time` to every inbox, filled from each row's `meta.time` |
| `InboxOneRowPerRecord` | Keeps each entity's newest row and makes `entity_id` unique |
| `StateInInboxRow` | Moves every entity's state from `sf_sync_state` into its inbox row (`next_attempt_at` only for a pending row; a state row without an inbox row, which only a row deleted by hand leaves, is dropped), adds each inbox's ready index and drops `sf_sync_state` |
| `DropReceivedAt` | Drops `received_at` from the inboxes: the sync orders events by `event_time`, and `last_updated` shows when a row last changed |
| `LeaseInNextAttemptAt` | Drops `locked_until` from the inboxes: a lease ends at the row's `next_attempt_at` (a lease in progress keeps its end there; an expired one leaves the row due) |

At startup `SyncSchemaInitializer` (registered before the workers) runs `SyncSchema.MigrateAsync`: under a
session-level advisory lock (`0x5346_5348`), so pods that start together migrate one at a time and each finds the work
of the one before done, it applies the pending migrations with `Database.MigrateAsync()`. (EF 9 also locks its history
table while it migrates; the advisory lock covers the check for a database from before migrations, below, as well.)
Changing the model:

```bash
dotnet tool restore
dotnet ef migrations add <Name> --project src/SalesforceSync --output-dir Data/Migrations
```

A unit test (`Every_change_to_the_model_has_its_migration`, `HasPendingModelChanges`) fails while a model change
has no migration.

**Databases from before migrations.** A database whose tables were created by `sql/schema.sql` (the version before
migrations) has no `__EFMigrationsHistory`. If its `sf_sync_state` has the `lock_token` column (the last shape that
script made, identical to what `InitialSchema` creates), `InitialSchema` is recorded as applied instead of run, and
later migrations apply as usual. Older shapes (an `sf_inbox` table, inboxes that held their own claim state with
`retry_count`, an `sf_sync_state` with `entity_key`) are no longer upgraded: startup stops with an error that says so.
Migrate such a database by hand to the shape `InitialSchema` creates and record that migration in
`__EFMigrationsHistory`, or start from a fresh database.

## Writing to Salesforce

A batch's writes are **sObject Collections upserts, one per level and object**
(`PATCH /composite/sobjects/{SObject}/{ExternalIdField}`), grouped into write requests (normally one), each **one
call**: the upsert on its own when the request has one (writes of one object), else **one Composite request** of the
upserts, parents first ([below](#one-request-per-batch)).

### Foreign keys: Text fields holding the parent's external id

In this org every foreign key is a **Text field holding the parent's external id** (the source system's key), not a
Lookup field holding a Salesforce Id:

| Object | Foreign key (Text) | Holds |
|---|---|---|
| `Profile__c` | `AccountId__c` | the Account's `ExternalId__c` (the account key) |
| `Account_Relationship__c` | `AccountId__c`, `RelatedAccountId__c` | the two Accounts' `ExternalId__c` |
| `Contact` | `AccountId__c` | the Account's `ExternalId__c` (the standard `AccountId` lookup is left alone) |

The sObject class marks such a field `[SObjectForeignKey(typeof(AccountSObject))]`:

```csharp
[JsonPropertyName("AccountId__c"), SObjectForeignKey(typeof(AccountSObject))]
public string? AccountId { get; set; }
```

For normalization, the read, the diff and the write it is an ordinary string field: compared with the event's value,
written as the text when it differs (an unchanged key is not sent), never resolved to an Id, never a `@{reference}`.
The attribute is used for two things only:

- **Order.** The org's automation rejects a child whose parent doesn't exist yet, so a record is sent after the
  records of its entity it points at: a record's level is one below its parents' (by foreign keys and by lookups),
  and the upserts go level by level (Composite runs its subrequests in order). A new Account with its new Profile is
  one Composite request: the Accounts' upsert, then the Profiles' upsert carrying `AccountId__c` = the account key.
- **Parent check, when the parent comes from another inbox** (a relationship's two Accounts, a contact's Account).
  The strategy's queries read the parent's object by external id
  (`SELECT Id, ExternalId__c FROM Account WHERE ExternalId__c IN (...)`, `SoqlQuery.LookupTargetsOf<AccountSObject>`)
  only to know it exists. A parent that is neither there nor in the same entity makes the entity wait
  (`DEPENDENCY_NOT_SYNCED`) with nothing sent. When the parent is written by the same entity (a Profile's Account),
  no query is needed: the parent's upsert goes first.

**A child whose new parent failed is still sent, and the org decides.** Parent and child are subrequests of the same
Composite request, so the child can't be held back once its parent's answer is known; the engine doesn't try to.
The org's automation rejects a child whose parent is missing (that rejection, in whatever words
`Salesforce:ParentMissingMessages` lists, means `DEPENDENCY_NOT_SYNCED`); if it accepts the child, the child is saved.
Either way the entity's outcome is its parent's error (a validation error dead-letters it, a row lock retries it), not
the child's. A parent that already exists is no reason to hold its child back.

**Reads per example batch:** accounts: `Account` and `Profile__c` by external id (the Profiles' Accounts are in the
batch); relationships: `Account_Relationship__c` and `Account` (the parents' existence); contacts: `Contact` and
`Account` (the same).

**Query size.** Each query is a `GET .../query?q=<SOQL>` subrequest of the read's Composite request, so its URL is
bounded: Salesforce allows 16,384 bytes for a call's URI and headers together (414 / 431 over it). The client keeps
4,096 of them for the headers (`SalesforceRestClient.HeaderAllowanceBytes`: the bearer token, Host, User-Agent and the
rest) and holds every URL it sends to the remaining 12,288 bytes (`MaxUrlBytes`), counted as sent, after
percent-encoding (`'`, `,` and spaces take 3 bytes, a non-ASCII character up to 12). `ReadQueryBuilder` fills a
query with up to the row type's `MaxValuesPerQuery` external ids (100 for the built-in objects; at most 2,000, a
query's result page, `SalesforceRestClient.MaxQueryValues`) while its URL still fits, then starts the next; the read sends 5 queries per
Composite request (one call each) and merges the results by reference id. Short ids give exactly the 1 read call
above; 100 ids of 250 characters take 3 queries per object, so a batch of Accounts and Profiles reads in 2 calls. A
query that doesn't fit even with one id (an absurd field list) fails the batch with a message naming the object and
field.

### One request per batch

Every batch is read and diffed first; only then is the API chosen. Every write is an **sObject Collections upsert**
by external id with exactly the planned fields, one upsert per level and object (≤ the row type's `MaxRecordsPerWrite`, 180 for the built-in objects, under
Salesforce's 200: children that link a new parent by `@{reference}` get an upsert of their own; more records of one
object take further upserts). The upserts are grouped into write requests of at most 5
(Composite's limit on sObject Collections subrequests; an entity's records always share a request), each **one call**:

- **The writes are of one object** (only Accounts changed, or only relationships, or only contacts): the request is
  that upsert, a plain `PATCH /composite/sobjects/{SObject}/{ExternalIdField}`.
- **The writes are of two or more objects** (Accounts and their Profiles, or a new parent and its new child by a
  Lookup field): the request is **one Composite request** (`POST /composite`, `allOrNone: false`) with an upsert
  subrequest per object and level, parents first (by `[SObjectForeignKey]` and `[SObjectLookup]` levels).

The rule lives in the engine (`CompositeWrite`, `SalesforceRestClient.UpsertAsync`) and knows no object by name: a new
strategy writing, say, Accounts and their Addresses gets it with no code of its own; its classes only declare their
foreign keys or lookups, which give the order.

| Write request | Write calls |
|---|---|
| new Accounts with their new Profiles | 1 Composite: Accounts' upsert, then Profiles' (`AccountId__c` = the account key) |
| Account-only changes, ≤180 | 1 upsert |
| Account-only changes + Profile-only changes | 1 Composite: Accounts, then Profiles |
| Accounts and Profiles both changed, of existing accounts | 1 Composite: Accounts, then Profiles |
| new relationships or contacts whose Accounts exist | 1 upsert, the Accounts' keys as text |
| 300 changed Accounts | 2 upserts (two requests): 180, then 120 |
| a new parent with a new child by a Lookup field (not in the examples) | 1 Composite, the child pointing at `@{Account[i].id}` |

An upsert, alone or as a subrequest, looks like this:

```jsonc
// PATCH /services/data/v62.0/composite/sobjects/Account/ExternalId__c
{ "allOrNone": false,
  "records": [
    { "attributes": { "type": "Account" }, "ExternalId__c": "ACC-1", "Rating": "Hot" },              // exists: what differs
    { "attributes": { "type": "Account" }, "ExternalId__c": "ACC-2", "Name": "Globex", "RecordTypeId": "012…" } ] }  // new: whole
// 200 OK
[ { "id": "001…", "success": true, "errors": [], "created": false },
  { "id": "001…", "success": true, "errors": [], "created": true } ]

```

With a Profile written too, the same upserts are the subrequests of one Composite request:

```jsonc
// POST /services/data/v62.0/composite
{ "allOrNone": false,
  "compositeRequest": [
    { "method": "PATCH", "url": "/services/data/v62.0/composite/sobjects/Account/ExternalId__c", "referenceId": "Account",
      "body": { "allOrNone": false, "records": [ /* as above */ ] } },
    { "method": "PATCH", "url": "/services/data/v62.0/composite/sobjects/Profile__c/ExternalId__c", "referenceId": "Profile__c",
      "body": { "allOrNone": false, "records": [
        { "attributes": { "type": "Profile__c" }, "ExternalId__c": "ACC-2", "AccountId__c": "ACC-2", "Tier__c": "Gold", "RecordTypeId": "012…" } ] } } ] }
// 200 OK: { "compositeResponse": [ { "referenceId": "Account", "httpStatusCode": 200, "body": [ /* per record */ ] }, … ] }
```

Each upsert carries exactly what the diff planned: a new record whole, an existing one only its changed fields (an
upsert accepts a different set of fields per record), each identified by its external id rather than its Id; a
foreign key as the parent's external id (a Lookup field as the Id the read found). Unchanged entities still write
nothing. A batch of new or changed Accounts and Profiles costs 2 calls (the read and one Composite write), each counted
by the rate limiter like any other.

The per-record answers go through one classification (`WriteResultInterpreter`), all upserts of a request together,
whether they came as a call of their own or as subrequests: an entity with an Account in one upsert and a Profile in
another is saved only if both are, and a Profile rejected in its upsert fails only its own entity, never the Accounts
of the other one. A Composite subrequest that fails as a whole (an error instead of per-record results) fails only its
records, retried and never dead-lettered; the other subrequests' records keep their own results.
Two answers are particular to an update sent as an upsert, both meaning the record was deleted after the read: the
upsert either created it from the changed fields alone (`"created": true`) or failed creating it
(`REQUIRED_FIELD_MISSING`). Both are retried, not dead-lettered: the retry's read sees what Salesforce now has and
writes the rest.

When a write call fails as a whole (the call, not records or subrequests in it), the batch stops, later requests (a
batch of more than 5 upserts) are not sent, and the failure applies to every event of the batch: retried with
backoff, held if the call's outcome is unknown (the Account and Profile Composite request's answer lost: the whole
batch is held), deferred if it was rate limited. What earlier requests answered is not recorded. That is safe even
though they may have saved their records: every write is matched by external id, so the retry reads Salesforce again
and writes only what still differs, and those entities end `done` without being written twice.

**`Sync:BatchSize`** is 1 to 100 entities (default 75). The bound comes from the read, not the write: the built-in row types query
100 external ids at a time (`MaxValuesPerQuery`) and a Composite request takes 5 queries, and a relationship needs up to three ids per entity (itself and
two Accounts).

### Record types

Every synced object must have a record type, and each sObject class names it by **developer name**:

```csharp
[SObject("Account", ExternalIdField = "ExternalId__c", RecordType = "Business")]
[SObject("Profile__c", ExternalIdField = "ExternalId__c", RecordType = "Standard")]
[SObject("Account_Relationship__c", ExternalIdField = "ExternalId__c", RecordType = "Standard")]
```

These are defaults; they must match record types in the org (see
[step 4](#4-create-the-objects-and-fields)). The engine turns the name into an Id with the RecordType data Salesforce
returns in the object's Describe (`recordTypeInfos`: `developerName`, `recordTypeId`, `active`, `available` for the
running user). It is the same Describe the engine already calls for the fields, so the record type Ids are cached with
it (`SalesforceSchemaCache`: same refresh interval, `If-Modified-Since`, one fetch in flight per object): no extra call
and no second cache.

`RecordTypeId` is then an ordinary field of the record's desired state: set on create, read back by the batch's read,
compared after normalization (15 → 18-character id), and written on update only when Salesforce has another one
(for example after a user changed it). A parent needs no record type to be found: it is found by its external id only.

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

(Every write is [a Collections upsert](#one-request-per-batch): "create" and "update" are what the upsert does by
external id, with the planned fields.)

A redelivered insert therefore writes nothing and creates no duplicate, and an `updated` event for a record that
isn't in Salesforce (its insert was lost or dead-lettered, or the record was deleted there) creates it. An
`updated` event stored before its `inserted` one was claimed overwrites it, so they become one write of the newest
data. A create that Salesforce answers with `DUPLICATE_VALUE` (someone created the record between the read and the
write) is retried, and the retry's read turns it into an update.

### Why compare with Salesforce before writing

Salesforce's default integration pattern is a blind **upsert by external id**: one call per batch, no read, no
comparison, idempotent. This sync reads first and writes only what differs. That is a common, legitimate pattern,
and it pays off when unnecessary writes cost something:

- **Salesforce doesn't skip no-op updates.** An update with identical values still runs triggers, flows and
  validation rules, bumps `LastModifiedDate`, can publish Change Data Capture events, and locks the record (and its
  parent Account, a source of `UNABLE_TO_LOCK_ROW` under concurrency). Comparing first means an unchanged record
  isn't written at all, and a changed one sends only its changed fields.
- **Events repeat data.** Every event carries the full record ([An event is the full record](#an-event-is-the-full-record)),
  so many are no-ops by the time they are synced; those cost no write request and trigger nothing.
- **Drift is repaired.** The comparison is against what Salesforce has now, so a synced field edited in Salesforce
  is set back by the next event for that record.

What it costs:

- **One more API call per batch**: one Composite request reads before the writes.
- **Exact value handling.** `FieldValues` normalizes each value to what Salesforce stores for its field type (scale,
  checkbox, 15/18-character ids, multi-picklist order, date/time precision, from Describe), or the diff would see a
  change on every event. It is the logic most likely to be subtly wrong; the sandbox checks under
  [Tune and check](#6-tune-and-check) cover it.
- **A small race**: an edit made in Salesforce between the read and the write is overwritten, but only on fields
  this sync owns, which the write would overwrite anyway.
- **Read access** for the integration user to every synced field.

The cheaper alternative is to keep a hash of what was last sent per entity in Postgres, skip entities whose event
hashes the same, and upsert the rest blindly. That removes the read, the diff and `FieldValues`, but repairs no
drift, and an upsert sends the whole record, so a changed record still fires automation on every mapped field.

Keep the comparison when the org has meaningful automation on the synced objects (triggers, flows, field history,
Change Data Capture) or edits made in Salesforce must be corrected, which is typical for Account. Choose hash +
blind upsert when the org is plain and API calls are the scarce resource. The org's admin knows which applies.

### `allOrNone: false`

`allOrNone: false` is sent explicitly on the Composite request and on every upsert, alone or in it: every record is saved
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
| any record with another error (row lock, `DUPLICATE_VALUE` on create, parent missing, unknown code) | retried with backoff |
| a child not saved only because its new parent in the same entity wasn't | whatever the parent's error says; the child's own "parent missing" error is ignored |
| an update [sent as an upsert](#one-request-per-batch) that created the record, or failed creating it with `REQUIRED_FIELD_MISSING` (deleted after the read) | retried with backoff; the retry reads it again |
| its whole subrequest went down because of another entity's record (`PROCESSING_HALTED` on a reference without a value) | released, no retry spent |
| a subrequest that failed as a whole for any other reason | retried with backoff, never dead-lettered (it says nothing about one record's data) |
| the call itself failed (any write request of the batch) | every entity of the batch: rate-limited → deferred; provably not applied → retried; otherwise [unknown outcome](#unknown-outcomes) → held |

### Lookup fields: Composite and `referenceId`

The engine still supports real **Lookup** fields (a reference field holding a Salesforce Id), for orgs that have
them: `[SObjectLookup(typeof(AccountSObject))]` on a string property holding the parent's external id. None of the
example objects uses it (the unit tests cover it with test-only classes). A lookup is resolved, not written as text:

- a parent that **already exists** gets its 18-character Id from the read (`"AccountId": "001…"`), and a parent that
  exists and has nothing to change is not written at all;
- a parent **created in the same request** is linked by a Composite reference to its upsert's result,
  `"AccountId": "@{Account[3].id}"` (the parent's position in its upsert's response). Parent and child are two
  objects, so the request is the same **Composite request** of upserts as any other of several objects:

```jsonc
{ "allOrNone": false,
  "compositeRequest": [
    { "method": "PATCH", "url": "/services/data/v62.0/composite/sobjects/Account/ExternalId__c", "referenceId": "Account",
      "body": { "allOrNone": false, "records": [
        { "attributes": { "type": "Account" }, "ExternalId__c": "ACC-1", "Name": "Acme" },      // new: whole
        { "attributes": { "type": "Account" }, "ExternalId__c": "ACC-2", "Rating": "Hot" } ] } }, // exists: what differs
    { "method": "PATCH", "url": "/services/data/v62.0/composite/sobjects/Contact/ExternalId__c", "referenceId": "Contact_ref",
      "body": { "allOrNone": false, "records": [
        { "attributes": { "type": "Contact" }, "ExternalId__c": "CON-1", "AccountId": "@{Account[0].id}" } ] } }
  ] }
```

One upsert per level and object, parents before children. Salesforce's limits are constants in
`SalesforceRestClient`: 200 records per collection (`MaxCollectionRecords`; a row type writes at most its
`MaxRecordsPerWrite` of them), 25 subrequests per Composite request of which **5** may be collections or queries. Units that need more are split over further write requests, an entity's records always in the
same one. Children that link by reference get an upsert of their own (`Contact_ref`). If a parent fails, its
reference has no value, which fails the **whole subrequest** of children that use references (`PROCESSING_HALTED`).
The other entities' parents are in, so their children go back to the inbox without spending a retry and are written
by the next claim, with a literal Id. Children of existing parents are in a separate subrequest and are not affected.
A lookup whose target is neither in Salesforce nor in the same entity fails the entity with `DEPENDENCY_NOT_SYNCED`
before anything is sent.

> **Verify in a sandbox (only with Lookup fields).** Salesforce documents `@{referenceId.field}` references between
> Composite subrequests, with `[]` indexing into arrays, and that sObject Collections can be Composite subrequests.
> That a reference may index into a collection's *response array* (`@{Account[0].id}`) is not spelled out, and
> neither is what Composite answers for a reference without a value. The mock org implements both the way this client
> expects. If a real org rejects such a subrequest, nothing is lost: a subrequest-level failure is retried, never
> dead-lettered, and the retry links the child by the Id it now reads; but every new parent would then take two rounds.

## Adding a Salesforce object

The engine (`SalesforceBatchUpsertService.UpsertBatchAsync<TEvent, TRow>`) owns claiming, ordering, coalescing, rate
limits, batching, normalization, diffing, parents first, retries and dead-lettering. Nothing in the engine, the consumer or
the worker loops names an object. An object supplies, in one file: a payload class, an sObject class, an inbox row
class (which selects the object's inbox table) and a static class with the mapping and the strategy, which says how an
event becomes records. `AddInboxSync` registers it as an `EntitySyncService<TEvent, TRow>` with its topic, which does
the rest of `ISalesforceEntitySync` through the shared `IRepository<TRow>`.

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
[SObject("Account_Relationship__c", ExternalIdField = "ExternalId__c", RecordType = "Standard")]
public sealed class AccountRelationshipSObject
{
    [JsonPropertyName("ExternalId__c")] public string? ExternalId { get; set; }

    // A foreign key: a Text field holding the parent's external id; the parent's class knows its object and id field
    [JsonPropertyName("AccountId__c"), SObjectForeignKey(typeof(AccountSObject))]
    public string? AccountId { get; set; }

    [JsonPropertyName("RelatedAccountId__c"), SObjectForeignKey(typeof(AccountSObject))]
    public string? RelatedAccountId { get; set; }

    [JsonPropertyName("Type__c")] public string? Type { get; set; }
    [JsonPropertyName("Active__c")] public bool? Active { get; set; }
}

// 3. Inbox row: the object's inbox table. Every column is on InboxRow, so every object's inbox has the same fields;
//    the row type gives its batch's query and write sizes (IInboxRow; checked at registration: 1 to 2,000 ids per
//    query, 1 to 200 records per upsert)
public sealed class AccountRelationshipInboxRow : InboxRow, IInboxRow
{
    // A full batch's read in one Composite request: 5 queries, up to 3 ids per relationship (itself, two Accounts)
    public static int MaxValuesPerQuery => 100;
    // A margin under Salesforce's 200
    public static int MaxRecordsPerWrite => 180;
}
//    ... and in SyncDbContext: a DbSet<AccountRelationshipInboxRow>, and in OnModelCreating
//    modelBuilder.Entity<AccountRelationshipInboxRow>(entity => ConfigureInbox(entity, "sf_account_relationship_inbox"));
//    Then a migration creates the table: dotnet ef migrations add <Name> --project src/SalesforceSync --output-dir Data/Migrations

// 4. Mapping and strategy: how to read an event (id, time, type), event -> records, and what to read
public static class AccountRelationshipSync
{
    public const string Type = "accountRelationship";
    public const string DefaultTopic = "account-relationship-changes";  // Kafka:Topics:accountRelationship overrides

    public static readonly UpsertStrategy<AccountRelationshipEvent> Strategy = new(Type,
        EntityId: e => e.AccountRelationship?.ExternalId,   // the only places that know where the id
        Meta: e => e.Meta,                                  // and the meta (time and type) are in the event
        Map: (_, e) => Map(e),
        Queries: Queries);

    // Throwing dead-letters the event (INVALID_EVENT)
    public static UpsertUnit Map(AccountRelationshipEvent e)
    {
        var payload = e.AccountRelationship;
        if (string.IsNullOrWhiteSpace(payload?.AccountExternalId) || string.IsNullOrWhiteSpace(payload.RelatedAccountExternalId))
            throw new ArgumentException("accountExternalId and relatedAccountExternalId are required");

        return new UpsertUnit([SObjectRecord.From("rel", ToRelationship(payload))]);
    }

    // The relationships themselves (to diff) and the Accounts they point at (to know they exist)
    public static IEnumerable<SoqlQuery> Queries(IReadOnlyCollection<UpsertUnit> units) =>
    [
        SoqlQuery.RecordsOf<AccountRelationshipSObject>(units),
        SoqlQuery.LookupTargetsOf<AccountSObject>(units)
    ];

    // The mapping: explicit, one line per field
    public static AccountRelationshipSObject ToRelationship(AccountRelationshipPayload payload) => new()
    {
        ExternalId = payload.ExternalId,
        AccountId = payload.AccountExternalId,
        RelatedAccountId = payload.RelatedAccountExternalId,
        Type = payload.Type,
        Active = payload.IsActive
    };
}
```

Register it in `AddInboxSync` (the open generic `InboxRepository<TRow>` is registered once and serves every row type):

```csharp
AddEntitySync<AccountRelationshipEvent, AccountRelationshipInboxRow>(services,
    AccountRelationshipSync.Strategy, AccountRelationshipSync.DefaultTopic);
```

Once its migration exists, the object's inbox table is created at startup, the consumer subscribes to its topic and stores its
events there, and the worker loops sync it. A record whose parent isn't in Salesforce yet (`DEPENDENCY_NOT_SYNCED`) is retried with backoff, so
related objects can arrive in any order. A strategy may build `SObjectRecord`s by hand; the typed layers are how the built-in objects do it, not something the
engine requires.

**Contact** has the same shape. `Sync/ContactSync.cs` is the complete example: `ContactEvent` / `ContactPayload` (the
`contact` object of the event), `ContactSObject`, `ContactInboxRow` (`sf_contact_inbox`), and the strategy, which reads
the contacts and the Accounts they point at, like relationships:

```csharp
// The Account by its external id, in a Text field; the read checks the Account exists
[SObject("Contact", ExternalIdField = "ExternalId__c", RecordType = "Standard")]
public sealed class ContactSObject
{
    [JsonPropertyName("ExternalId__c")] public string? ExternalId { get; set; }

    [JsonPropertyName("AccountId__c"), SObjectForeignKey(typeof(AccountSObject))]
    public string? AccountId { get; set; }

    [JsonPropertyName("LastName")] public string? LastName { get; set; }
    // ... FirstName, Email, Phone
}

public static class ContactSync
{
    public const string Type = "contact";
    public const string DefaultTopic = "contact-changes";

    public static readonly UpsertStrategy<ContactEvent> Strategy =
        new(Type, e => e.Contact?.ExternalId, e => e.Meta, (_, e) => Map(e), Queries);

    // The contacts themselves (to diff) and the Accounts they point at (to know they exist)
    public static IEnumerable<SoqlQuery> Queries(IReadOnlyCollection<UpsertUnit> units) =>
    [
        SoqlQuery.RecordsOf<ContactSObject>(units),
        SoqlQuery.LookupTargetsOf<AccountSObject>(units)
    ];
    // Map and ToContact as for relationships
}
```

registered the same way:

```csharp
AddEntitySync<ContactEvent, ContactInboxRow>(services, ContactSync.Strategy, ContactSync.DefaultTopic);
```

Contact is a standard object, so it has no record type of its own out of the box; like the others, the sync expects
one named `Standard` (see [step 4](#4-create-the-objects-and-fields)), or change `RecordType` on `ContactSObject`.

**Why `SemaphoreSlim` isn't used for parallel batches:** it limits concurrency within one process, while
Salesforce's limits are org-wide. Parallelism comes from competing worker loops in every pod pulling from the
shared inbox, capped globally by Postgres advisory-lock slots and a shared token bucket. (The Describe cache
does use a `SemaphoreSlim`, as an in-process single-flight so concurrent workers don't all describe at once.)

## Repository layout

```
src/
  SalesforceSync/                 Worker:
    Inbox/                        Kafka consumer, worker loops, InboxSql, InboxRow, IRepository (and the
                                  claim and outcome types), InboxRepository
    Sync/                         The engine (SalesforceBatchUpsertService), PreparedBatch, EntitySyncService, the
                                  strategy and event meta, the engine's model types, and one file per synced object
                                  (AccountSync, AccountRelationshipSync, ContactSync: payload, sObjects, mapping, strategy,
                                  inbox row)
    Sync/Upsert/                  The write path's rules, no I/O: normalize, read queries, diff, plan (parents first),
                                  pack (one upsert per level and object; 2+ in a request → Composite), interpret
    Salesforce/                   The REST client and its request/result types, Describe cache, FieldValues, errors,
                                  SObjectMetadata and the attributes the sObject classes are read by
    RateLimiting/                 Token bucket, daily budget, pause, concurrency slots, the batch gate
    Data/                         SyncDbContext (the EF model: every table), SyncSchema (migrates at startup),
                                  PostgresRetryStrategy, raw-SQL helpers
    Data/Migrations/              EF migrations (InitialSchema: the tables, and the sf_limit_status view)
  SalesforceSync.FakeSalesforce/  In-memory Salesforce (describe, query, composite, sObject Collections upsert, Salesforce-like
                                  value storage, Text foreign keys checked like the org's automation, error injection,
                                  write history, Sforce-Limit-Info): FakeSalesforceStore
                                  is the org's surface and lock; FakeRecords, FakeSoql, FakeCollectionWriter,
                                  CompositeReferences, FakeFaults and FakeCounters are its parts
  SalesforceSync.FakeSalesforce.Server/  The in-memory org as an HTTP service (+ /__fake inspection API); the worker
                                  never references the fake: it reaches it, like a real org, at Salesforce:InstanceUrl
  SalesforceSync.LoadGenerator/   Produces account change (with bursts), relationship and contact events to Kafka
  SalesforceSync.AppHost/         Aspire: Postgres + pgAdmin, Kafka + Kafka UI, mock Salesforce, worker, generator, benchmarks
tests/
  SalesforceSync.Tests/           Unit tests: the Salesforce half of the engine with every strategy (and test-only
                                  Lookup objects, LookupObjects.cs) against the in-memory org, the engine's flow with a
                                  stand-in inbox, the pure write-path rules, type normalization, error classification, execution strategy, limits
  SalesforceSync.IntegrationTests/  End-to-end tests: Postgres + Kafka in Testcontainers, worker pods and the mock org
                                  in-process; Kafka in, assertions on the mock org + inbox
benchmarks/
  SalesforceSync.Benchmarks/      BenchmarkDotNet: real Postgres inbox, the in-memory org as the HTTP handler
sql/
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
- **fake-salesforce**: mock Salesforce org over HTTP (OAuth token, describe, query, sObject Collections upsert by
  external id on its own or as Composite subrequests, gzip). Its Text foreign keys (`AccountId__c`,
  `RelatedAccountId__c`) are stored as plain text, and a create or update whose key names an Account that doesn't
  exist is rejected (`FIELD_CUSTOM_VALIDATION_EXCEPTION` "Parent record not found"), as the org's automation would. Logs its counters every 15s; inspect it at `/__fake/stats`, `/__fake/transport`,
  `/__fake/records/{sobject}[/{externalId}]`, `/__fake/history/{sobject}/{externalId}` and `/__fake/requests` (the
  write requests as received: Composite requests, and upserts as `{ method, url, body }`); inject errors with
  `POST /__fake/faults`
- **salesforce-sync**: consumes `account-changes`, `account-relationship-changes` and `contact-changes` into their inbox tables and syncs
  to `fake-salesforce` through the real OAuth + REST client
- **load-generator**: 20k account events (full records) over 500 accounts at 200/s, including bursts to the same
  account, plus relationship events between accounts and contact events of accounts
- **benchmarks**: explicit start, against `benchdb`: **Run benchmarks...** asks which run (quick, about 2 minutes;
  drain, about 20; full, about 30) and starts it; ▶ repeats the last run chosen (quick at first)

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

The build checks XML doc comments in every project (`Directory.Build.props` turns on `GenerateDocumentationFile`):
a comment that documents some parameters but not all (CS1573), a `cref` that doesn't resolve (CS1574) and the like
are warnings, so `dotnet build -warnaserror` fails on them. Undocumented public members (CS1591) are allowed.

Libraries: **xUnit** (2.x), **FluentAssertions** (pinned to 7.x, `[7.2.2]`: version 8 needs a commercial license),
**AutoFixture** (+ `AutoFixture.Xunit2` for `[AutoData]`) for values that only need to exist or be unique (keys,
names, numbers that are written and read back; documented examples such as the 15 → 18-character id stay
literal), and **Testcontainers** (`Testcontainers.PostgreSql`, `Testcontainers.Kafka`) for the integration tests.

### Integration tests

Each xUnit collection fixture (`SyncFixture`, `MultiPodFixture`, `FewRetriesFixture`, `DailyBudgetFixture`,
`ConcurrentLimitFixture`)
starts its own environment once, shared by the tests in that collection:

- **Postgres** (`postgres:17`) and **Kafka** (KRaft) in containers, removed when the collection finishes. Every
  topic is created up front with 6 partitions, like production. `MultiPodFixture` also starts **Redis**
  (`redis:7.4-alpine`), so its pods share their Describe cache like replicas in production; the other fixtures'
  pods use the in-memory fallback.
- **The mock Salesforce org**: the `SalesforceSync.FakeSalesforce.Server` app itself (`FakeSalesforceServer.Build`,
  the same code its `Program.cs` runs) on Kestrel at a random loopback port, so the worker goes through real HTTP:
  OAuth token, gzip, the REST client.
- **Worker pods**: each pod is its own in-process `IHost` built with `AddSalesforceSyncWorker()`, the exact
  registrations `Program.cs` uses (options and validation, Npgsql, the Salesforce client with the rate limiter,
  auth handler and token provider, `KafkaInboxConsumer`, `SyncSchemaInitializer`, `RateLimitInitializer`,
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
bursts coalesced to the newest full record, stale versions never written (left out of the row, or superseded by
the watermark: the `synced_time` of the entity's inbox row), unchanged events skip the
write, exactly 3 API calls per batch of new Accounts and Profiles (the read, the Accounts' upsert, the Profiles'), a
redelivered event overwriting its row and superseded without a write, an older event stored after a newer one changing
nothing, A → B → A ending at A, one account's events under different or no Kafka keys
ending at the newest `meta.time`, invalid events (no time offset, no or an unknown `meta.type`, another type's event,
not JSON) skipped at ingest, an event waiting for its entity's lease, a newer event overwriting the row whatever its
status and an older one leaving it as it is (`OneRowPerEntityTests`), an outcome for a claimed event never recorded on
the newer event that overwrote it meanwhile, an
expired worker unable to record anything, the unknown-outcome hold, every inbox table created at startup with
identical columns, the unique `entity_id` indexes, the partial ready index and autovacuum settings, `InboxOneRowPerRecord`
keeping each entity's newest row, `StateInInboxRow` moving each entity's state into its row (and back down),
migrating again changing nothing, a database created
before migrations recorded as migrated with its rows kept, and older shapes stopping startup untouched. Tests
publish version N as `meta.time` = a fixed base time + N ms, and read it back from the stored payload. Relationships
(`RelationshipTests`): one upsert with the two Accounts' keys as text once both exist, later only changed fields
(an unchanged key not sent, a changed one sent); a relationship that arrives before its Accounts waits
(`DEPENDENCY_NOT_SYNCED`, retried) with nothing sent and syncs once they exist; a malformed
relationship event is dead-lettered alone. Contacts (`ContactTests`): upserted with their Account's key in `AccountId__c` once the read
has found the Account, an unchanged contact not written and a changed one sent with only what differs; a contact that arrives
before its Account waits (`DEPENDENCY_NOT_SYNCED`, found by the read) and syncs once it exists; a contact without `lastName` or its
Account dead-lettered alone before any call; a lost upsert answer held, then ending at the newest event. Every record
ends with its class's `RecordTypeId`. Pods share their
Describes through Redis, so a pod that has described nothing finds them there (`SharedDescribeCacheTests`). Writes are checked with the mock's per-record write history
(`/__fake/history/{sobject}/{externalId}`) and with the requests it received (`/__fake/requests`).

The write itself (`CollectionsWriteTests`): a new Account and Profile as one Composite request, the Account's upsert,
then the Profile's carrying `AccountId__c` = the account key, with `allOrNone: false` and no reference; an Account
saved while its Profile failed being retried with the watermark unmoved until both are in; a failing Account
dead-lettered alone, its Profile sent with it and rejected by the org, while another account is saved; `inserted` and
`updated` pending together becoming one create; a later `inserted` for an existing record updating only what differs,
sent as one Collections upsert since only the Account changed; the next change of the Account alone as an upsert of
that field (the unchanged Profile and its key not written); a change of both the Account and its Profile as one
Composite request, the Account's upsert first, each with only its changed field. `DeadLetterTests` (`FewRetriesFixture`, `Sync:MaxRetries=3` from configuration): a
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

Unit tests (`dotnet test tests/SalesforceSync.Tests`, no Docker) run the Salesforce half of the engine with every
strategy against the in-memory org: 2 calls per batch of new Accounts and Profiles (the read and one Composite request
of an upsert per object, the Profiles carrying the account key), the exact verbs, bodies and `allOrNone` flags sent,
one object written as one upsert and several as one Composite request whatever the strategy's classes, values
Salesforce stores differently written once rather than on every event, a bad value failing only its own unit before
any call, a validation error or a row lock costing only its own entity, an Account saved without its Profile, a
create losing a race, a failed new Account taking its Profile (sent, rejected by the org) down without dead-lettering
anyone else, more than 5 upserts split over two Composite requests with each entity's records together, failures without a
cause spending a retry, create or update decided by
what Salesforce has, a Profile moved to another Account (its key rewritten), relationships with their Accounts' keys
and missing dependencies (found by the read, or the org's rejection with its message configured in
`Salesforce:ParentMissingMessages`, permanent without it), the mock org rejecting a key whose parent doesn't exist,
gzip, clearing fields that became empty, an Account-only change sent as one upsert with only that field,
Account-only and Profile-only changes as one Composite request (Accounts first, 2 calls), an entity written in two
upserts saved only when both are while the other entities keep their outcome, a lost answer of the Account and Profile
Composite request holding the whole batch (the retry then finds both applied and writes nothing), a later request of
a batch failing as a whole failing, holding or deferring the whole batch. Contacts: a new one as one upsert (its non-empty fields, the Account's
key as text, the `RecordTypeId`), an unchanged one read and not written, a changed one as one upsert with only
the changed fields, a missing Account as `DEPENDENCY_NOT_SYNCED` before any write, bad values and Salesforce
rejections costing only their own entity, a lost answer as an unknown outcome, the upsert call's per-record answer
(`created`). Real Lookup fields, with test-only classes (`LookupObjects.cs`: a Contact with the standard `AccountId`
lookup): a new parent and its new child in one Composite request linked by `@{Account[i].id}`, a failed new
parent taking down the reference subrequest without dead-lettering anyone else, a parent of another inbox resolved to
the Id the read found or waited for, Salesforce rejecting a lookup target at write time as `DEPENDENCY_NOT_SYNCED`.
As pure rules: the upserts of a request (one per level and object, Accounts first; one object one upsert, several
objects one request of several, also for objects the sync has no strategy for; a foreign key needs no reference; a
Lookup's child linked by `@{reference}` in an upsert of its own; 300 records of one object as 200 + 100, each once),
a foreign key written only when its text differs, a missing parent (lookup or foreign key) waiting with nothing sent,
a child whose new parent failed in an earlier upsert sent anyway with the parent's error deciding, the configured
"parent missing" message meaning wait, one outcome per entity across upserts, and an update upserted onto a deleted
record. Also: each mapper (an AutoFixture payload in, the expected
sObject out, nulls passed through), sObject → record (names from the attributes, nulls included, foreign keys as
fields and parents, lookups and external id), the guard that every Salesforce name on an sObject class exists in the mock org's schema, the event mapping
(account with and without a profile, relationships, contacts, missing Account ids, camelCase and PascalCase bodies), what the
engine does with a claim (newest `meta.time` wins, ties, watermark, A → B → A, invalid rows alone), what the consumer
admits (the typed read: id, time with offset, `meta.type`), the classification of failed writes (unknown outcome or
not), inbox table names as plain identifiers, every normalization rule (including the documented 15 → 18-character id
example), error classification, the Postgres execution strategy (retried, not retried, gives up), the daily budget
(the configured allocation as the maximum, lowered by the one Salesforce reports), the token bucket's decision
(`TokenBucketTests`: granted with the refill capped at the capacity, out of tokens, paused, over the budget until the
oldest bucket leaves a default or configured window, at least a minute), startup validation (including every
registered row type's query and write limits) and throttle decisions.

### Concurrency: many threads, many pods, same accounts

`ConcurrencyTests` (`MultiPodFixture`) scales the worker like a Kubernetes deployment: **3 pods x 8 worker loops**, all on one
inbox and one org, while 60 accounts each receive 25 updates in waves (new events keep arriving while those
accounts are being processed). Salesforce has nothing that rejects an out-of-order write, so a race would
land in the org and show up in the mock's write history. The rate limiter is opened up and batches are small
(10 accounts) so workers contend for the same accounts. The test asserts:

- **No overlapping claims**: `sql/claim-audit.sql` (test-only trigger on the inbox tables) records every claim from the
  moment a worker sets an entity's `lock_token` until that worker clears it, ordered by a sequence; no two claims on the
  same account may overlap.
- No out-of-order write: in every record's write history, the version carried in the data (`Description`
  `vN` on the Account, `Score__c` on the Profile) only moves forward. Every account's inbox row ends holding its
  newest event, `done`.
- Every Account and Profile ends with the version 25 values.
- Every account was claimed repeatedly (real contention) and all 3 pods wrote to Salesforce (`/__fake/clients`, per
  User-Agent).

`ClaimRaceTests` replays the interleavings this can't force on purpose: a claim while another claim of the entity is
open, a claim while an event of it is being stored, two events of it stored at once (the second waits, then
overwrites the first), every kind of outcome waiting for a store of a newer event (not recorded on it), and an event
stored while the outcome transaction has the row (it waits, then overwrites the finished row and leaves it due).

`Pods_survive_their_Postgres_connections_being_dropped` repeats this while every worker and consumer connection
is terminated (`pg_terminate_backend`) every 200 ms (the test's own sessions are spared by their
`application_name`): every account's row still ends at its newest event, `done` (or `superseded`, if a store cut off
by a dropped connection was retried), with no overlapping claims and no out-of-order writes. The fixture allows 15 short Postgres retries (`Postgres:Retry`, at most about 12 s, inside
the 20 s lease), so a unit of work outlasts the chaos instead of giving up and leaving its lease to expire.

## Benchmarks

Only the Salesforce HTTP endpoint is mocked; the Postgres inbox, workers, client and rate limiter are real.

```bash
# use a dedicated database: the benchmark truncates the inbox tables and resets sf_rate_limit
export ConnectionStrings__benchdb="Host=localhost;Port=5432;Database=benchdb;Username=postgres;Password=postgres"

dotnet run -c Release --project benchmarks/SalesforceSync.Benchmarks                 # asks which run in the console
dotnet run -c Release --project benchmarks/SalesforceSync.Benchmarks -- --run quick   # 5 drain cases, 3 iterations, about 2 minutes
dotnet run -c Release --project benchmarks/SalesforceSync.Benchmarks -- --run drain   # every drain and streaming case, about 20 minutes
dotnet run -c Release --project benchmarks/SalesforceSync.Benchmarks -- --run full    # drain, streaming and micro, about 30 minutes
dotnet run -c Release --project benchmarks/SalesforceSync.Benchmarks -- --filter '*Micro*'   # any BenchmarkDotNet arguments
```

The quick run is drain with 1 and 4 workers x 1 and 3 pods at 100 accounts and no latency, plus streaming with 4
workers in 1 pod; it still checks correctness on every iteration.

Results, the SQL statement measurements and the before/after of this tuning: [docs/benchmarks/README.md](docs/benchmarks/README.md).

The micro benchmarks need no database: header parsing, preparing a claim's rows (`PrepareTenEntities`), the typed mapping (`MapAccountEvent`: event JSON →
payload → sObjects → records; `MapPayloadToSObject` and `MapSObjectToRecord` for its two halves) and the type-aware
diff.

Both drain benchmarks run P **pods** (independent service providers: own connection pool, rate limiter and
HTTP client, like separate processes) x W worker loops against one Postgres inbox and one org. Each iteration
**fails** if any account doesn't end at its newest version, any record's write history shows an older version
after a newer one, any entity's inbox row doesn't hold its newest event or isn't `done` or `superseded`, or the
claim audit shows two overlapping claims on the same entity. Each iteration prints a `//` line with the mock org's
counters (`api`, `writeRequests`, `recordsOk`, ...), `entities` (inbox rows) and `superseded` rows, the coalescing
(events per entity claim), claims, retries, overlaps, stale rows and regressions.

- `InboxDrainBenchmarks`: the events are loaded as storing them in order would leave the inbox (each entity's newest,
  through a temporary table), then drained. Params: `Pods` (1/3), `Workers` (1/4/8),
  `Accounts` (100/1000), `LatencyMs` (0/25), `Events` (5000).
- `StreamingIngestBenchmarks`: events keep arriving (`InboxSql.Store`) while the pods drain (batches of 10
  accounts), so the same account is claimed, released and re-claimed by different workers in different pods, and its
  row is overwritten while a worker writes the event it claimed. Params: `Pods`
  (1/3), `Workers` (4/8), 50 accounts, 5000 events, 5 ms latency, `CoalesceDelayMs` (the `Sync:CoalesceDelay`
  default, 50 ms).

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
  "DailyBudgetShare": 0.5,          // this integration's share of the org's daily allocation, per BudgetWindow
  "BudgetWindow": "1.00:00:00",     // the rolling window the share is counted over
  "UsageBucket": "00:10:00",        // the granularity of that count (sf_api_usage rows)
  "DailySoftRatio": 0.8, "DailySoftSpeed": 0.25,
  "DailyHardRatio": 0.95, "DailyHardPause": "00:15:00",
  "ConcurrentLimitPause": "00:00:30",
  "MaxWait": "00:00:20"
}
```

| Setting (`RateLimit:`) | Default | Meaning |
|---|---|---|
| `MaxRequestsPerSecond` | 2 | Token bucket refill, all pods together |
| `BurstCapacity` | 5 | Token bucket size: calls that may be made back to back after an idle period |
| `DailyBudgetShare` | 0.5 | This integration's share of the org's allocation (the lower of `SalesforceLimits:DailyApiAllocation` and what Salesforce reports), in (0, 1] |
| `BudgetWindow` | `1.00:00:00` | The rolling window `DailyBudgetShare` is counted over: at most that many calls in any window of this length. 24 hours, as Salesforce counts the allocation; a shorter window doesn't scale the budget down, so it allows more calls per 24 hours |
| `UsageBucket` | `00:10:00` | The granularity of the usage count: calls are counted in buckets of this length, and every bucket that overlaps the window counts in full (over- rather than under-counted by up to one bucket). Must be > 0 and shorter than `BudgetWindow`, or startup fails |
| `MaxWait` | `00:00:20` | Longest wait for a token before the batch is handed back as rate limited |
| `DailySoftRatio` / `DailySoftSpeed`, `DailyHardRatio` / `DailyHardPause`, `ConcurrentLimitPause` | 0.8 / 0.25, 0.95 / 15 min, 30 s | Slow down, pause (see [Slowing down and pausing](#slowing-down-and-pausing)) |

| Salesforce limit | How it's applied |
|---|---|
| **Total API requests per 24 hours**: depends on the edition, licenses and add-ons (e.g. 115,000 for an Enterprise org with 15 Salesforce licenses); Setup shows the org's number | `SalesforceLimits:DailyApiAllocation` (required) as the maximum, lowered to the limit in `Sforce-Limit-Info: api-usage` when Salesforce reports less. The allocation is shared by every user and integration of the org, so this integration gets `DailyBudgetShare` of it. Every call is counted in `sf_api_usage` (`UsageBucket`, 10-minute buckets) and checked under the shared row lock, so all pods together never exceed the budget over any rolling `BudgetWindow` (24 hours). When it's used up, events are deferred until the oldest calls leave the window. Usage is rolling, not reset at midnight. |
| **What a call is**: each HTTP request; a Composite request (and an sObject Collections request) counts as one call however many subrequests it holds | One token per HTTP request: a describe (cached), each query page, each Composite write and each upsert (one per object of a request without references). Every call goes through the same `SalesforceRateLimitHandler`; a batch's calls are sent one after another under its one concurrency slot. |
| **Org-wide usage** (also other integrations): `Sforce-Limit-Info: api-usage=used/limit` on every REST response | `api-usage` and, when Salesforce sends it, `per-app-api-usage` (the connected app's quota): slow down at `DailySoftRatio`, pause at `DailyHardRatio`. The allocation Salesforce reports, the configured maximum and the resulting budget are logged when first seen and whenever Salesforce's changes. `api-bursts` appears in Salesforce's example of the header but isn't defined, so it is recorded in `sf_limit_status` and not acted on. |
| **Requests per second**: Salesforce publishes no such limit, and none on concurrent requests shorter than 20 s | `MaxRequestsPerSecond` and `BurstCapacity` are this integration's own pacing, so it spreads its share of the daily allocation instead of spending it in bursts. |
| **Concurrent long-running requests** (20 s or longer): 25 for production and sandboxes, 5 for Developer Edition and trials | `SalesforceLimits:ConcurrentLongRunningLimit` (default 25). `Sync:MaxConcurrentRequests` (global advisory-lock slots) must not exceed it, or startup fails. Calls that take `LongRunningThreshold` or longer are logged. `REQUEST_LIMIT_EXCEEDED` with `ConcurrentPerOrgLongTxn` pauses all workers for `ConcurrentLimitPause`. |
| **Total requests exceeded** (`REQUEST_LIMIT_EXCEEDED`, `TotalRequests`) | Pauses all workers for `DailyHardPause`. Salesforce may allow some overage on paid orgs, but it isn't relied on. |
| **API timeout**: 10 minutes for REST calls, for a composite request as a whole (query calls follow the SOQL limits and fail with `QUERY_TIMEOUT`, retried as transient) | `RequestTimeout` (HTTP client timeout, including the token wait) must be at most 10 minutes, or startup fails. The worker also gives up at 80% of its lease. |

The limiter is registered by `AddSalesforceRateLimiting()`, which the worker (`AddSalesforceSyncWorker`), the
benchmark pods and the test pods all use. Its handler is always the outermost on the Salesforce client (rate limiter
→ gzip → auth), so every request takes a token before it is compressed and authenticated.

### Where the limiter keeps its state

| Where | What it holds | Rows |
|---|---|---|
| `sf_rate_limit` | What is true now, shared by every pod: the token bucket (`tokens`, `capacity`, `refill_per_sec`, `base_refill_per_sec`, `updated_at`), the shared pause (`paused_until`), this integration's budget (`daily_budget`) and what Salesforce last reported in `Sforce-Limit-Info` (`api_used`/`api_max`, `app_used`/`app_max`, `burst_used`/`burst_max`) | Exactly one (`name = 'salesforce'`), seeded by the migration |
| `sf_api_usage` | This integration's own calls, counted per `UsageBucket` (10 minutes) | About 145 (24 hours / 10 minutes, plus the bucket that straddles the window's start) |
| `sf_limit_status` | A view of `sf_rate_limit` for people: usage as percentages, the current and normal rate, the pause, the budget | None of its own |
| Advisory locks | The [concurrency slots](#concurrency-slots-postgres-advisory-locks) | No table: Postgres's lock manager, in memory |

**Why two tables.** `sf_rate_limit` is the current state, one row that every change locks. `sf_api_usage` is history,
and the budget needs it: Salesforce counts the allocation over a rolling 24 hours, not from midnight, so a single
counter can't say which calls have left the window. A row per call would be exact but means about 57,000 inserts and
deletes a day. A row per 10-minute bucket keeps about 145 rows; a bucket that is partly inside the window counts in
full, so usage is over-counted by up to one bucket of calls, the safe direction (the limiter stops a little early
rather than overspending). A grant adds 1 to the current bucket; buckets older than `BudgetWindow + UsageBucket` are
deleted.

**How a call acquires a token.** `SalesforceRateLimiter.AcquireAsync`, in one transaction:

1. Lock the `sf_rate_limit` row (`select * ... for update`, the only raw SQL besides `select clock_timestamp()`). Every
   acquire in every pod queues here, so no two decide at once. The database clock is read in the same statement, so
   every pod uses one clock, whatever its own says.
2. Read this integration's calls in the buckets since `BudgetWindow + UsageBucket` ago (LINQ).
3. `TokenBucket.Decide` (pure C#, unit-tested), in this order:
   - paused: wait until `paused_until`;
   - budget used up: wait until the oldest bucket leaves the window (at least a minute);
   - too few tokens (`min(capacity, tokens + elapsed x rate)`): wait until enough have refilled;
   - otherwise grant: write the tokens back and add the call to the current bucket (a plain read-then-insert-or-update
     with EF: the row lock serializes it).
4. Commit, which releases the lock.

Not granted, it waits (with jitter) and tries again, until `MaxWait`. Then the batch is handed back as rate limited:
its events are deferred without spending an attempt. Applying the configuration, recording `Sforce-Limit-Info` and
pausing lock the row the same way, refill with the same function, and delete the buckets older than the window.

### Slowing down and pausing

Every Salesforce response carries `Sforce-Limit-Info: api-usage=used/limit` and, when the connected app has a quota of
its own, `per-app-api-usage=...`. `SalesforceRateLimiter.ReportLimitsAsync` stores them on the row (at most every
`ReportInterval`, 5 seconds, unless the throttle level changes) and takes the highest of three ratios: the org's usage
(every integration and user), the connected app's usage, and this integration's own calls over its budget (from
`sf_api_usage`).

| Highest ratio | Effect on every pod |
|---|---|
| below `DailySoftRatio` (80%) | Normal: `refill_per_sec` = `base_refill_per_sec` (`MaxRequestsPerSecond`) |
| `DailySoftRatio` or more | Slower: `refill_per_sec` = `base_refill_per_sec` x `DailySoftSpeed` (0.25) |
| `DailyHardRatio` (95%) or more | Stopped: `paused_until` = now + `DailyHardPause` (15 minutes) |

The normal rate is kept beside the current one, so slowing down and recovering never lose the configured rate.

Responses that pause every pod (`paused_until`; `SalesforceBatchGate` starts no batch while it is in the future):

| Salesforce answers | Pause |
|---|---|
| `REQUEST_LIMIT_EXCEEDED`, `TotalRequests` (the org's daily allocation) | `DailyHardPause` (15 minutes) |
| `REQUEST_LIMIT_EXCEEDED`, `ConcurrentPerOrgLongTxn` (too many long-running requests) | `ConcurrentLimitPause` (30 seconds) |
| 503, or 429 from something in front of Salesforce | `Retry-After`, or 30 seconds without one |

### Configuration and monitoring

Every pod applies the bucket size, rate and budget to the shared `sf_rate_limit` row at startup (keeping the current
tokens and slowdown), so the last pod to start wins: roll out `RateLimit` changes to all pods together.

```sql
-- Salesforce's last report, the current and normal rate, the pause and this integration's budget
select * from sf_limit_status;

-- this integration's own calls over the default window (a view can't read the configured BudgetWindow)
select sum(calls) from sf_api_usage where bucket >= now() - interval '24 hours 10 minutes';
```

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
so there is no expiry, heartbeat or cleanup job. (`pg_advisory_lock` in `SyncSchema` does the same for
migrations: it lets one pod at a time migrate the database.)

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
client credentials flow only works on the org's own domain. The token URL defaults to the same address plus
`/services/oauth2/token`; set `Salesforce:TokenUrl` only if your org issues tokens at another address.

### 2. Create the user the sync runs as

With the client credentials flow there is no login. Salesforce runs every API call as one user that you choose, so the
sync can do only what that user may do.

1. **Users → New User**. User License **Salesforce Integration**, Profile **Minimum Access - API Only Integrations**.
   Give it a name like "Salesforce Sync" and a real email address you can read. The username must be unique across
   all Salesforce orgs, for example `salesforce-sync@acme.dev-sandbox`.
2. **Permission Sets → New**. Label `Salesforce Sync`, License **Salesforce API Integration**. In **Object Settings**,
   give it **Read, Create and Edit** on Account, Contact, Profile and Account Relationship (the objects of step 4), and
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

The sync writes four objects. Account and Contact are standard; `Profile__c` and `Account_Relationship__c` are custom
objects to create. Create them and their fields in **Object Manager** (**Create → Custom Object**, then **Fields &
Relationships → New** on each object). The API name is what you type in **Field Name** plus `__c`: Field Name
`ExternalId` gives `ExternalId__c`.

Every object needs an `ExternalId__c` field with **Unique** and **External ID** ticked. The sync finds and links
records by it, never by the Salesforce Id.

**Account** (standard object). These standard fields need nothing: `Name`, `Rating`, `Industry`, `Description`,
`AnnualRevenue`, `NumberOfEmployees`, `Phone`, `Website`. Add:

| Field Name → API name | Type |
|---|---|
| `ExternalId__c` | Text(255), Unique, External ID |
| `Active__c` | Checkbox |
| `Tags__c` | Picklist (Multi-Select). Values the events send, e.g. `B2B`, `Retail` |
| `Customer_Since__c` | Date |
| `Last_Contacted__c` | Date/Time |

**Contact** (standard object). `FirstName`, `LastName`, `Email` and `Phone` need nothing; the standard `AccountId`
lookup is left alone. Add:

| Field Name → API name | Type |
|---|---|
| `ExternalId__c` | Text(255), Unique, External ID |
| `AccountId__c` | Text(255): the Account's `ExternalId__c` |

Contacts are read and matched by `ExternalId__c`, and their Account is checked by the Account's `ExternalId__c`;
both must be marked **External ID**. The user needs **Read** and **Edit** on every Contact field the sync sends.

**`Profile__c`** (custom object). Label `Profile`, Object Name `Profile`, Record Name `Profile Name` of type Text.
If Salesforce refuses the name, pick another and change it on `ProfileSObject` (see [field mappings](#field-mappings)).

| Field Name → API name | Type |
|---|---|
| `ExternalId__c` | Text(255), Unique, External ID |
| `AccountId__c` | Text(255): the Account's `ExternalId__c` |
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
| `ExternalId__c` | Text(255), Unique, External ID |
| `AccountId__c` | Text(255): the Account's `ExternalId__c` |
| `RelatedAccountId__c` | Text(255): the related Account's `ExternalId__c` |
| `Type__c` | Picklist: `Parent`, `Subsidiary`, `Partner`, `Reseller` |
| `Start_Date__c` | Date |
| `Active__c` | Checkbox |

The `…Id__c` fields are the org's foreign keys: Text fields holding the parent's external id, not Lookup fields (see
[Foreign keys](#foreign-keys-text-fields-holding-the-parents-external-id)). The org's own automation is expected to
reject a record whose key names an Account that doesn't exist; the sync sends parents first and checks the Accounts of
relationships and contacts before writing them.

While creating each field, Salesforce asks for field-level security: make it visible (not read-only) for the
`Salesforce Sync` permission set, or grant field access in the permission set afterwards.

**Record types.** Every object needs the record type its sObject class names (`RecordType` in `[SObject]`, see
[Record types](#record-types)):

| Object | Record type developer name (Record Type Name) |
|---|---|
| Account | `Business` |
| `Profile__c` | `Standard` |
| `Account_Relationship__c` | `Standard` |
| Contact | `Standard` |

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

A mapping is code, in three parts of the object's file (`Sync/AccountSync.cs`, `Sync/AccountRelationshipSync.cs`,
`Sync/ContactSync.cs`):

1. **Payload**: the event's JSON fields.
2. **sObject**: the Salesforce object and field API names and the record type, in the
   `[SObject]` and `[JsonPropertyName]` attributes. This is the only place Salesforce names appear.
3. **Mapping** (`ToAccount`, `ToProfile`, `ToRelationship`, `ToContact`): one line per field, from the payload to the sObject.

The mappings as they are:

| Event field | Salesforce field |
|---|---|
| `account.externalId` | `Account.ExternalId__c` |
| `account.name`, `rating`, `industry`, `description`, `annualRevenue`, `numberOfEmployees`, `phone`, `website` | `Account.Name`, `Rating`, `Industry`, `Description`, `AnnualRevenue`, `NumberOfEmployees`, `Phone`, `Website` |
| `account.isActive` | `Account.Active__c` |
| `account.tags` (a list) | `Account.Tags__c` (values joined with `;`) |
| `account.customerSince` | `Account.Customer_Since__c` |
| `account.lastContactedAt` | `Account.Last_Contacted__c` |
| `account.profile.externalId` (the account's own if missing) | `Profile__c.ExternalId__c` |
| the account itself (its `externalId`) | `Profile__c.AccountId__c` (Text) |
| `account.profile.name`, `tier`, `score`, `notes`, `optIn`, `startDate` | `Profile__c.Name`, `Tier__c`, `Score__c`, `Notes__c`, `Opt_In__c`, `Start_Date__c` |
| `account.profile.discountPercent` | `Profile__c.Discount__c` |
| `accountRelationship.externalId`, `name`, `type`, `startDate`, `isActive` | `Account_Relationship__c.ExternalId__c`, `Name`, `Type__c`, `Start_Date__c`, `Active__c` |
| `accountRelationship.accountExternalId` | `Account_Relationship__c.AccountId__c` (Text; the Account must exist) |
| `accountRelationship.relatedAccountExternalId` | `Account_Relationship__c.RelatedAccountId__c` (Text; the Account must exist) |
| `contact.externalId`, `firstName`, `lastName`, `email`, `phone` | `Contact.ExternalId__c`, `FirstName`, `LastName`, `Email`, `Phone` |
| `contact.accountExternalId` | `Contact.AccountId__c` (Text; the Account must exist, checked by the read) |
| (none: the sObject class's `RecordType`) | `RecordTypeId` of every object: the Id of `Business` (Account) or `Standard` (`Profile__c`, `Account_Relationship__c`, Contact), from the cached Describe |

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

Store the values from steps 1 and 3 in the AppHost's user secrets. Nothing else changes: the worker talks to the mock
org and to a real one the same way, and only `Salesforce:InstanceUrl` (with the real credentials) tells them apart.
When they are set, the AppHost skips the mock org, points the worker at the sandbox, and makes the load generator send a small run (200 events over 20 accounts) instead
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
SELECT ExternalId__c, Name, Description, Active__c, LastModifiedDate FROM Account WHERE ExternalId__c LIKE 'ACC-%'
SELECT ExternalId__c, AccountId__c, Tier__c, Score__c FROM Profile__c
SELECT ExternalId__c, AccountId__c, RelatedAccountId__c, Type__c FROM Account_Relationship__c
SELECT ExternalId__c, AccountId__c, LastName, Email FROM Contact WHERE ExternalId__c LIKE 'CON-%'
```

The `salesforce-sync` logs in the Aspire dashboard show each batch, and pgAdmin shows the inbox tables: every row should
end `done` or `superseded`. A `dead` row's `last_error` says what Salesforce rejected. Delete the test records
afterwards: `ACC-…`, `PRF-…`, `REL-…` and `CON-…`.

To run the worker without Aspire (a server or Kubernetes), give it the same settings as environment variables
(`Salesforce__InstanceUrl`, `Salesforce__ClientId`, `Salesforce__ClientSecret`,
`SalesforceLimits__DailyApiAllocation`), plus `ConnectionStrings__inboxdb` and `ConnectionStrings__kafka`, and
`ConnectionStrings__redis` so the replicas share their Describe results (optional: without it each process describes on
its own; see [The Describe cache](#the-describe-cache)). With Aspire, Redis needs no setup: the AppHost starts it and
passes the connection string.

### 6. Tune and check

- Choose this integration's share of the daily allocation with `RateLimit:DailyBudgetShare`, and set
  `SalesforceLimits:ConcurrentLongRunningLimit` (5 on Developer Edition and trial orgs). See
  [Salesforce API limits](#salesforce-api-limits).
- Tune `Salesforce:PermanentErrorCodes`, `Sync:RetryBaseDelay` / `MaxRetryDelay` / `MaxRetries` and `Postgres:Retry`
  if needed.
- Set `Salesforce:ParentMissingMessages` (a list of case-insensitive substrings, empty by default): a record error
  whose message contains one of them is `DEPENDENCY_NOT_SYNCED` (the record waits for its parent, retried like a
  parent the read didn't find), whatever its code. Find the message your org returns for a child whose Text key names
  a missing parent (below) and add it here, e.g. in `appsettings.json`:
  `"Salesforce": { "ParentMissingMessages": [ "Account not found" ] }` (or `Salesforce__ParentMissingMessages__0`).
  Empty, such a rejection is what its code says (`FIELD_CUSTOM_VALIDATION_EXCEPTION`: permanent, dead-lettered). `Sync` values that can't work (no workers or slots, `BatchSize` outside 1-100, ...), a row type's limits outside
  Salesforce's (`MaxValuesPerQuery` 1-2,000, `MaxRecordsPerWrite` 1-200) and a missing `Salesforce:InstanceUrl` fail startup.
- The mock org was built from Salesforce's documentation. These behaviours are worth confirming in the sandbox:
  - an empty checkbox (`null`) is stored as `false`, and an empty text (`""`) as empty;
  - a number with more decimals than the field allows is rounded;
  - a Date/Time keeps whole seconds;
  - updating a record whose Id no longer exists fails with a clear error code;
  - **the exact error your org returns when a Text key names a parent that doesn't exist**: e.g. create a
    `Profile__c` with `AccountId__c = 'NOPE'` (and an `Account_Relationship__c`, a `Contact` likewise). Expect the
    org's automation to answer `FIELD_CUSTOM_VALIDATION_EXCEPTION` (validation rule, trigger `addError`, flow Custom
    Error) or `CANNOT_INSERT_UPDATE_ACTIVATE_ENTITY` (an unhandled trigger exception) with a message of its own. Put a
    distinctive part of that message in `Salesforce:ParentMissingMessages`, so it maps to `DEPENDENCY_NOT_SYNCED`
    (the mock org answers `FIELD_CUSTOM_VALIDATION_EXCEPTION` "Parent record not found");
  - the Text foreign key fields exist with these API names and hold the parent's `ExternalId__c`:
    `Profile__c.AccountId__c`, `Account_Relationship__c.AccountId__c` and `RelatedAccountId__c`, `Contact.AccountId__c`;
  - a required field that has a default value can be left out on create;
  - concurrent workers on the same account, and a worker killed mid-batch, still end with every account at its newest
    version;
  - the sObject Collections upsert (`PATCH /composite/sobjects/{SObject}/ExternalId__c`): each result has `created`,
    and records in one call may carry different fields; as a Composite subrequest (Account and Profile upserts in
    one `POST /composite`), with the same per-record answers in `compositeResponse`.

## Status

Scaffold, not yet production-tested. Verify in a Salesforce sandbox, in particular concurrent workers on
the same account and killing a worker mid-batch.

## License

[MIT](LICENSE)
