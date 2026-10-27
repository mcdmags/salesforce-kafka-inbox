# salesforce-kafka-inbox

Kafka change events → Postgres inbox → Salesforce, with per-entity ordering, atomic multi-record upserts,
type-aware change-only updates, and a pause when Salesforce says it's over its API limits. One generic engine syncs every object;
each object only defines its typed payload, its Salesforce sObject class, the mapper between them and its queries.
Written in C# / .NET 10.

## The problem

An upstream system publishes account aggregates to Kafka: an account with its account-to-account relationships and
its contacts, as they were before a change and as they are after it. Each account change has to land in Salesforce
as an update to an **Account** and its related **Profile**; each relationship as an **Account_Relationship__c**
linking two Accounts, which may not be synced yet when the relationship arrives; each contact as a **Contact** of its
Account, which may not be synced yet either. A relationship or contact that is gone from the aggregate, or a deleted
account, is deleted in Salesforce. Several things make this harder than "consume and PATCH":

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
5. **Rate limits.** The org has a daily API allocation (also used by other integrations) and a cap on concurrent
   long-running requests. Many workers in many pods must stay under them, and back off when Salesforce says so.
6. **At-least-once delivery.** Kafka redelivers, workers crash, leases expire, and none of that may
   duplicate or regress data.

## How it's solved

| Problem | Solution |
|---|---|
| Durable, replayable intake | One topic (`account-changes`) of **account aggregates** (the Account with its Profile, relationships and contacts, `before` and `after`). The Kafka consumer reads each one once (`AccountAggregate.TryRead`) and hands it to every entity type, which projects the events of its own entities that changed (`IAggregateProjection`), deletes included (an unchanged child or Account is not stored: no row, no claim, no Salesforce read), and writes each event's **JSON** to its object type's Postgres **inbox table** (`sf_account_inbox`, `sf_account_relationship_inbox`, `sf_contact_inbox`) with the entity's id in a plain `entity_id` column, marking the entity due on that row in the same statement, before storing the message's offset (committed in the background, so there is no broker round trip per message). The id is read from the typed event in C#; **no SQL reads inside an event**. An inbox keeps **one row per entity**, holding its newest event: a newer event overwrites it, an older one changes nothing, and neither does a redelivery of an event that is synced already, so a crash halfway through a message is safe (the redelivery stores its events again). A message that isn't a valid aggregate (see below) is logged with its offset and skipped, rather than blocking its partition. |
| Ordering across parallel batches | Workers claim **entities, not events**: every entity has one inbox row, which holds its sync state too (due time, which is also the lease's end), and a claim is `SELECT … FOR UPDATE SKIP LOCKED` on the due rows, which it then leases (`next_attempt_at` moved to the lease's end, so a leased row is not due until then) and reads, in one statement. Two workers can never hold the same entity: they would lock the same row. Which event is the newest is decided by **`meta.time`**, not by arrival: the Kafka key and partition order are not used. |
| Bursts of updates to one entity | Pending events for an entity are **coalesced** in the inbox: every event carries the full record, so an event with a newer `meta.time` (or the same, stored later) overwrites the entity's row, and what a claim writes is the newest event stored before it. Five updates in five seconds become one Salesforce write with the newest data. An event that overwrites a row while a worker writes the event it claimed is not lost: that worker's outcome is recorded only on rows that still hold the event time it claimed, and the newer event is claimed next. |
| Late or replayed older events | **Never stored**: the entity's row is overwritten only by an event newer than the one it holds, or as new while that one is not `done` yet (`InboxRepository.AddAsync`, by `event_time`). An older event, or a redelivery of an event already in Salesforce, changes nothing and is never claimed, so it can't be written. Nothing is stored in Salesforce for this. |
| Expired leases | The Salesforce call is cancelled at 80% of the lease (measured from the claim), so a slow worker gives up before another worker can claim the entity. A worker whose lease did expire records nothing once another worker has claimed the entity: every outcome needs the row still under its claim's lease end (see [Why there is no lock token](docs/inbox/README.md#why-there-is-no-lock-token)). |
| Mapping errors | A field the code sends that Salesforce doesn't have (`INVALID_FIELD`) or won't let us set (`INVALID_FIELD_FOR_INSERT_UPDATE`) is wrong for every event of that kind, so it **pauses the inbox** instead of dead-lettering one entity at a time. Salesforce itself finds it: `INVALID_FIELD` on the read (the read selects every field the code sends, so a field missing in the org shows before anything is written) or `INVALID_FIELD_FOR_INSERT_UPDATE` / `INVALID_FIELD` in a write's answer (the other entities of that write are saved already and are read as unchanged next time). The batch's rows and every other due row of that inbox are then held (`MAPPING_ERROR`) for `Sync:MappingErrorHold` (5 minutes), with the error on the row (`last_error`: the field and the object) and an ERROR in the log. Nothing is dead-lettered and no attempt is spent, so after the fix is deployed (or the field added) the rows go on with no replay. The rows named are those whose records send the column Salesforce names (all of the object's when it names none). Nothing is cached about the org's fields, so a field that was only just added is found at once. |
| Org storage full | `STORAGE_LIMIT_EXCEEDED` in a write's answer means the org's data storage is full: every create fails the same way until an admin frees space (deleted records keep counting until the Recycle Bin is emptied; [`bulk-delete`](#7-clean-up-hard-delete-records-in-bulk) frees it at once). Like a mapping error, it **pauses the inbox** (a circuit breaker whose state is the rows' `next_attempt_at`, so every pod sees it): the batch's rows and every other due row of that inbox are held (`STORAGE_LIMIT_EXCEEDED`, the error on the rows) for a fixed `Sync:StorageFullHold` (15 minutes), with an ERROR in the log. No attempt is spent and nothing is dead-lettered. When the hold ends, the next claim tries again: still full pauses the inbox for another hold; otherwise syncing goes on. The other records of that write are saved and read as unchanged next time. Mapping errors and full storage take one path: `SalesforceCalls.InboxPausers` lists the error codes that pause an inbox, each with its reason (`PauseReason`), and the batch service holds the inbox for that reason's setting. Another org-wide error is one more line there. |
| Writes whose outcome is unknown | A write that timed out, was cancelled or lost its connection may still be applied by Salesforce later. Its entities are **held** (`UNKNOWN_OUTCOME`) for `Sync:UnknownOutcomeHold` (10 minutes) before anyone may write them again, so a late write can't overwrite newer data. See [Unknown outcomes](docs/inbox/README.md#unknown-outcomes). |
| Records that belong together | Written in the same batch, **parents first**: the Accounts' **upsert** is one call, then the Profiles' is another. The org's foreign keys are **Text fields holding the parent's external id** (`Profile__c.AccountId__c` = the account key), written as the text, never resolved to an Id, never a reference. A parent of another inbox (a relationship's or a contact's Account) must already be in Salesforce: the read checks, and a missing one makes the entity wait (`DEPENDENCY_NOT_SYNCED`) with nothing sent. Each record is saved or rejected on its own (`allOrNone: false`); the entity is done only when all of its records are in. An entity that failed in one call is not sent in the next: its outcome is already decided. See [Writing to Salesforce](#writing-to-salesforce). |
| Create or update | Decided by the read alone: a record Salesforce has is updated with what differs, one it doesn't have is created. `inserted` and `updated` are the same to the sync. |
| Deletes | A child gone from an aggregate's `after` (or the whole Account, when `after` is missing) is a `deleted` event in its inbox, ordered by `meta.time` like any other: an older insert can't bring it back, a newer one recreates it. The batch's read finds the record's Id by its external id, and every record to delete goes into one **sObject Collections delete** by Id (`DELETE /composite/sobjects?ids=...&allOrNone=false`), after the batch's upserts. A record the read doesn't find (never synced, or deleted already) costs nothing and is done. Already deleted is success, a refusal is permanent or transient by its code, a lost answer is held (`UNKNOWN_OUTCOME`). See [Deletes](#deletes). |
| Change-only updates | **Diff against Salesforce by the value's .NET type**: the current records are read by external id with every field of their sObject class (an event is the full record; an empty field is cleared if Salesforce has a value). Values are put in a canonical form by their type (`""` = `null`, numbers as decimals, date/time to the second, time to the millisecond; a checkbox `null` equals Salesforce's `false`) and only fields that differ are sent. The org's field metadata is not read, so a value Salesforce stores differently (a number with more decimals than the field's scale, multi-select values in another order than the picklist's) looks changed and is written again: the mapping should send values as Salesforce stores them. A new record's `null` checkbox is not sent (Salesforce defaults it to `false`). The comparison is with what is in Salesforce *now*, so edits made directly in Salesforce are corrected. |
| Batching and API calls | Every call is one operation on many records (`SelectAsync`, `UpsertAsync`, `DeleteAsync`); there is no Composite request. **One read call per object** per batch of up to `Sync:BatchSize` entities (180, max 200): a query of up to 200 external ids, whose URL fits Salesforce's 16,384-byte limit on a call's URI and headers (long ids split a query further). Then, after the diff, the writes: **one `UpsertAsync` per object** (≤200 records, Salesforce's limit; a batch is at most 200 entities), parents first, with exactly the planned fields, matched by external id; then **one `DeleteAsync`** of the Ids to delete (≤180). So a batch of new Accounts with their Profiles costs 4 calls (two reads, two upserts), an Account-only change 3 (two reads, one upsert: the Profile is read too), and relationships and contacts 3 (their own object and the Account they point at, read; one upsert). A batch of deletes only is its reads plus one delete, and a delete Salesforce has nothing for costs the reads only. See [One call per object](#one-call-per-object) for the cost against Composite requests. A batch with a new Account adds one query on `RecordType` the first time in a pod, then every 15 minutes (`Salesforce:RecordTypeRefreshInterval`). Request bodies are gzipped, responses accepted compressed. |
| Record types | Only a new Account gets a record type: `Business` for a business, `PersonAccount` for a person (`account.isPerson`), named by developer name in `AccountSync`. Its Id comes from one SOQL query on `RecordType`, cached for `Salesforce:RecordTypeRefreshInterval` (15 minutes), and is sent only when the Account is created; an update never sends or compares it. Profiles, relationships and contacts send none. A record type the org doesn't have (or not active) fails that new Account alone before any write (`INVALID_RECORD_TYPE`, retried) and makes the next batch read the record types again. See [Record types](#record-types). |
| Bad records | A value Salesforce rejects (too long, wrong type, required field emptied, a validation rule) fails only its own unit, by Salesforce's answer for that record (permanent or transient by its code; a required field emptied on an *update* is retried until `MaxRetries`, since the code can also mean the record was deleted meanwhile); an unknown or read-only field is a mapping error and pauses the inbox; a malformed event is dead-lettered on its own (`INVALID_EVENT`). |
| Failures | Each record's result decides per entity: **permanent** (validation rules, invalid fields, ...; configurable) → dead-lettered immediately, alone; **transient** (`UNABLE_TO_LOCK_ROW`, a parent not synced yet, a create that lost a race, unknown codes) → exponential backoff, dead-lettered after `MaxRetries`; The code is stored in the inbox row's `last_error_code`. See [Retries and dead letters](docs/inbox/README.md#retries-and-dead-letters). |
| Postgres failures | Every unit of work (claim, outcomes, ingest, schema) runs through EF Core's execution strategy (**`PostgresRetryStrategy`**, a `NpgsqlRetryingExecutionStrategy` configured from `Postgres:Retry`, used as `db.Database.CreateExecutionStrategy().ExecuteAsync(...)`; the inbox's statements commit one by one, with no explicit transaction): a dropped connection, failover, timeout, deadlock or serialization failure re-runs the whole unit (change tracker cleared) with exponential backoff and jitter. After a lost connection the Npgsql pool is cleared, since its idle connections are usually dead too. Every unit is safe to run twice; rows leased by a claim whose commit was lost wait for the lease to expire. A batch's outcomes are recorded with two statements, not one per entity: `MarkDoneAsync` for the synced rows and `MarkFailedAsync` for the failed ones (a permanent failure is dead-lettered at once by the same statement). |
| Rate limits | Salesforce publishes a daily total (a soft limit, rolling 24 hours) and a cap on concurrent long-running requests, and no per-second limit (see [Salesforce API limits](#salesforce-api-limits)). The sync doesn't throttle itself: it makes few calls (one per object per batch) and backs off when Salesforce refuses. `REQUEST_LIMIT_EXCEEDED` (either limit), 429 and 503 **pause the inbox** like a mapping error: its rows wait `Sync:RequestLimitHold` (1 minute), no attempt spent, nothing dead-lettered. `Sync:Workers` x pods is kept under the org's concurrent limit by configuration. |

```
Kafka (any key, any order) ─► KafkaInboxConsumer ─► one inbox table per object type (Postgres), one row per
  account-changes:             split before/after into  │  entity: entity_id + event JSON + status, and its
  account aggregates           the changed entities'     │  sync state (due time, lease)
  (before, after)              events (deletes too),    │
                               typed read + validate,   │
                               store + mark due         │
                                                       │  claim entities: their inbox rows, SKIP LOCKED
                                                       ▼
                              InboxWorkerService ×N loops (any number of pods), round-robin over:
                                AccountSync ─┐
                                AccountRelationshipSync ─┤   each: SalesforceBatchSyncService.SyncBatchAsync
                                ContactSync ─┤                 (reader, inbox, its Send step)
                                                                ▼
                              the flow (SalesforceBatchSyncService): claim → parse each entity's newest event
                                → the type's Send step: read what Salesforce has (1 query per object)
                                  → decide (create / changed fields / nothing / wait for a parent)
                                  → write: 1 collections upsert per object, parents first, then 1 delete (by Id)
                                → per-record answers → outcome per entity → record outcomes
                                                       │  inbox: IRepository<TRow> (EF Core + raw SQL, PostgresRetryStrategy)
                                                       ▼
                                                  Salesforce  (standard + existing fields only)
```

### How the code is layered

The sync is three layers, each with one job:

| Layer | Classes | Does |
|---|---|---|
| **Thin I/O** | `SalesforceRestClient` (the only class that talks to Salesforce: `SelectAsync`, and `UpsertAsync` and `DeleteAsync` for Collections upserts and deletes); `InboxRepository<TRow>` (`IRepository<TRow>`: store, claim, record, hold, defer, fail, over `SyncDbContext` and `PostgresRetryStrategy`) | Sends requests and returns raw results. Decides nothing. |
| **The flow** | `SalesforceBatchSyncService` | `SyncBatchAsync` is the shared structure of a batch and nothing else: claim → parse each entity's event → the type's **send step** (a delegate) → record outcomes, with the ways a batch can fail as a whole (unknown outcome → hold; mapping error, full storage or API limits → pause the inbox; anything else → retry all). The lease deadline is here too. |
| **What a type does** | `AccountSync`, `AccountRelationshipSync`, `ContactSync` (`ToSalesforce/`) | Each is one send step, written top to bottom: map its events to the fields Salesforce names, **read** what Salesforce has (`SalesforceCalls.ReadAsync`), **decide** per entity (`SalesforceCalls.ChangedFields`: create with the non-empty fields, update the fields that differ, or nothing; wait for a missing parent), **write** (`SalesforceCalls.UpsertAsync` / `DeleteAsync`). `SalesforceCalls` is a small toolbox of one-call primitives (record types, read, changes, upsert, delete, and what an answer means); the types call what they need and can do it differently. |

**Data access** is Entity Framework Core over Npgsql (`SyncDbContext`, pooled, on the app's `NpgsqlDataSource`). The
model defines every table in `OnModelCreating` (partial indexes with `HasFilter`, autovacuum settings with
`HasStorageParameter`), and EF migrations (`src/SalesforceSync.Data/Migrations`) create them at startup (see
[Schema and migrations](docs/inbox/README.md#schema-and-migrations)); `SchemaTests` checks the model against the real tables column by
column, and a unit test fails when the model has changed without a migration. Every inbox table has the same columns, so
they all live on the abstract `InboxRow`; `AccountInboxRow`, `AccountRelationshipInboxRow` and `ContactInboxRow` only
name their tables (independent entity types, no shared table), and the one
`InboxRepository<TRow>` (`IRepository<TRow>`, registered as an open generic) serves them all. The table name comes
from the EF model. Storing an event, the
claim, every outcome (done, fail, dead-letter, hold) and the end of a lease (of rows released or deferred, and of a
lost claim attempt's leases) are the SQL texts of `InboxRepository` run unchanged through
`Database.SqlQueryRaw` / `ExecuteSqlRaw`, their `@name` parameters bound from an anonymous object's properties
(`new { ids, times }`), since `FOR UPDATE SKIP LOCKED`, `UPDATE ... RETURNING` and `INSERT ... ON CONFLICT` have
no LINQ form.

### The record type cache

The only metadata of the org the sync reads is its record types' Ids (Salesforce takes `RecordTypeId` as an Id, which
differs between orgs). `RecordTypeCache` reads them with one SOQL query per object that needs a record type (only Account)
(`SELECT Id, DeveloperName, IsActive FROM RecordType WHERE SobjectType IN ('Account')`, rows read into a typed
`RecordTypeRow` by `SelectAsync<TRow>`) and keeps the active ones in a `HybridCache`
(`Microsoft.Extensions.Caching.Hybrid`), which is two layers:

| Layer | What it holds | Shared by |
|---|---|---|
| In memory | The Ids of an object, read without I/O; one query in flight per object | The pod's worker loops |
| `IDistributedCache` (Redis) | The same entry as JSON | Every pod |

Both layers keep an entry for `Salesforce:RecordTypeRefreshInterval` (15 min). A pod that has none looks in Redis first,
so the org is asked about once per interval in total rather than once per pod, and a new pod starts warm.

- **Key**: `salesforce-sync:recordtypes:{org host}:{API version}:{object, lower case}`, e.g.
  `salesforce-sync:recordtypes:acme.my.salesforce.com:v62.0:account`. Different orgs and API versions never share one.
- **Invalidation**: when a developer name is missing, or Salesforce rejects a `RecordTypeId` (`INVALID_RECORD_TYPE`), the
  object's entry is removed (`RemoveAsync`: from Redis and from this pod's memory), so the next batch reads it again.
  Other pods drop their in-memory copy when it expires, at most one interval later.
- **Failures**: Redis only saves calls. `HybridCache` logs a failed read and treats it as a miss, and logs a failed write
  without throwing, so syncing never waits for or fails because of Redis.
- **Configuration**: `ConnectionStrings:redis` (Aspire injects it; elsewhere set `ConnectionStrings__redis`, e.g.
  `redis:6379`). Without it the second layer is an in-memory cache of the process (`AddDistributedMemoryCache`), so
  tests, benchmarks and single-process runs need no Redis.

Two pods that miss at the same moment may both query once; that costs a call, never correctness.

Why a `HybridCache` and not an `IDistributedCache` alone, with the pros and cons of each:
[docs/caching](docs/caching/README.md).

### Why the sync does not call Describe

The sync used to read each object's Describe (cached per process and shared through Redis, revalidated with
`If-Modified-Since`). Caching Describe is what Salesforce recommends when metadata is needed, but the sync can do without
it, and the schema types and the rules built on them were a lot of code for little: Salesforce checks every
write itself, so the sync now sends what the mapping says and reads Salesforce's answer.

| | With Describe | Without |
|---|---|---|
| Unknown field | Found before any call, from the cached metadata | Found by the read (`INVALID_FIELD`), before any write; pauses the inbox either way |
| Read-only field | Found by the diff, before any write | Found in the write's answer (`INVALID_FIELD_FOR_INSERT_UPDATE`); the other entities of that write are saved, and the inbox pauses |
| Too long, wrong type, required field emptied, lookup vs foreign key | The unit failed alone before any call | The unit fails alone by Salesforce's answer for its record (a call is spent on it) |
| Number scale, checkbox, id case, multi-select order | Normalized to what Salesforce stores | Compared by the .NET value: a value Salesforce stores differently is written again until the mapping sends it as stored |
| Record type Ids | From the cached Describe | One `RecordType` query per object per interval |
| Calls | One Describe (or 304) per object per interval | One `RecordType` query per object that has one, per interval |
| Moving parts | Cache of the whole schema (in process and Redis), schema types, validation rules | The same two-layer cache, of Ids only |

## Event format

The Kafka topic (`account-changes`, `Kafka:Topic` overrides it) carries **account aggregates**: `meta` and two
snapshots of the account, `before` and `after` the change, each the Account (with its Profile) and its two child
lists, `accountRelationships` and `contacts`. `before` is null or absent for a new account; `after` is null or absent
(or `meta.type` is `deleted`) for a deleted one. **camelCase** JSON with the source system's own field names (nothing
in an event is a Salesforce name):

```json
{ "meta": { "time": "2026-10-14T01:02:05.000Z", "type": "updated" },
  "before": { "externalId": "ACC-000001", "name": "Acme", "profile": { "tier": "Gold" },
              "accountRelationships": [ { "externalId": "REL-1", "relatedAccountExternalId": "ACC-000002", "type": "Parent" } ],
              "contacts": [ { "externalId": "CON-1", "lastName": "Lovelace" }, { "externalId": "CON-2", "lastName": "Hopper" } ] },
  "after":  { "externalId": "ACC-000001", "name": "Acme", "profile": { "tier": "Gold" },
              "accountRelationships": [ { "externalId": "REL-1", "relatedAccountExternalId": "ACC-000002", "type": "Partner" } ],
              "contacts": [ { "externalId": "CON-1", "lastName": "Lovelace" } ] } }
```

**The consumer reads an aggregate once** (`AccountAggregate.TryRead`, pure). Then every entity type takes from it the
events of the entities it changed (`Project` of `AccountIngest`, `AccountRelationshipIngest`, `ContactIngest`) and
stores each in its own inbox (`IAggregateIngest.IngestAsync`), all with the aggregate's `meta.time`. The snapshots
are compared as the typed payloads they are read into (property order doesn't matter, list order does), children
matched by `externalId`. Persisting to the inbox and sending to Salesforce are separate, each in its own folder and namespace: `Persistence/`
(`AccountIngest`, `AccountRelationshipIngest`, `ContactIngest`, each an `IAggregateIngest`)
knows the aggregate and the inbox, nothing of Salesforce; `ToSalesforce/` (`AccountSync`, ... each an `IEntitySync`, and the engine) knows the inbox and Salesforce, nothing of aggregates. They share only
`Sync/`: the events, `EventMeta` and `EventReader` (entity id, meta, validation). `AddInboxSync` registers each ingest as an
`IAggregateIngest` and each sync as an `IEntitySync`, parents first.
`AccountAggregate.TrySplit` runs all the projections and returns their events (tests, benchmarks):

| In the aggregate | Stored |
|---|---|
| the Account part (everything but the two lists, so its Profile too) differs, or `before` is missing | the Account's event, `inserted` if `before` is missing, else `updated` |
| a child in `after` that `before` doesn't have, or has differently | the child's event, as it is in `after`, with `accountExternalId` = the account's key |
| a child of `before` that `after` doesn't have | a delete of the child: `{ "meta": { ..., "type": "deleted" }, "<type>": { "externalId": "..." } }` |
| a child, or the Account, unchanged | nothing: no inbox row, no claim, no Salesforce read |
| `after` missing or null, or `meta.type` `deleted` | a delete of the Account (with its Profile) and of every child of `before`, each in its own inbox |

The example above stores two events: `REL-1` updated (its type changed) and `CON-2` deleted. A child without an
`externalId` can't be matched; it is passed on as a change and dead-lettered by its type's read, never deleted. Which
fields of a record are sent is still the engine's diff against Salesforce, not this comparison: a changed child
writes only the fields that differ from what Salesforce has ([An event is the full record](#an-event-is-the-full-record)).

The per-entity events are what the inboxes store and the syncs read. Each is one entity object next to `meta`:

```json
{ "meta": { "time": "2026-10-14T01:02:03.456Z", "type": "updated" },
  "account": { "externalId": "ACC-000001", "name": "Acme", "rating": "Hot", "industry": "Technology",
               "description": null, "annualRevenue": 1250000.50, "numberOfEmployees": 250, "phone": "+1 555 0100",
               "website": "https://example.com", "isActive": true, "tags": ["B2B", "Retail"],
               "customerSince": "2020-01-15", "lastContactedAt": "2026-10-12T01:02:03Z",
               "profile": { "name": "Acme profile", "tier": "Gold", "score": 87.5, "discountPercent": 12.5,
                            "notes": null, "optIn": true, "startDate": "2020-02-01" } } }
{ "meta": { "time": "2026-10-14T01:02:03.789Z", "type": "inserted" },
  "account": { "externalId": "ACC-000003", "isPerson": true, "firstName": "Ana", "lastName": "Cruz", "phone": "+1 555 0102" } }
{ "meta": { "time": "2026-10-14T01:02:04.000Z", "type": "inserted" },
  "accountRelationship": { "externalId": "REL-1", "accountExternalId": "ACC-000001", "relatedAccountExternalId": "ACC-000002",
                           "name": "Acme / Globex", "type": "Parent", "startDate": "2026-01-31", "isActive": true } }
{ "meta": { "time": "2026-10-14T01:02:05.000Z", "type": "inserted" },
  "contact": { "externalId": "CON-1", "accountExternalId": "ACC-000001", "firstName": "Ada", "lastName": "Lovelace",
               "email": "ada@example.com", "phone": "+1 555 0101" } }
{ "meta": { "time": "2026-10-15T01:02:03.000Z", "type": "deleted" }, "contact": { "externalId": "CON-1" } }
{ "meta": { "time": "2026-10-15T01:02:03.000Z", "type": "deleted" }, "account": { "externalId": "ACC-000001" } }
```

- **Entity type**: decided by the aggregate's part the event comes from: the account part → account,
  `accountRelationships` → accountRelationship, `contacts` → contact. Each type has its own typed event class, its own
  inbox table and its own worker pass.
- **Entity id**: `<object>.externalId`, the record's Salesforce external id (`ExternalId__c`). It is read from the
  deserialized event (`EventReader`) and stored in the inbox row's `entity_id` column.
- **Order**: `meta.time`, an ISO-8601 time with an offset or `Z`. **The Kafka message key is not used, and neither is
  arrival order**: an entity's events may come over any partition, through any consumer, in any order. The event with
  the newest `meta.time` wins; an event that is not newer than what was already synced is not stored. This relies on
  the source's clock. Two events of an entity with the same time: the one stored later overwrites the other's row,
  and is written, unless the first one was already synced (then it changes nothing).
- **Type**: `meta.type`, `"inserted"`, `"updated"` or `"deleted"` (any case), is required on the aggregate and set
  on every event the split stores. `deleted` deletes the record ([Deletes](#deletes)); `inserted` and `updated` are
  the same to the sync: whether a record is created or updated is decided by Salesforce's current state.
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

Other rules (in the syncs, `AccountSync`, `AccountRelationshipSync` and `ContactSync`):

- **account**: `profile.externalId` defaults to the account's `externalId`, and the Profile always belongs to the
  event's account (`AccountId__c` = the account's `externalId`).
- **accountRelationship**: `accountExternalId` and `relatedAccountExternalId` are the two Accounts (by external id,
  written to `AccountId__c` and `RelatedAccountId__c`). An event without both is dead-lettered (`INVALID_EVENT`), as is an event whose value has the wrong
  type for its payload property (e.g. `"numberOfEmployees": "lots"`).
- **contact**: `accountExternalId` is the contact's Account (by external id, written to `AccountId__c`); an event without it is dead-lettered
  (`INVALID_EVENT`), and one without `lastName` (required on Contact) with `REQUIRED_FIELD_MISSING`.

## What an object is made of

An object has two halves that share only its event: persisting an aggregate's events to its inbox, and sending them to
Salesforce. Each half is its own folder and namespace (the build checks that `Persistence` and `ToSalesforce` never
reference each other).

| Part | Where | Knows |
|---|---|---|
| **Event** (`AccountEvent` with `AccountPayload` and `ProfilePayload`, `AccountRelationshipEvent`, `ContactEvent`) and its `EventReader` | `Sync/<Object>Event.cs` | The event as the source system sends it: PascalCase CLR properties bound from camelCase JSON, no attributes, no Salesforce names. The reader says whose an event is and when it happened (`meta`). |
| **Ingest** (`AccountIngest`, `AccountRelationshipIngest`, `ContactIngest`, each an `IAggregateIngest`) | `Persistence/<Object>Ingest.cs` | What an Account aggregate calls for of this type (`Project`) and storing it in the type's inbox. No Salesforce. |
| **Sync** (`AccountSync`, `AccountRelationshipSync`, `ContactSync`, each an `IEntitySync`) | `ToSalesforce/<Object>Sync.cs`, `ToSalesforce/SalesforceRecords.cs` | Salesforce's names (object and fields in the record classes, record type developer names in the sync) and what to send for an event. No aggregates. |

```
raw event JSON ─► AccountEvent { AccountPayload { …, ProfilePayload } }     System.Text.Json (Web defaults)
               ─► AccountSync: the fields as Salesforce names them (a Dictionary per record, one line per field)
               ─► SalesforceCalls.ChangedFields (against what the read found) ─► UpsertAsync / DeleteAsync
```

**Salesforce API names live only in the record classes** (`ToSalesforce/SalesforceRecords.cs`): one class per object
(`AccountRecord`, ...), one property per field the sync writes, the API name as `[JsonPropertyName]` where it differs
from the property's name, the object and its external-id field as `[SObject]`. The syncs fill a record from the event
and send `SalesforceFields.Of(record)`; the Account record type developer names are in `AccountSync`. Pointing the sync at an org
whose fields are named differently (`LLC_BI__Active__c` instead of `Active__c`) means editing those attributes and
nothing else. A typo is found by the integration and unit tests, which run every sync against the mock org (whose
schema, `FakeSchema`, rejects an unknown field or `RecordTypeId` like Salesforce).

## The inbox

Each object type has its own inbox table in Postgres, one row per entity holding its newest event not yet in
Salesforce. The Kafka consumer stores events there; worker loops in every pod claim batches of rows, call Salesforce,
and record the result. While a worker sends an entity, the row is **leased** to it (`next_attempt_at` is set to a time
in the future), so other workers skip it, and nothing is locked or kept open while Salesforce is called. A result is
recorded only on a row still under that lease and still holding the event that was sent, so a newer event that
arrived meanwhile is sent next, never lost.

The whole design, with the reasons for it, is in **[docs/inbox/README.md](docs/inbox/README.md)**:
[how events are stored](docs/inbox/README.md#how-events-are-stored),
[a newer event while an entity is being synced](docs/inbox/README.md#a-newer-event-while-an-entity-is-being-synced),
[how a lease works](docs/inbox/README.md#how-a-lease-works),
[why there is no lock token](docs/inbox/README.md#why-there-is-no-lock-token),
[why the Salesforce calls run outside a transaction](docs/inbox/README.md#why-the-salesforce-calls-run-outside-a-transaction),
[retries and dead letters](docs/inbox/README.md#retries-and-dead-letters),
[unknown outcomes](docs/inbox/README.md#unknown-outcomes),
[parallel processing](docs/inbox/README.md#parallel-processing-what-is-guaranteed) and
[schema and migrations](docs/inbox/README.md#schema-and-migrations).

## Writing to Salesforce

A batch is written with plain, bulk calls: **one sObject Collections upsert per object**
(`PATCH /composite/sobjects/{SObject}/{ExternalIdFieldName}`, parents first), then **one sObject Collections delete** by
Id for the records to delete (`DELETE /composite/sobjects?ids=...`, [Deletes](#deletes)). Each is one call and takes up
to 200 records; there is no Composite request. The steps, in the order a type's send step runs them (`ToSalesforce/AccountSync.cs`
reads like this, top to bottom; the calls are in `SalesforceCalls`):

1. **Read** (`ReadAsync`, with `desired.FieldNames()` for the records a type writes): one query per object, by external id (up to 200 ids a query), for the fields the type
   sends, the external id and the Id (a delete needs it).
3. **Decide** (`ChangedFields`, applied by `UpsertChangedAsync`; `WithParentsInSalesforce`; and the type's own rules): each record becomes a create (all its non-empty fields), an update
   (only the fields that differ, values compared in their .NET type's canonical form, `FieldValues`), a delete (by the Id
   the read found) or nothing. An entity with nothing to send is done; one whose parent is in neither Salesforce nor
   the entity waits (`DEPENDENCY_NOT_SYNCED`). Accounts the read didn't find get the record type
   (`RecordTypeIdAsync`, via `RecordTypeCache`); a new Account whose record type the org doesn't have fails here, alone
   (`INVALID_RECORD_TYPE`, retried). Everything else Salesforce rejects, it rejects at the write.
3. **Write** (`UpsertAsync`, `DeleteAsync` of the records `Existing` finds): one upsert per object, each after the objects its records point at (an
   entity's record is not sent once an earlier record of the entity failed), then the deletes (200 records a call at
   most). Each record's answer says saved, retry or dead-letter (`SalesforceErrors`); an entity's outcome is its worst
   record's.

**The external id in an upsert: the field's name once, each record's value.** In
`PATCH /composite/sobjects/{SObject}/{ExternalIdField}` the last part is the **name** of the field the records are matched
by (e.g. `ExternalId__c`), the same for the whole call, so one call holds records of one object matched by one field. Each
record carries its own **value** of that field in its body (`{"attributes":{"type":"Account"},"ExternalId__c":"A1","Name":"Acme"}`),
and Salesforce updates the record with that value or creates it. Each object can have its own external-id field, so each
record class declares its object with its own (`[SObject("Account", "ExternalId__c")]`, read as
`SalesforceObject.Of<AccountRecord>()`, e.g. `AccountSync.AccountObject`); the
sample uses `ExternalId__c` on every object, and the relationship and contact syncs read their parent Accounts with
`AccountSync.AccountObject`.

### Foreign keys: Text fields holding the parent's external id

In this org every foreign key is a **Text field holding the parent's external id** (the source system's key), not a
Lookup field holding a Salesforce Id:

| Object | Foreign key (Text) | Holds |
|---|---|---|
| `Profile__c` | `AccountId__c` | the Account's `ExternalId__c` (the account key) |
| `Account_Relationship__c` | `AccountId__c`, `RelatedAccountId__c` | the two Accounts' `ExternalId__c` |
| `Contact` | `AccountId__c` | the Account's `ExternalId__c` (the standard `AccountId` lookup is left alone) |

For the read, the diff and the write it is an ordinary string field: compared with the event's value, written as the
text when it differs (an unchanged key is not sent), never resolved to an Id, never a `@{reference}`. What a type does
with it is two things only:

- **Order.** The org's automation rejects a child whose parent doesn't exist yet, so an object is upserted after the
  objects its records point at: `AccountSync` upserts the Accounts, then the Profiles carrying `AccountId__c` = the
  account key. The order is of objects, not of records: it is the same for every entity of the batch.
- **Parent check, when the parent comes from another inbox** (a relationship's two Accounts, a contact's Account).
  The type reads the parent's object by external id (`SELECT Id, ExternalId__c FROM Account WHERE ExternalId__c IN (...)`)
  only to know it exists. A parent that isn't there makes the entity wait (`DEPENDENCY_NOT_SYNCED`) with nothing sent.
  When the parent is written by the same entity (a Profile's Account), no query is needed: the parent's upsert goes first.

**A child whose parent failed is not sent.** The calls are in order, so the Profiles' upsert comes after the Accounts'
answer is known. An entity whose Account failed is left out of the Profiles' upsert, and its outcome is the Account's
error (a validation error dead-letters it, a row lock retries it); the other entities' Profiles are sent. If the org
rejects a child whose parent is missing in whatever words `Salesforce:ParentMissingMessages` lists, that rejection means
`DEPENDENCY_NOT_SYNCED`.

**Reads per example batch:** accounts: `Account` and `Profile__c` by external id (the Profiles' Accounts are in the
batch); relationships: `Account_Relationship__c` and `Account` (the parents' existence); contacts: `Contact` and
`Account` (the same).

**Query size.** Each query is a `GET .../query?q=<SOQL>`, so its URL is bounded: Salesforce allows 16,384 bytes for a
call's URI and headers together (414 / 431 over it). The client keeps 4,096 of them for the headers
(`SalesforceRestClient.HeaderAllowanceBytes`: the bearer token, Host, User-Agent and the rest) and holds every URL it
sends to the remaining 12,288 bytes (`MaxUrlBytes`), counted as sent, after percent-encoding (`'`, `,` and spaces take 3
bytes, a non-ASCII character up to 12). `SalesforceCalls.ReadAsync` puts up to 200 external ids in a
query and halves a query while its URL doesn't fit. Short ids give one query per object; 100 ids of 250 characters take 3
queries per object. A query that doesn't fit even with one id (an absurd field list) fails the batch with a message
naming the object and field.

### One call per object

Every batch is read and diffed first; only then is anything written. Every write is an **sObject Collections upsert**
by external id with exactly the planned fields, one call per object (≤ 200 records, Salesforce's limit; more records of one object
take further calls), parents first. Records to
delete go last in one Collections delete by Id ([Deletes](#deletes)).

The calls are `SalesforceCalls` and `SalesforceRestClient`, which know no object by name: a new sync writing, say,
Accounts and their Addresses uses them as they are; it only decides which object goes first.

| Batch | Calls |
|---|---|
| new Accounts with their new Profiles | 2 reads (Accounts, Profiles) + 2 upserts: Accounts', then Profiles' (`AccountId__c` = the account key) |
| Account-only changes, ≤180 | 2 reads + 1 upsert |
| Account changes + Profile changes | 2 reads + 2 upserts, Accounts first |
| new relationships or contacts whose Accounts exist | 2 reads (their object, the Accounts) + 1 upsert, the Accounts' keys as text |
| 300 changed Accounts | 2 reads + 2 upserts: 180, then 120 |
| contacts changed and contacts removed | 2 reads + the Contacts' upsert + 1 delete by Id |
| contacts removed only | 2 reads + 1 delete by Id |
| removed contacts that Salesforce doesn't have | the 2 reads only |

An upsert looks like this:

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

Each upsert carries exactly what the diff planned: a new record whole, an existing one only its changed fields (an
upsert accepts a different set of fields per record), each identified by its external id rather than its Id; a foreign
key as the parent's external id. Unchanged entities still write nothing.

**What the Composite request would save.** A Composite request carries up to 5 queries or collections in one call, so
reads of several objects and the Accounts' and Profiles' upserts could share calls. This design doesn't use it, to keep
the client to three calls and the write path to what is listed above. The price is calls: on the benchmark's preloaded
drain (5,000 events, 1 worker, 1 pod) 47 calls against 30 at 100 accounts, and 151 against 78 at 1,000
([benchmarks](docs/benchmarks/README.md#latest-run-plain-crud-no-composite-request)). With live streaming, whose batches
are small, the call counts are about the same. The daily allocation is what pays for the difference.

The per-record answers go through one classification (`Interpret`): an entity with an Account in one upsert
and a Profile in another is saved only if both are, and a Profile rejected in its upsert fails only its own entity,
never the Accounts of the other one. Two answers are particular to an update sent as an upsert, both meaning the record
was deleted after the read: the upsert either created it from the changed fields alone (`"created": true`) or failed
creating it (`REQUIRED_FIELD_MISSING`). Both are retried, not dead-lettered: the retry's read sees what Salesforce now has
and writes the rest.

When a call fails as a whole (the call, not records in it), the batch stops, later calls are not sent, and the failure
applies to every event of the batch: retried with backoff, held if the call's outcome is unknown (the Accounts' upsert's
answer lost: the whole batch is held), the inbox paused if Salesforce is over its API limits. What earlier calls answered is not recorded.
That is safe even though they may have saved their records: every write is matched by external id, so the retry reads
Salesforce again and writes only what still differs, and those entities end `done` without being written twice.

**`Sync:BatchSize`** is 1 to `Sync:MaxEntitiesPerBatch` entities (default 180; the cap defaults to 200). An entity is one record of
an object, so a batch's upsert of one object fits one call (Salesforce's 200 records).

### Record types

Only a new Account needs a record type, and it decides what the Account is: a **business account** or a **person
account** (the org must have Person Accounts enabled). The event says which (`account.isPerson`: true for a person;
false or absent for a business), and `AccountSync` names each by **developer name**:

```csharp
private const string BusinessRecordType = "Business";       // AccountSync
private const string PersonRecordType = "PersonAccount";
```

Each kind takes its own name fields, so the sync sends a business `Name` (`AccountRecord`) and a person `FirstName` and
`LastName` (`PersonAccountRecord`), never both: Salesforce refuses the other kind's fields
(`INVALID_FIELD_FOR_INSERT_UPDATE`, "bad field names on insert/update call") and sets a person's `Name` itself. The
fields both kinds have are in their common base, `AccountFields`. There is no field to send that makes an Account a
person: `IsPersonAccount` is read-only, set by Salesforce from the record type.

Both must match record types in the org (see [step 4](#4-create-the-objects-and-fields)). The engine turns the name
into an Id with one SOQL query on Salesforce's `RecordType`
(`SELECT Id, DeveloperName, IsActive FROM RecordType WHERE SobjectType IN ('Account')`), cached in the process for
`Salesforce:RecordTypeRefreshInterval` (15 minutes; see [The record type cache](#the-record-type-cache)). That is one
extra call per interval per pod, and none while every Account is already in Salesforce.

`AccountFields.RecordTypeId` is `[CreateOnly]`: `SalesforceFields.Of` leaves it out while it is null, so it is not
read, not compared and not sent on update. After the read, each Account Salesforce doesn't have gets its kind's record
type added before the diff (each record type looked up once a batch, and only if the batch creates that kind).

The read also returns each existing Account's `IsPersonAccount`. An event of the other kind than Salesforce has (a
person event for a business account, or the other way round) can't be applied by an update, because changing the
kind is a conversion in Salesforce. That entity is dead-lettered before anything is written (`ACCOUNT_KIND_MISMATCH`,
its Profile not sent either): convert or delete the Account in Salesforce, then replay the event. A record type changed later in Salesforce (by a user) stays as it is. Profiles, relationships and contacts
send none; Salesforce gives them the default record type of the run-as user, if the object has any.

| Problem | Result |
|---|---|
| The developer name of the new Account's kind isn't among Account's active record types | Each new Account fails alone, before any write (its Profile waits for it), with `INVALID_RECORD_TYPE` (the message names the object and developer name). The object's cached record types are dropped, so the next batch reads them again. |
| Salesforce rejects the `RecordTypeId` at write time (`INVALID_CROSS_REFERENCE_KEY` on `RecordTypeId`: unassigned to the run-as user, or deactivated after its Id was read) | Recorded as the same `INVALID_RECORD_TYPE`, record types dropped. |

**Why a query on `RecordType`.** It is the one piece of metadata the sync needs, and it is a plain SOQL query: no
Describe, no `If-Modified-Since`, no shared cache. It does not say whether a record type is *available* to the run-as
user; Salesforce's rejection of the Id at write time covers that.

`INVALID_RECORD_TYPE` is transient (it is not in `Salesforce:PermanentErrorCodes`): it is retried with backoff, so
events go through once an admin has created, activated or assigned the record type, and dead-lettered only after
`Sync:MaxRetries`.

### Create or update: decided by what Salesforce has

Which verb a record gets is decided by the read alone; the event's `meta.type` is not consulted:

| In Salesforce | What happens |
|---|---|
| no | **create** from the full record |
| yes | **update** with the fields that differ; nothing differs → no write (`done`) |

(Every write is [a Collections upsert](#one-call-per-object): "create" and "update" are what the upsert does by
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

- **More API calls per batch**: a query per object reads before the writes.
- **Value handling by type.** `FieldValues` puts each value in a canonical form by its .NET type (empty text as
  `null`, numbers as decimals, date/time to the second, time to the millisecond) and compares it with the JSON
  Salesforce returned. It does not know a field's scale, length or picklist order, so a value Salesforce stores
  differently is written again on every event until the mapping sends it as stored; the sandbox checks under
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

`allOrNone: false` is sent explicitly on every upsert and delete: every record is saved or rejected on its own. A record
with a validation error fails alone; the other 99 entities of the batch are saved in the same call. A record that fails
without any error details, or has no answer, spends a retry and backs off (`UNKNOWN_ERROR`), so a call can't be repeated
forever.

**Not atomic.** An Account can be saved while its Profile is rejected (or the reverse). The entity is then not
done: it is retried (or dead-lettered, by the failing record's error), the retry's read finds the record that is
already in and writes only what is missing, and **the entity is done only when every record of it has
succeeded**. Until then Salesforce can show the new Account with the old or no Profile.

### Per-record results → what happens to the entity

| Records of the entity | Entity |
|---|---|
| all saved, or nothing to change | `done` |
| any record with a permanent error (`Salesforce:PermanentErrorCodes`) | dead-lettered at once, alone |
| any record with another error (row lock, `DUPLICATE_VALUE` on create, parent missing, unknown code) | retried with backoff |
| a record Salesforce never answered for, or answered without error details | retried with backoff (`UNKNOWN_ERROR`) |
| a record not sent because an earlier call of its entity failed | the earlier failure decides the entity |
| an update [sent as an upsert](#one-call-per-object) that created the record, or failed creating it with `REQUIRED_FIELD_MISSING` (deleted after the read) | retried with backoff; the retry reads it again |
| the call itself failed (any call of the batch) | every entity of the batch: over the API limits → the inbox pauses; provably not applied → retried; otherwise [unknown outcome](docs/inbox/README.md#unknown-outcomes) → held |
| a delete the read found nothing for (never synced, or deleted already) | `done`, no call |
| a delete answered `ENTITY_IS_DELETED`, `INVALID_CROSS_REFERENCE_KEY` or `NOT_FOUND` (deleted after the read) | `done`: already deleted is success |
| a delete refused because records still name it (`DELETE_FAILED` "... associated with ...", an Account whose children's deletes aren't synced yet) | `DEPENDENCY_NOT_SYNCED`, retried with backoff |
| a delete refused otherwise (`DELETE_FAILED`, `INSUFFICIENT_ACCESS_OR_READONLY`, ... or a row lock) | by its code, like any write: permanent → dead-lettered, else retried |

### Deletes

A record is deleted when it is gone from its account's aggregate: a relationship or contact that `before` has and
`after` doesn't, or every record of an account whose `after` is missing (or whose `meta.type` is `deleted`)
([Event format](#event-format)). The split stores a `deleted` event for it in its own inbox, and from there it is an
event like any other: one row per entity, ordered by `meta.time`, claimed, leased, retried, held and dead-lettered
by the same code. Each sync's send step decides the action: for a `deleted` event it deletes the records the read found
(`SalesforceCalls.DeleteAsync`), otherwise it upserts them; the flow has no delete path of its own.

**What Salesforce documents.** The [REST API Developer Guide](https://resources.docs.salesforce.com/latest/latest/en-us/sfdc/pdf/api_rest.pdf)
and the [SOAP API Developer Guide](https://resources.docs.salesforce.com/latest/latest/en-us/sfdc/pdf/apex_api.pdf)
(both version 68.0, Winter '27, as PDFs: the developer.salesforce.com pages refuse automated reads; the HTML of the
same chapters is linked where it is cited):

- [Delete Records Using sObject Collections](https://developer.salesforce.com/docs/platform/api-rest/guide/resources-composite-sobjects-collections-delete.html):
  `DELETE /services/data/vXX.X/composite/sobjects?ids=<id>,<id>&allOrNone=false`, up to **200 Ids**, of any objects,
  **in the URL**, no body. `allOrNone` defaults to false. A well-formed request answers **`200 OK`** with one
  DeleteResult per Id (`id`, `success`, `errors` of `statusCode`, `message`, `fields`), "in the order in which the IDs
  ... were specified"; a request that isn't well formed `400`. A **malformed Id fails alone**, as one of the results,
  without an `id` (`MALFORMED_ID`, the guide's example). With `allOrNone=true` the answer is still `200`, the other
  records failing with `ALL_OR_NONE_OPERATION_ROLLED_BACK`.
- [Delete by external id](https://developer.salesforce.com/docs/platform/api-rest/guide/resources-sobject-upsert-delete.html)
  (`DELETE /sobjects/{SObject}/{ExternalIdField}/{value}`) and
  [by Id](https://developer.salesforce.com/docs/platform/api-rest/guide/resources-sobject-retrieve-delete.html) delete
  **one record per call**. No collection deletes by external id.
- A deleted record goes to the **Recycle Bin** for 15 days and can be undeleted (SOAP API, `emptyRecycleBin()`,
  `undelete()`); [Query](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_query.htm)
  "automatically filter[s] out items that have been deleted", only `queryAll` returns them.
- **Children** (SOAP API, "Referential Integrity" and `undelete()`): a delete takes along the children of a
  relationship whose `cascadeDelete` is true ("deleting an account deletes all the contacts associated with that
  account", by the standard `Contact.AccountId`), and fails if one of them can't be deleted. A Text field is no
  relationship.
- [Considerations for Deleting Contacts](https://help.salesforce.com/s/articleView?language=en_US&id=contacts_del.htm&type=0):
  a contact with cases, contracts, service contracts, assets with cases, a partner user or an active portal user
  can't be deleted.
- Per-record codes (SOAP API, Core Data Types, StatusCode): `ENTITY_IS_DELETED` "You can't reference an object that
  has been deleted", `INVALID_CROSS_REFERENCE_KEY` "A record with the specified ID does not exist", `DELETE_FAILED`
  "You can't delete a record because it is in use by another object", `MALFORMED_ID` "15 characters, or 18 characters
  with a valid case-insensitive extension". Client libraries and API mirrors report an Id deleted already or unknown
  as `ENTITY_IS_DELETED` or `INVALID_CROSS_REFERENCE_KEY` (or `DELETE_FAILED` "entity is deleted")
  ([active_force #119](https://github.com/Beyond-Finance/active_force/pull/119),
  [a mirror of the reference](https://www.withone.ai/knowledge/salesforce/conn_mod_def::GLcnp-sUxIg::r49y34cKRYGmW-tXoriZIg)).

**Decisions:**

| Question | Decision | Why |
|---|---|---|
| Which call | One **Collections delete by Id** (`DeleteAsync`) per batch, after the batch's upserts | 1–200 records in one call. The batch's read already finds each record's Id by its external id, so no call is spent on looking it up. Last, so children's upserts and deletes both come after their parents' upserts. |
| `allOrNone` | `false`, in the URL, as on every upsert | One record that can't be deleted fails alone. |
| Size | ≤ 200 Ids, and the URL checked against the 16,384-byte limit like a query's (200 Ids of 18 characters are about 3,900 bytes) | Same margin as the upserts. |
| A record Salesforce doesn't have | No call, `done` | The read decides, as for create or update: never synced, or deleted (in the Recycle Bin, which SOQL doesn't return). |
| Already deleted at the write | Success | Deleted between the read and the write: what was asked for. |
| A refusal | The existing interpreter: `Salesforce:PermanentErrorCodes` (now with `DELETE_FAILED`) dead-letter, the rest retry; a lost answer holds the batch (`UNKNOWN_OUTCOME`), and the next read finds the record or not | No error handling of its own. |
| Order against later events | `meta.time`, as always: an aggregate older than the delete changes nothing, a newer one that has the child again recreates it (an upsert by external id) | The inbox's one-row-per-entity rule. |
| Same client and limits | Every delete goes through `SalesforceRestClient.DeleteAsync`, the same client as the upserts | One call is one API call, whatever it does. |
| New tables, migrations, config | None for deletes. `Kafka:Topic` (one topic) replaces `Kafka:Topics` | A delete is an event in the existing inboxes. |

**A deleted account.** The account's event deletes its Profile and its Account, in that order in one Collections
delete (children before parents), and the split stores a delete for every relationship and contact `before` has,
each in its own inbox. Those inboxes sync on their own, so the Account's delete may come first. In this org the
foreign keys are **Text fields**, so Salesforce knows no relationship: it deletes the Account while children still
name it, and deletes or clears nothing of theirs; every child is deleted by its own event. The mock org does the
same. An org whose automation refuses to delete an Account that records still name (as it refuses a child whose
parent is missing) answers `DELETE_FAILED` "... is associated with the following records: ..." (the mock org with
`FakeSalesforce:RefuseDeletingReferencedRecords`, set by the integration tests), and the account is retried as
`DEPENDENCY_NOT_SYNCED`, the existing backoff, until its children are gone. The Profile, deleted in the same call,
isn't found by the retry's read and costs nothing.

**What Salesforce does to children by itself.** Nothing, with Text keys. With real relationship fields: a child whose
relationship cascades is deleted with its parent (documented above; standard Contacts linked by `AccountId` are, and
the mock org models that one); for a custom **lookup**, the help pages couldn't be read automatically, so from
[secondary](https://www.asagarwal.com/?p=29136) [sources](https://www.xappex.com/glossary/master-detail-relationship-salesforce/):
it is cleared by default, or blocks the parent's delete if set to restrict; a **master-detail** child is deleted
with its master. A cascade would make the children's own deletes find nothing (no call); a restrict would refuse the
Account like the automation above.

**Not touched:** a relationship in **another** account's aggregate whose `relatedAccountExternalId` is the deleted
account. It belongs to that aggregate, which hasn't removed it. In an org that refuses to delete an Account still
named by records, that relationship keeps the Account's delete waiting: retried as `DEPENDENCY_NOT_SYNCED` until
`Sync:MaxRetries`, then dead-lettered, until the other aggregate removes it.

**Confirmed from the guides** (above): the call, its limits and its `200` per-Id answer in Id order, a malformed Id
failing alone, `allOrNone=true`, the Recycle Bin left
out of queries, what cascades.

**Unconfirmed** (not checked in a real org; the mock org does what is said here):

- which code a delete answers for an Id deleted already or one that never existed (the mock org:
  `ENTITY_IS_DELETED`, `INVALID_CROSS_REFERENCE_KEY`; the sync treats every code above as success);
- that the Ids of one Collections delete are **processed** in the order sent (the guide promises the order of the
  results only; the Profile before its Account); if not, the Account is retried and deleted on the next round;
- the same Id twice in one delete (the sync never sends that; the mock org answers the second `ENTITY_IS_DELETED`);
- the code for more than 200 Ids or records (the mock org: `EXCEEDED_ID_LIMIT`, the SOAP API's code for too many Ids,
  "record limit reached" as client libraries report it); the sync never sends more than 180;
- the wording of the "still referenced" refusal, in an org whose automation refuses it: `Salesforce:ParentMissingMessages`
  decides for creates; for deletes `DELETE_FAILED` with "associated with" means wait. Another contact refusal with
  those words (a contact with cases) also waits, until `Sync:MaxRetries`;
- that upserting the external id of a record in the Recycle Bin creates a new record rather than failing with
  `DUPLICATE_VALUE` because of the deleted one (no guide says; the mock org creates it, and a `DUPLICATE_VALUE` would
  be retried, not lost). An upsert matches only one record, so it can't update the deleted one.

### Lookup fields are not supported

This org links records by **Text foreign keys**, so the engine has no Lookup support: no resolving a parent to its
Salesforce Id, and no Composite `referenceId` or `@{Account[0].id}` to link a new child to its new parent in the same
request. A Lookup field (a reference holding an Id) is refused by Salesforce when a foreign key's text is written into it
(a malformed Id). An org that needs real Lookups would resolve the parent's Id from
the read, and write the parent's object before the child's, as it does for foreign keys; a new parent's Id would then
come from the parent's upsert answer rather than from a reference. That is not built.

## Adding a Salesforce object

The flow (`SalesforceBatchSyncService`) owns claiming, the lease deadline, parsing, and the way a failed batch is held,
deferred, paused or retried; `SalesforceCalls` has the Salesforce calls a send step is made of. Nothing in them, the
consumer or the worker loops names an object. An object supplies:

- its **event** and payload classes (`Sync/<Object>Event.cs`) with an `EventReader` (which property is the entity id, which
  is the meta);
- its **ingest** (`Persistence/<Object>Ingest.cs`, an `IAggregateIngest`): `Project`, the events of this type an aggregate
  calls for (a child type reads its own list of the snapshot), stored in its inbox;
- its **record class** (`ToSalesforce/SalesforceRecords.cs`): `[SObject(name, external-id field)]` and one property per
  field the sync writes, named by its API name (`[JsonPropertyName]` where it differs);
- its **sync** (`ToSalesforce/<Object>Sync.cs`, an `IEntitySync`): a `Send` step, which maps the events to the record
  class (`SalesforceFields.Of(record)` gives the fields) and does the read, the decision and the write with `SalesforceCalls`; the flow runs it
  (`batch.SyncBatchAsync(Event.Reader, inbox, Send, ct)`);
- an **inbox row class** (which selects the object's inbox table) in `SalesforceSync.Data/Inbox/`, and its EF migration.

`AddInboxSync` registers each ingest as an `IAggregateIngest` and each sync as an `IEntitySync`, parents first:

```csharp
AddEntitySync<AccountRelationshipIngest, AccountRelationshipSync, AccountRelationshipInboxRow>(services);
```

`ToSalesforce/ContactSync.cs` is the smallest complete example (72 lines): it parses nothing itself (the flow did), maps a
contact to its fields, reads the contacts and the Accounts they point at, waits for a missing Account, and upserts or
deletes. A step may do all of this differently (another write order, another API): the flow only needs the entities that
failed, and why.

Once its migration exists, the object's inbox table is created at startup, the consumer stores the events the aggregate
gives its type there, and the worker loops sync it. A record whose parent isn't in Salesforce yet
(`DEPENDENCY_NOT_SYNCED`) is retried with backoff, so related objects can arrive in any order. A new object sends no record
type; give its record class a `[CreateOnly]` `RecordTypeId` and set it in the sync, as `AccountSync` does, if creating
one needs it.

**Why `SemaphoreSlim` isn't used for parallel batches:** it limits concurrency within one process, while
Salesforce's limits are org-wide. Parallelism comes from competing worker loops in every pod pulling from the
shared inbox, kept under Salesforce's concurrent limit by `Sync:Workers` x pods. (The record type cache's `HybridCache` runs one query per
object at a time, so concurrent workers don't all read the record types at once.)

## Repository layout

```
src/
  SalesforceSync/                 The entry project, the worker: the only project that references the other two
                                  (Program, DI wiring):
    Inbox/                        Kafka consumer, worker loops
    Hosting/                      The options validators: one FluentValidation validator per settings section
                                  (Sync, Salesforce, SalesforceLimits, Postgres:Retry), run at startup
                                  through FluentOptionsValidator, so a wrong setting stops the worker with every
                                  problem listed
    Sync/                         What both halves share: the events and their payloads (AccountEvent, ...), EventMeta,
                                  EventReader, EventJson, AccountAggregate (a Kafka aggregate → AggregateChange)
    Persistence/                  An aggregate's events into the inboxes: IAggregateIngest and one ingest per object
                                  (AccountIngest, AccountRelationshipIngest, ContactIngest)
    ToSalesforce/                 The inbox to Salesforce: SalesforceBatchSyncService (the flow), SalesforceCalls (the calls
                                  a send step is made of), IEntitySync and one sync per object (AccountSync,
                                  AccountRelationshipSync, ContactSync: its mapping and send step), the record classes
                                  (SalesforceRecords: each object's API names) and SalesforceFields
  SalesforceSync.Infrastructure/  The Salesforce API calls (references no other project of ours):
    Salesforce/                   The REST client and its request/result types, RecordTypeCache, FieldValues, errors,
                                  the org's limits (SalesforceLimitsOptions)
  SalesforceSync.Data/            Everything that touches Postgres (references no other project of ours):
    (root)                        SyncDbContext (the EF model: every table), SyncSchema (migrates at startup),
                                  PostgresRetryStrategy, raw-SQL helpers
    Inbox/                        InboxRow and one row type per object, IRepository (and the claim and outcome
                                  types), InboxRepository (with its SQL), SyncOptions
    Migrations/                   EF migrations (InitialSchema: the tables)
  SalesforceSync.FakeSalesforce/  In-memory Salesforce (OAuth sessions, RecordType and record queries, query pages, sObject
                                  Collections upsert and delete, allOrNone, a Recycle Bin, Salesforce-like value
                                  storage, Text foreign keys checked like the org's automation on write (and, opted
                                  in, on delete), error injection, write history, Sforce-Limit-Info): FakeSalesforceStore
                                  is the org's surface and lock; FakeRecords, FakeSoql, FakeCollectionWriter,
                                  FakeFaults and FakeCounters are its parts
  SalesforceSync.FakeSalesforce.Server/  The in-memory org as an HTTP service (+ /__fake inspection API); the worker
                                  never references the fake: it reaches it, like a real org, at Salesforce:InstanceUrl
  SalesforceSync.LoadGenerator/   Produces account aggregates (before and after, with bursts; children added, changed
                                  and removed) to Kafka
  SalesforceSync.AppHost/         Aspire: Postgres + pgAdmin, Kafka + Kafka UI, mock Salesforce, worker, generator, benchmarks
tests/
  SalesforceSync.Tests/           Unit tests: the flow (with a stand-in inbox), the Salesforce calls and the three syncs
                                  against the in-memory org, the ingests and the aggregate, type normalization, error
                                  classification, execution strategy, limits
  SalesforceSync.IntegrationTests/  End-to-end tests: Postgres + Kafka in Testcontainers, worker pods and the mock org
                                  in-process; Kafka in, assertions on the mock org + inbox
benchmarks/
  SalesforceSync.Benchmarks/      BenchmarkDotNet: real Postgres inbox, the in-memory org as the HTTP handler
sql/
  claim-audit.sql                 Test/benchmark-only trigger that records every entity claim
```

## Run everything with Aspire

Requires Docker (or OrbStack/Colima) and the .NET 10 SDK.

```bash
dotnet run --project src/SalesforceSync.AppHost
```

This uses the `http` launch profile (dashboard at http://localhost:15180). Both profiles set
`ASPIRE_DCP_USE_DEVELOPER_CERTIFICATE=false`: otherwise Aspire's orchestrator exports the ASP.NET Core development
certificate's private key for its own TLS on every start, which on macOS asks for keychain access each time.
`--launch-profile https` serves the dashboard over HTTPS with that certificate, read from its copy on disk
(`dotnet dev-certs https --trust` once; recreate it with `--clean` first if it predates .NET 7).

The dashboard shows:

- **postgres / inboxdb / benchdb** (+ pgAdmin)
- **kafka** (+ Kafka UI)
- **redis** (+ Redis Insight): the record type cache shared by the sync's pods ([The record type cache](#the-record-type-cache))
- **fake-salesforce**: mock Salesforce org over HTTP (OAuth token, query, sObject Collections upsert by
  external id and delete by Id, gzip). Its Text foreign keys (`AccountId__c`,
  `RelatedAccountId__c`) are stored as plain text, and a create or update whose key names an Account that doesn't
  exist is rejected (`FIELD_CUSTOM_VALIDATION_EXCEPTION` "Parent record not found"), as the org's automation would.
  Collections deletes by Id (`DELETE /composite/sobjects?ids=...`) move records
  to a Recycle Bin that queries don't see, and refuse to delete a record still named by another's Text foreign key
  (`DELETE_FAILED` "... is associated with the following records: ..."). Logs its counters every 15s; inspect it at `/__fake/stats`, `/__fake/transport`,
  `/__fake/records/{sobject}[/{externalId}]`, `/__fake/history/{sobject}/{externalId}` and `/__fake/requests` (the
  write requests as received: upserts as `{ method, url, body }` and deletes as `{ method, url }`); inject errors with
  `POST /__fake/faults`
- **salesforce-sync**: consumes the account aggregates of `account-changes`, splits them into their inbox tables
  and syncs to `fake-salesforce` through the real OAuth + REST client
- **load-generator**: 20k account aggregates (before and after, full records) over 500 accounts at 200/s, including
  bursts to the same account; each keeps its account's relationships to other accounts and contacts, adding,
  changing and sometimes removing one (`LoadGenerator:ChildRemovalProbability`, 0.2), which the sync deletes.
  Account and Profile external ids are GUIDs (the same on every run). Every record is built on the event template,
  `templates/account-event.json` (or the file `LoadGenerator:Template` names): a copy of its account, first
  relationship and first contact, where AutoFixture gives every filled-in field a new value on every event (texts 8
  characters), with the ids, links and version markers set (see [Event template](#event-template)).
- **benchmarks**: explicit start, against `benchdb`: **Run quick benchmarks** (about 2 minutes) or **Run full
  benchmarks** (about 6) starts that run; ▶ repeats the last run chosen (quick at first). When a run ends, its console log
  prints the HTML report's link, and **benchmark-reports** (nginx, under benchmarks) serves it: its **Benchmark report**
  link opens `index.html`, the last run's reports first, then earlier ones. Each report says what its benchmark
  measures, what every column means and, for a benchmark that failed (NA in the table), the exception and where the
  whole log is (`drain` stays available from the command line: `--run drain`)

To sync to a Salesforce sandbox instead of the mock org, see [Run against the sandbox](#5-run-against-the-sandbox).

## Tests

Requires the .NET 10 SDK; the integration tests also need **Docker** (Docker Desktop, OrbStack or Colima), nothing
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
names, numbers that are written and read back; documented examples stay
literal), and **Testcontainers** (`Testcontainers.PostgreSql`, `Testcontainers.Kafka`, `Testcontainers.Redis`) for the integration tests.

### Integration tests

Each xUnit collection fixture (`SyncFixture`, `MultiPodFixture`, `FewRetriesFixture`,
`ConcurrentLimitFixture`)
starts its own environment once, shared by the tests in that collection:

- **Postgres** (`postgres:17`) and **Kafka** (KRaft) in containers, removed when the collection finishes. The
  topic is created up front with 6 partitions, like production. `MultiPodFixture` also starts **Redis**
  (`redis:7.4-alpine`), so its pods share their record type cache like replicas in production; the other fixtures'
  pods use the in-memory fallback.
- **The mock Salesforce org**: the `SalesforceSync.FakeSalesforce.Server` app itself (`FakeSalesforceServer.Build`,
  the same code its `Program.cs` runs) on Kestrel at a random loopback port, so the worker goes through real HTTP:
  OAuth token, gzip, the REST client.
- **Worker pods**: each pod is its own in-process `IHost` built with `AddSalesforceSyncWorker()`, the exact
  registrations `Program.cs` uses (options and validation, Npgsql, the Salesforce client,
  auth handler and token provider, `KafkaInboxConsumer`,
  `InboxWorkerService`). A pod has its own connection pool, Kafka consumer (all pods share the consumer group),
  token cache, HTTP stack and worker loops, so
  several pods behave like replicas. Pods read the worker's `appsettings.json` plus the fixture's settings, never
  the environment. Each pod sets `Salesforce:UserAgentInstance` so the mock org can tell them apart.

A fixture shapes the deployment in its constructor: the pod count, `WorkerSettings` that every pod gets (e.g.
`Sync:Workers`, `Sync:BatchSize`, `Sync:Lease`, `Sync:RetryBaseDelay`, `Sync:RequestLimitHold`) and `FakeOrgSettings` for the
org (`FakeSalesforce:Latency`, `LongRunningThreshold`, `ConcurrentLongRunningLimit`, ...). Waits for a result poll
with `Eventually`; the only fixed delays check that nothing more happens. Tests
publish account aggregates to Kafka (`SyncFixture.PublishAggregateAsync`; `PublishAsync`, `PublishRelationshipAsync`
and `PublishContactAsync` publish an aggregate whose only change is that one entity) and assert on the records in
the mock org and the inbox rows' state (waiting, done or dead, read from their columns). `LeaseTests` play a worker by hand
with the worker's own SQL, so a lease lasts exactly as long as the test wants.

Covered: create Account + linked Profile, a full record in which one field changed (only that field is sent), a
field that becomes null or absent being cleared, an event without a profile leaving the Profile alone, the diff
being taken against the record in Salesforce (a direct edit in Salesforce is corrected by the next event),
bursts coalesced to the newest full record, stale versions never written (left out of the row), unchanged events
skip the write, exactly 4 API calls per batch of new Accounts and Profiles (a read of each, then the
Accounts' upsert and the Profiles'), a redelivered event of a synced entity changing nothing and not written, an older event stored after a newer one changing
nothing, A → B → A ending at A, one account's events under different or no Kafka keys
ending at the newest `meta.time`, invalid aggregates (no time offset, no or an unknown `meta.type`, no account, not
JSON) skipped at ingest, an event waiting for its entity's lease, a newer event overwriting the row whatever its
status and an older one leaving it as it is (`OneRowPerEntityTests`), an outcome for a claimed event never recorded on
the newer event that overwrote it meanwhile, an
expired worker unable to record anything, the unknown-outcome hold, every inbox table created at startup with
identical columns, the unique `entity_id` indexes, the partial ready index and autovacuum settings, migrating again changing nothing. Tests
publish version N as `meta.time` = a fixed base time + N ms, and read it back from the stored payload. Relationships
(`RelationshipTests`): one upsert with the two Accounts' keys as text once both exist, later only changed fields
(an unchanged key not sent, a changed one sent); a relationship that arrives before its Accounts waits
(`DEPENDENCY_NOT_SYNCED`, retried) with nothing sent and syncs once they exist; a malformed
relationship event is dead-lettered alone. Contacts (`ContactTests`): upserted with their Account's key in `AccountId__c` once the read
has found the Account, an unchanged contact not written and a changed one sent with only what differs; a contact that arrives
before its Account waits (`DEPENDENCY_NOT_SYNCED`, found by the read) and syncs once it exists; a contact without `lastName` or its
Account refused by Salesforce and dead-lettered alone; a lost upsert answer held, then ending at the newest event. A new Account
gets its `RecordTypeId`; the other objects send none. Pods share their
record type Ids through Redis, so a pod that has read none finds them there (`SharedRecordTypeCacheTests`). Writes are checked with the mock's per-record write history
(`/__fake/history/{sobject}/{externalId}`) and with the requests it received (`/__fake/requests`).

The write itself (`CollectionsWriteTests`): a new Account and Profile as two upserts, the Account's first,
then the Profile's carrying `AccountId__c` = the account key, with `allOrNone: false` and no reference; an Account
saved while its Profile failed being retried, and not done until both are in; a failing Account
dead-lettered alone, its Profile not sent, while another account is saved; `inserted` and
`updated` pending together becoming one create; a later `inserted` for an existing record updating only what differs,
sent as one Collections upsert since only the Account changed; the next change of the Account alone as an upsert of
that field (the unchanged Profile and its key not written); a change of both the Account and its Profile as two
upserts, the Account's first, each with only its changed field. `DeadLetterTests` (`FewRetriesFixture`, `Sync:MaxRetries=3` from configuration): a
relationship whose Account never arrives dead-lettered after `MaxRetries` with `DEPENDENCY_NOT_SYNCED`, then synced
once the Accounts exist and the dead letter is re-queued.

Deletes (`DeleteTests`): a contact removed from its aggregate deleted in Salesforce, an older aggregate that still
has it arriving afterwards leaving it deleted, and a newer one that has it again recreating it; a relationship
removed while a contact is added in the same aggregate; a removed child that was never synced done with no delete
call; a delete refused permanently dead-lettered; a lost delete answer held, then done; a deleted account
(`after` null) removing its Profile, contact and relationship, while the related Account stays.

API limits (`RequestLimitTests`, its own fixture, i.e. its own containers, org and pods):

- **Concurrent long-running limit**: the mock org allows 2 concurrent long-running calls while 12 workers send
  them (nothing caps them across pods). Each `REQUEST_LIMIT_EXCEEDED` pauses its inbox briefly
  (`Sync:RequestLimitHold`), and every event still syncs, with no retries spent.

### Unit tests

Unit tests (`dotnet test tests/SalesforceSync.Tests`, no Docker):

- **The flow** (`SalesforceBatchSyncServiceTests`): nothing claimed when nothing is due; the claimed
  events parsed and handed to the send step; an unreadable row, or one about another entity than its row, dead on its
  own while the others sync; entities done, retried or dead by what the step reports (the permanent error first); a
  write of unknown outcome holding every claimed entity, a mapping error pausing the inbox with the error on the rows,
  Salesforce's API limits (`REQUEST_LIMIT_EXCEEDED`, 429, 503) pausing the inbox without spending an attempt, any other failure retrying all, the step given up on at 80% of the
  lease, and shutdown recorded as nothing.
- **The Salesforce calls** (`SalesforceCallsTests`, against the in-memory org): what to send for a record (a new one's
  non-empty fields, an existing one's differing fields, nothing when it is as wanted, a null clearing a field, values in
  the form Salesforce takes), record types by developer name, a read by external id (200 ids a query, a field the org
  lacks as a mapping error), an upsert creating then updating and 200 records a call, a record's failure on its entity
  and permanent only when the data is wrong, a missing parent (the org's configured message) waited for, a field the org
  lacks as a mapping error, a delete by Id, an already deleted record as success, a delete waiting for records that still
  name it. A failed write is "not applied" only when the failure proves it, else an unknown outcome
  (`WriteOutcomeTests`).
- **The three syncs** (`AccountSyncTests`, `AccountRelationshipSyncTests`, `ContactSyncTests`, a mock org and in-memory
  inboxes): an Account created with its Profile (the Account's key as text, record type on the Account only, tags as a multi-select value), an update that neither sends nor changes the record type,
  a Profile with a key of its own, an event without a profile leaving Salesforce's alone, only the fields that differ sent
  and nothing for an event that changes nothing, a missing field clearing it, deletes (Profile first, an absent record
  already deleted), a failed Account dead and its Profile not sent, a retried failure, an event without its account dead,
  two queries and one upsert per object; relationships and contacts waiting for a missing Account and sent once it is
  there, dead without their Accounts' keys, a contact without its required last name dead, updates with only the
  changed fields, deletes.
- **Also**: the aggregate split and the ingests (`AccountAggregateTests`, `AggregateProjectionTests`: an insert, an
  unchanged aggregate storing nothing, children added, changed, unchanged or removed, a deleted account in its forms,
  invalid messages, a child without an id); what the consumer admits (`InboxEventTests`: the typed read, id, time with
  offset, `meta.type`); every normalization rule (`FieldValuesTests`); error classification, the Postgres execution
  strategy, and startup validation (every registered row type's limits, the org's limits).

The mock
org against the REST and SOAP API guides (`FakeSalesforceTests`): a Collections delete answering 200 per Id in order,
a malformed Id failing alone without an `id`, an Id in the Recycle Bin, one never seen and one with a wrong suffix
told apart, a 15-character Id, `allOrNone=true` rolling back (`ALL_OR_NONE_OPERATION_ROLLED_BACK`), more than 200
Ids or records failing as a whole, a deleted record left out of queries and its external id upserted again as a new
record, an Account's delete cascading to the contacts of the standard lookup but not to those a Text key names, the
opt-in refusal; upsert records sharing an external id all failing, a failed
update without an `id`, an upsert with `allOrNone` true writing nothing; query pages of 2,000 followed by the client
through `nextRecordsUrl`; a call without the current token answered 401 `INVALID_SESSION_ID`, and the client
getting a new token after the session expired. The benchmarks' workload (`AggregateWorkloadTests`, the file
compiled in from `benchmarks/`): its expected end state is what its split aggregates lead to, it removes, re-adds
and deletes, and its check passes an org that got every event in order and catches a stale record and a missed
delete.

### Concurrency: many threads, many pods, same accounts

`ConcurrencyTests` (`MultiPodFixture`) scales the worker like a Kubernetes deployment: **3 pods x 8 worker loops**, all on one
inbox and one org, while 60 accounts each receive 25 updates in waves (new events keep arriving while those
accounts are being processed). Salesforce has nothing that rejects an out-of-order write, so a race would
land in the org and show up in the mock's write history. Batches are small
(10 accounts) so workers contend for the same accounts. The test asserts:

- **No overlapping claims**: `sql/claim-audit.sql` (test-only trigger on the inbox tables) records every claim from the
  moment a worker leases an entity (moves its due `next_attempt_at` to the lease's end) until that lease is ended, ordered by a sequence; no two claims on the
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
`application_name`): every account's row still ends at its newest event, `done`, with no overlapping claims and no out-of-order writes. The fixture allows 15 short Postgres retries (`Postgres:Retry`, at most about 12 s, inside
the 20 s lease), so a unit of work outlasts the chaos instead of giving up and leaving its lease to expire.

## Benchmarks

Only the Salesforce HTTP endpoint is mocked; the Postgres inbox, workers and client are real.

```bash
# use a dedicated database: the benchmark truncates the inbox tables
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

The micro benchmarks need no database: header parsing, the
consumer's typed read of an event (`ReadAndValidateEvent`), and the type-aware
diff.

Both drain benchmarks run P **pods** (independent service providers: own connection pool and
HTTP client, like separate processes) x W worker loops against one Postgres inbox and one org. The input is what
the upstream publishes: `Events` **account aggregates** (`AggregateWorkload`, before and after) over `Accounts`
accounts, each stored through the types' ingests (`IAggregateIngest`) as the consumer does. Every aggregate
changes its Account and Profile; some also add, change or remove one of the account's relationships or contacts (a
removed one may come back later, as a new record), and a tenth of the accounts may be deleted with all their
children, so the drain syncs upserts and deletes of all three objects. Each iteration **fails** if any entity the
aggregates end with isn't in the org at its newest version (a relationship or contact naming the right Accounts),
any removed or deleted one is still there, any record's write history shows an older version after a newer one, any
entity's inbox row doesn't hold its newest event or isn't `done`, or the claim audit shows two overlapping claims
on the same entity. Each iteration prints a `//` line with the mock org's counters (`api`, `writeRequests`,
`recordsOk`, ..., `deletes=N (requests=M)`), the entity events (`entityEvents`, `relationshipEvents`,
`contactEvents`, `deleteEvents`, `deletedEntities`), `entities` (inbox rows), the coalescing (entity events per
entity claim), claims, retries, overlaps, stale rows and regressions.

- `InboxDrainBenchmarks`: the entity events are loaded as storing them in order would leave the inboxes (each
  entity's newest, through a temporary table), then drained. Params: `Pods` (1/3), `Workers` (1/4/8),
  `Accounts` (100/1000), `LatencyMs` (0/25), `Events` (5000 aggregates).
- `StreamingIngestBenchmarks`: aggregates keep arriving, split and stored the way the consumer does
  (`IAggregateIngest.IngestAsync`), while the pods drain (batches of 10 entities), so the same entity is
  claimed, released and re-claimed by different workers in different pods, and its row is overwritten while a worker
  writes the event it claimed, a delete over an upsert or the other way round. Params: `Pods`
  (1/3), `Workers` (4/8), 50 accounts, 5000 aggregates, 5 ms latency.

The workload's expected end state and the check are unit-tested (`AggregateWorkloadTests`) without running the
benchmarks. [docs/benchmarks/README.md](docs/benchmarks/README.md#the-drain-workload) has the workload's counts and the results.

<a id="event-template"></a>**Event template.** The load generator and the benchmarks build every aggregate on one
required file, `templates/account-event.json`: a whole aggregate message as your source sends it. Replace it with a
real event of yours (or pass another file: `LoadGenerator:Template`, benchmarks `--template my.json`). It is read into
the sync's event classes and refused at startup if it doesn't fit them, every problem listed. Each record is a copy
of the template's, where AutoFixture gives every filled-in field a new value; fields the template leaves null stay
null, lists and the fields in `AggregateShape.Kept` (picklists, the contact's email) stay as written, `{key}` replaced
by the entity's id. The structure, which property is each entity's id, foreign key, child list and version marker,
is declared once, as typed properties, in `AggregateShape` (`src/SalesforceSync.LoadGenerator`): change the event
classes and the build breaks there, never a JSON field name typed as text. Details in
[docs/benchmarks/README.md](docs/benchmarks/README.md#using-your-own-aggregate).

```bash
dotnet run -c Release --project benchmarks/SalesforceSync.Benchmarks -- --filter '*StreamingIngest*'
```

## Salesforce API limits

Salesforce limits two things (see "API Request Limits and Allocations" in the
[Salesforce Developer Limits and Allocations Quick Reference](https://developer.salesforce.com/docs/platform/salesforce-app-limits-cheatsheet/guide/salesforce-app-limits-platform-api.html)):
the **total calls per rolling 24 hours** of the org, a soft limit that fails with `REQUEST_LIMIT_EXCEEDED` only once a
system protection limit is hit, and the **concurrent requests that run 20 seconds or longer** (25 for production orgs and
sandboxes, 5 for Developer Edition and trials). It publishes no limit per second, and none on shorter concurrent requests.
What it recommends is to make fewer calls (sObject Collections, composite, Bulk), to watch usage, and to back off when
it refuses. The sync does exactly that and no more: **it doesn't throttle itself.** It makes few calls (one query and
one write per object per batch, of up to 200 records), and when Salesforce refuses, the inbox pauses for a while.

```json
"SalesforceLimits": {
  "ConcurrentLongRunningLimit": 25, // the org's limit: 25 for production and sandboxes, 5 for Developer Edition and trials
  "RequestTimeout": "00:01:30"
},
"Sync": { "RequestLimitHold": "00:01:00" }
```

| Setting | Default | Meaning |
|---|---|---|
| `SalesforceLimits:ConcurrentLongRunningLimit` | 25 | The org's limit of concurrent requests lasting 20 seconds or longer. Startup fails when one pod's `Sync:Workers` exceeds it |
| `SalesforceLimits:RequestTimeout` | 90 s | HTTP client timeout per call; at most 10 minutes, Salesforce's own REST timeout, and shorter than `Sync:Lease` |
| `Sync:RequestLimitHold` | 1 min | How long an inbox pauses when Salesforce refuses for its limits |

| Salesforce limit | What the sync does |
|---|---|
| **Total API requests per 24 hours**, shared by every user and integration of the org | Nothing until Salesforce refuses (403 `REQUEST_LIMIT_EXCEEDED`, `TotalRequests`): then the inbox that got it pauses for `RequestLimitHold`, and tries again after it. Watch usage in Setup (below). |
| **What a call is**: each HTTP request; an sObject Collections request counts as one call however many records it holds | Each HTTP request: a record type query (cached), each query, and each upsert or delete. A worker's batch sends its calls one after another. |
| **Requests per second**: no such limit | Nothing paces calls per second; the workers' own round trips set the rate. |
| **Concurrent long-running requests** (20 s or longer) | Nothing caps them across pods. Each worker has one call in flight, so keep **`Sync:Workers` x pods** well below `ConcurrentLongRunningLimit` (4 workers x 5 pods = 20 of 25). Startup fails when one pod's `Workers` alone exceeds it. If the org does reject one (`REQUEST_LIMIT_EXCEEDED`, `ConcurrentPerOrgLongTxn`), the inbox pauses for `RequestLimitHold`; the rejected call was not applied. |
| **Unavailable**: 503 (maintenance, overloaded), or 429 from something in front of Salesforce | The inbox pauses for `RequestLimitHold`. A 503 that isn't Salesforce's own answer to a write may still have been applied: that batch is held as an [unknown outcome](docs/inbox/README.md#unknown-outcomes) instead. |
| **API timeout**: 10 minutes for REST calls | `RequestTimeout` must be at most 10 minutes, or startup fails. The worker also gives up at 80% of its lease. |

A pause is per inbox (Account, relationship, contact) and seen by every pod, since it is the rows' `next_attempt_at`.
No attempt is spent and nothing is dead-lettered: events wait in the inbox, and newer events of an entity replace older
ones. Each inbox meets the limit on its own next call, so each pauses on its own.

**Watching usage.** The daily allocation is the org's, shared by every integration that calls its API (REST, SOAP,
Bulk and Connect, installed managed packages included). Set up an **API Usage Notification** in Setup (Quick Find:
"API Usage Notifications") at about 80%, and look at **System Overview** for the last 24 hours. If the notification
comes regularly, make fewer calls (a bigger `Sync:BatchSize`, the Bulk API for one-off backfills) or ask Salesforce for
a bigger allocation.

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

Every REST call goes to `{Salesforce:BaseApiPath}/{Salesforce:ApiVersion}` on the instance (default
`/services/data/v62.0`, in `appsettings.json`). Change `ApiVersion` there to move to another API version; versions never share anything.

### 2. Create the user the sync runs as

With the client credentials flow there is no login. Salesforce runs every API call as one user that you choose, so the
sync can do only what that user may do.

1. **Users → New User**. User License **Salesforce Integration**, Profile **Minimum Access - API Only Integrations**.
   Give it a name like "Salesforce Sync" and a real email address you can read. The username must be unique across
   all Salesforce orgs, for example `salesforce-sync@acme.dev-sandbox`.
2. **Permission Sets → New**. Label `Salesforce Sync`, License **Salesforce API Integration**. In **Object Settings**,
   give it **Read, Create and Edit** on Account, Contact, Profile and Account Relationship (the objects of step 4), and
   **Edit** access on every field listed in step 4, and **Delete** on Account, Contact, `Profile__c` and
   `Account_Relationship__c` (records removed from an aggregate are deleted).
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
If Salesforce refuses the name, pick another and change it in `AccountSync` (see [field mappings](#field-mappings)).

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

**Record types.** Account needs the two record types `AccountSync` names (developer names; see
[Record types](#record-types)): `Business` for businesses and `PersonAccount` for people. The other objects need none.
Person Accounts must be enabled in the org (Setup → Account Settings; once on, it can't be turned off): without them
`FirstName`, `LastName` and `IsPersonAccount` don't exist on Account, and every Account read fails with `INVALID_FIELD`.

1. **Object Manager → Account → Record Types → New**, once for each. For businesses: Record Type Label e.g. `Business`,
   **Record Type Name** (the developer name) exactly `Business`. For people: a **Person Account** record type, Record
   Type Name exactly `PersonAccount` (Salesforce creates one with that name when Person Accounts are enabled). Tick
   **Active**. Use names that already exist in your org by changing `BusinessRecordType` and `PersonRecordType` in
   `AccountSync` instead.
2. Assign both to the user the sync runs as: **Permission Sets → Salesforce Sync → Object Settings → Account → Edit**,
   and under **Record Type Assignments** tick them. (A user on a standard profile can have it assigned on
   the profile's **Record Type Settings** instead.)

A record type that is missing, inactive or not assigned fails new Accounts with `INVALID_RECORD_TYPE`, retried
with backoff; they go through once the record type is fixed.

A picklist with **Restrict picklist to the values defined in the value set** ticked rejects any other value. That
event is then dead-lettered (`INVALID_OR_NULL_FOR_RESTRICTED_PICKLIST`). Add every value the source system sends, or
leave the picklist unrestricted.

### Field mappings

A mapping is code, in three places:

1. **Payload** (`Sync/<Object>Event.cs`): the event's JSON fields.
2. **Record** (`ToSalesforce/SalesforceRecords.cs`): the Salesforce object (`[SObject]`: its name and external-id
   field) and the fields the sync writes, each a property whose API name is its own or its `[JsonPropertyName]`. This
   is the only place Salesforce object and field names appear.
3. **Mapping** (the sync's `Send` step, `ToSalesforce/AccountSync.cs` and the others): one line per field, from the
   payload property to the record property, typed. The Account record type developer names are in `AccountSync`.

The mappings as they are:

| Event field | Salesforce field |
|---|---|
| `account.externalId` | `Account.ExternalId__c` |
| `account.isPerson` | Which record type a new Account gets: `PersonAccount` if true, else `Business`; and which name fields are sent |
| `account.name` (a business) | `Account.Name` |
| `account.firstName`, `lastName` (a person) | `Account.FirstName`, `LastName` (Salesforce sets a person's `Name` from them) |
| `account.rating`, `industry`, `description`, `annualRevenue`, `numberOfEmployees`, `phone`, `website` | `Account.Rating`, `Industry`, `Description`, `AnnualRevenue`, `NumberOfEmployees`, `Phone`, `Website` |
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
| (none: the record type developer names in `AccountSync`) | `Account.RecordTypeId`, on create only: the Id of `PersonAccount` (a person) or `Business`, from a query on `RecordType` |

To change a mapping:

- **A field has a different API name in your org** (e.g. `LLC_BI__Active__c`): change its `[JsonPropertyName]` in the
  record class. Same for an object name or its external-id field (`[SObject]`); the Account record type developer names are in
  `AccountSync`.
- **Drop a field**: remove its property from the record class, and the build shows the mapping line to remove. A field
  the sync doesn't know is never touched.
- **Add a field**: add the property to the payload class (the JSON name in camelCase), to the record class (its API
  name), and one line to the sync's mapping.
  Add it to `FakeSchema` (`src/SalesforceSync.FakeSalesforce/FakeSchema.cs`) too: the mock org rejects fields it
  doesn't know, so the tests fail until it is there.
- **A new object**: see [Adding a Salesforce object](#adding-a-salesforce-object). In `FakeSchema`, give it a
  `RecordTypeId` field and its record types in `RecordTypes` if the sync sets one.

Every property of the record class is part of the record's full desired state: an event without a value clears that field in
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
cd ../..
dotnet run --project src/SalesforceSync.AppHost
```

The org's daily API allocation needs no setting: Salesforce reports it in `Sforce-Limit-Info` on every response.
`dotnet user-secrets clear` (in `src/SalesforceSync.AppHost`) goes back to the mock org.

Then check the result in the sandbox, for example in **Developer Console → Query Editor**:

```sql
SELECT ExternalId__c, Name, Description, Active__c, LastModifiedDate FROM Account WHERE ExternalId__c LIKE 'ACC-%'
SELECT ExternalId__c, AccountId__c, Tier__c, Score__c FROM Profile__c
SELECT ExternalId__c, AccountId__c, RelatedAccountId__c, Type__c FROM Account_Relationship__c
SELECT ExternalId__c, AccountId__c, LastName, Email FROM Contact WHERE ExternalId__c LIKE 'CON-%'
```

The `salesforce-sync` logs in the Aspire dashboard show each batch, and pgAdmin shows the inbox tables: every row should
end `done`. A `dead` row's `last_error` says what Salesforce rejected. Delete the test records
afterwards: `ACC-…`, `PRF-…`, `REL-…` and `CON-…`.

To run the worker without Aspire (a server or Kubernetes), give it the same settings as environment variables
(`Salesforce__InstanceUrl`, `Salesforce__ClientId`, `Salesforce__ClientSecret`), plus `ConnectionStrings__inboxdb` and `ConnectionStrings__kafka`, and
`ConnectionStrings__redis` so the replicas share their record type Ids (optional: without it each process reads them on
its own; see [The record type cache](#the-record-type-cache)). With Aspire, Redis needs no setup: the AppHost starts it and
passes the connection string.

### 6. Tune and check

- Keep `Sync:Workers` x the number of pods well below `SalesforceLimits:ConcurrentLongRunningLimit` (25; 5 on
  Developer Edition and trial orgs). See
  [Salesforce API limits](#salesforce-api-limits).
- Tune `Salesforce:PermanentErrorCodes`, `Sync:RetryBaseDelay` / `MaxRetryDelay` / `MaxRetries` and `Postgres:Retry`
  if needed.
- Set `Salesforce:ParentMissingMessages` (a list of case-insensitive substrings, empty by default): a record error
  whose message contains one of them is `DEPENDENCY_NOT_SYNCED` (the record waits for its parent, retried like a
  parent the read didn't find), whatever its code. Find the message your org returns for a child whose Text key names
  a missing parent (below) and add it here, e.g. in `appsettings.json`:
  `"Salesforce": { "ParentMissingMessages": [ "Account not found" ] }` (or `Salesforce__ParentMissingMessages__0`).
  Empty, such a rejection is what its code says (`FIELD_CUSTOM_VALIDATION_EXCEPTION`: permanent, dead-lettered).
- Startup fails on `Sync` values that can't work (no workers, `BatchSize` outside 1 to `MaxEntitiesPerBatch`, ...), and on a missing
  `Salesforce:InstanceUrl`.
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
    and records in one call may carry different fields.

### 7. Clean up: hard-delete records in bulk

`bulk-delete` hard-deletes any number of records of one object with one
[Bulk API 2.0](https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/bulk_api_2_0.htm) job, then
exits (`src/SalesforceSync/Hosting/BulkDeleteCommand.cs`). A hard delete skips the Recycle Bin, so the records' storage
is freed at once (the cure for `STORAGE_LIMIT_EXCEEDED`, [Org storage full](#how-its-solved)),
and **they can't be restored**.

**From the Aspire dashboard** (against the org the AppHost uses: the mock org, or the sandbox of step 5):

1. **list-delete-ids → ▶ Start.** For each object the sync writes (Contact, Account_Relationship__c, Profile__c,
   Account) it runs `SELECT Id, ExternalId__c FROM <object> WHERE ExternalId__c != null` (every record the sync made)
   and writes `bulk-delete/<object>.json` at the repo root (gitignored). It deletes nothing. Its log lists the files.
2. Optionally edit a file: remove the records to keep. Each has its external id, to tell them apart.
3. **bulk-delete → Delete Ids from a JSON file.** Pick the file; the dashboard asks "Hard-delete N Account records?",
   then runs the job. The job's result is in bulk-delete's log. **▶ Start** on bulk-delete deletes the last file
   picked again (harmless: already deleted Ids just fail).

An Ids file (example: [`src/SalesforceSync.AppHost/bulk-delete.example.json`](src/SalesforceSync.AppHost/bulk-delete.example.json)).
`object` is the object to delete from and each record's `id` its Salesforce Id (15 or 18 characters); `query` and
`externalId` are there to tell which records they are, and are not read:

```json
{
  "object": "Account",
  "query": "SELECT Id, ExternalId__c FROM Account WHERE ExternalId__c != null",
  "records": [
    { "id": "001000000000001AAA", "externalId": "ACC-0001" },
    { "id": "001000000000002AAA", "externalId": "ACC-0002" }
  ]
}
```

**From the command line:**

```bash
# The JSON files above: list, then delete one (the file names its object). dotnet run runs in the project's folder,
# so give full paths ($PWD)
dotnet run --project src/SalesforceSync -- bulk-delete-ids "$PWD/bulk-delete"
dotnet run --project src/SalesforceSync -- bulk-delete "$PWD/bulk-delete/Account.json"

# Or a text file: one Id per line (15 or 18 characters). An "Id" header, blank lines and # comments are skipped,
# so a CSV exported from a query (SELECT Id FROM Account WHERE ExternalId__c LIKE 'ACC-%') works as is.
dotnet run --project src/SalesforceSync -- bulk-delete Account "$PWD/ids.txt"
```

Delete children before their Accounts when an org refuses deleting an Account that Text keys still name.

- It uses the worker's Salesforce settings (`Salesforce:InstanceUrl`, `ClientId`, `ClientSecret`, from user secrets or
  `Salesforce__…` environment variables).
- It asks for `yes` before deleting. `--yes` skips the question; without a terminal (a script, CI) `--yes` is required.
- The run user needs **Delete** on the object and the **Bulk API Hard Delete** system permission (Permission Sets →
  System Permissions). Without it the job fails as a whole.
- It prints the job Id, how many records were deleted and each Id that wasn't, with Salesforce's error (e.g.
  `INVALID_CROSS_REFERENCE_KEY` for an Id that doesn't exist or is of another object). Children are deleted as with any
  delete (cascading relationships).
- Exit code: `0` every record deleted, `1` some failed, the job failed or the run was cancelled, `2` bad arguments or a
  line that isn't an Id (nothing is sent).
- Ctrl+C stops waiting, not the job: a job already uploaded keeps running in Salesforce (Setup → Bulk Data Load Jobs).

## Status

Scaffold, not yet production-tested. Verify in a Salesforce sandbox, in particular concurrent workers on
the same account and killing a worker mid-batch.

## License

[MIT](LICENSE)
