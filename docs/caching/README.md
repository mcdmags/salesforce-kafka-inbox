# Why the record type cache is a HybridCache

`RecordTypeCache` (`src/SalesforceSync.Infrastructure/Salesforce/RecordTypeCache.cs`) keeps the Ids of the org's
record types: one SOQL query on `RecordType` per object, kept for `Salesforce:RecordTypeRefreshInterval` (15 minutes).
How it works (key, invalidation, failures, configuration) is in the main README,
[The record type cache](../../README.md#the-record-type-cache). This page explains the choice of cache.

It needs two things:

- **Fast reads.** Every batch that creates an Account looks up a record type.
- **Sharing between pods.** What one pod read, the others can use, so the org isn't asked once per pod.

`HybridCache` (`Microsoft.Extensions.Caching.Hybrid`) gives both: a cache in the process's memory in front of an
`IDistributedCache` (Redis). An `IDistributedCache` used on its own gives only the sharing.

## HybridCache against IDistributedCache alone

| | `HybridCache` | `IDistributedCache` alone |
|---|---|---|
| Reading a cached entry | From memory, no I/O | A Redis round trip on every read (about 0.5–1 ms), which can fail |
| Many workers miss at once (start, expiry) | One query per key in the process; the others wait for it | Every worker that misses queries Salesforce, unless you write the locking yourself |
| Turning the entry into bytes and back | Done for you (System.Text.Json) | Your code: `byte[]` in, `byte[]` out |
| Redis down | Logged and treated as a miss; the memory layer keeps working | Every call throws; your code catches and falls back |
| Code | One `GetOrCreateAsync(key, factory, options)` | Get, check for null, run the query, serialize, Set |
| Shared by every pod | Yes, through the distributed layer | Yes |

## What HybridCache costs

- **A removal doesn't reach other pods' memory.** `RemoveAsync` clears Redis and this pod's memory; every other pod
  keeps its in-memory copy until it expires (`LocalCacheExpiration`, 15 minutes here). With `IDistributedCache` alone
  there is one copy, so a removal is seen everywhere at once. For record types that is fine: a stale Id is refused by
  Salesforce (`INVALID_RECORD_TYPE`), the entity is retried, and the pod that saw the refusal reads the record types
  again.
- **The "one query at a time" holds per process, not across pods.** Three pods that miss at the same moment make three
  queries.
- **Mutable values are copied on every read.** HybridCache reuses the cached instance only for immutable types. The
  entry here is an array (`RecordTypeRow[]`), so each read deserializes a new one: a few small objects per batch.
- **One more package** to know.

## How much the shared layer saves

Little. Without Redis, each pod would query `RecordType` once per 15 minutes: a handful of API calls a day. Most of
what HybridCache brings here is the memory layer (no I/O per batch) and one query per process when it starts or the
entry expires. Sharing through Redis is a small extra; it matters most when many pods start together. Without
`ConnectionStrings:redis` the distributed layer is an in-memory one of the process (`AddDistributedMemoryCache`), and
the cache works the same, per pod.

## Which to choose

| Data | Choose |
|---|---|
| Small, read often, changes rarely, a stale copy is harmless or self-correcting (record type Ids, settings read from the org) | `HybridCache`, or `IMemoryCache` alone if pods needn't share |
| Every pod must see a change or removal at once (a revoked session, a feature flag) | `IDistributedCache` directly, or `HybridCache` with a short `LocalCacheExpiration` |
| Large values, or values too costly to copy on each read | `HybridCache` with immutable types (sealed, init-only, marked `[ImmutableObject(true)]`), or `IMemoryCache` |
