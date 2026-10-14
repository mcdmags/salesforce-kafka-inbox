# Benchmark report: inbox sync core (without rate limiting)

> **Note:** this report predates the inbox redesign (one inbox table per object type with an `entity_id` column, no
> SQL reading the payload, typed full-record events, newest-`meta.time` coalescing). The numbers below were measured on
> the earlier single `sf_inbox` table and will be re-run; they are kept unchanged for reference.

The Kafka → Postgres inbox → Salesforce pipeline with the generic batch upsert engine, the raw-JSON inbox and **no** rate limiter or concurrency slots. In-flight batches = pods × workers.

## Environment

```
BenchmarkDotNet v0.14.0, macOS 26.6.2 (25G83) [Darwin 25.6.0]
Apple M4, 1 CPU, 10 logical and 10 physical cores
.NET SDK 10.0.100
Postgres 17 (Docker), fake Salesforce in-process with simulated latency
```

Run with `dotnet run -c Release --project benchmarks/SalesforceSync.Benchmarks -- --filter '*'` against a
dedicated database (`ConnectionStrings__benchdb`).

## What is measured

- **Drain inbox**: 5,000 events are bulk-loaded, then drained by P pods × W worker loops.
- **Drain while ingesting**: the same 5,000 events arrive in per-entity order while the pods drain, so the same
  entity is claimed again and again by different workers. This is the race the head-row claim must prevent.
- About 13% of events are `accountRelationship` changes between accounts in the same run; the rest are `account`
  changes (Account + Profile). A pod is wired with the worker's own registrations and runs the worker's own pass
  over every entity type.
- `LatencyMs` is the simulated Salesforce round trip. Each case is 1 warm-up + 5 measured iterations.

## Correctness (checked after every iteration)

**196 iterations, 0 failures.** Every iteration ended with:

- every Account, Profile and relationship at its newest version, relationships linked to the right two Accounts;
- no entity ever receiving an older version after a newer one (from the org's write history);
- 0 overlapping claims on the same entity (claim audit), 0 failed or halted graphs, every inbox row `done`.

## Drain inbox

| Workers | Pods | Accounts | LatencyMs | Mean | Median | Allocated |
| -------- | ----- | --------- | ---------- | ------------: | ------------: | ----------: |
| 1 | 1 | 100 | 0 | 2,648.5 ms | 2,557.7 ms | 95.96 MB |
| 1 | 1 | 100 | 25 | 2,398.4 ms | 2,139.3 ms | 96.17 MB |
| 1 | 1 | 1000 | 0 | 3,239.6 ms | 2,975.9 ms | 423.13 MB |
| 1 | 1 | 1000 | 25 | 3,534.5 ms | 3,577.6 ms | 418.76 MB |
| 1 | 3 | 100 | 0 | 1,304.9 ms | 1,329.1 ms | 100.48 MB |
| 1 | 3 | 100 | 25 | 1,269.3 ms | 1,226.9 ms | 100.29 MB |
| 1 | 3 | 1000 | 0 | 1,885.6 ms | 1,642.5 ms | 419.27 MB |
| 1 | 3 | 1000 | 25 | 2,044.9 ms | 1,828.2 ms | 421.56 MB |
| 4 | 1 | 100 | 0 | 1,299.7 ms | 1,412.4 ms | 103.75 MB |
| 4 | 1 | 100 | 25 | 1,288.7 ms | 1,274.1 ms | 103.98 MB |
| 4 | 1 | 1000 | 0 | 1,063.2 ms | 914.5 ms | 420.56 MB |
| 4 | 1 | 1000 | 25 | 1,903.8 ms | 2,003.3 ms | 423.08 MB |
| 4 | 3 | 100 | 0 | 2,630.9 ms | 2,268.7 ms | 122.54 MB |
| 4 | 3 | 100 | 25 | 13,000.2 ms | 13,568.4 ms | 111.71 MB |
| 4 | 3 | 1000 | 0 | 1,675.0 ms | 1,985.3 ms | 424.95 MB |
| 4 | 3 | 1000 | 25 | 1,054.5 ms | 1,057.6 ms | 434.22 MB |
| 8 | 1 | 100 | 0 | 1,091.2 ms | 1,085.8 ms | 118.85 MB |
| 8 | 1 | 100 | 25 | 1,045.2 ms | 1,016.9 ms | 115.56 MB |
| 8 | 1 | 1000 | 0 | 984.5 ms | 986.4 ms | 430.31 MB |
| 8 | 1 | 1000 | 25 | 1,877.4 ms | 1,813.2 ms | 425.34 MB |
| 8 | 3 | 100 | 0 | 8,267.6 ms | 8,396.0 ms | 137.19 MB |
| 8 | 3 | 100 | 25 | 21,858.8 ms | 10,545.2 ms | 127.77 MB |
| 8 | 3 | 1000 | 0 | 8,074.9 ms | 2,288.2 ms | 422.63 MB |
| 8 | 3 | 1000 | 25 | 1,537.4 ms | 1,484.7 ms | 429.61 MB |

## Drain while ingesting

| Workers | Pods | Accounts | LatencyMs | Mean | Median | Allocated |
| -------- | ----- | --------- | ---------- | --------: | --------: | ----------: |
| 4 | 1 | 50 | 5 | 8.880 s | 9.239 s | 100.96 MB |
| 4 | 3 | 50 | 5 | 4.686 s | 4.563 s | 135.16 MB |
| 8 | 1 | 50 | 5 | 1.452 s | 1.078 s | 132.86 MB |
| 8 | 3 | 50 | 5 | 6.237 s | 7.016 s | 241.48 MB |

## Per-case behaviour (medians over the iterations)

`API calls` = Salesforce requests (composite read + graph write per batch). `Events/graph` = coalescing.
`Retries` = attempts beyond the first, almost all relationships claimed before their Accounts had synced
(`DEPENDENCY_NOT_SYNCED`, retried with backoff).

| Mode | Workers | Pods | Accounts | LatencyMs | API calls | Events/graph | Retries |
| --- | --- | --- | --- | --- | ---: | ---: | ---: |
| preloaded | 1 | 1 | 100 | 0 | 22 | 7.0 | 55 |
| preloaded | 1 | 1 | 100 | 25 | 22 | 7.0 | 55 |
| preloaded | 1 | 1 | 1000 | 0 | 57 | 3.1 | 618 |
| preloaded | 1 | 1 | 1000 | 25 | 56 | 3.1 | 626 |
| preloaded | 1 | 3 | 100 | 0 | 28 | 7.0 | 299 |
| preloaded | 1 | 3 | 100 | 25 | 28 | 7.0 | 299 |
| preloaded | 1 | 3 | 1000 | 0 | 58 | 3.1 | 481 |
| preloaded | 1 | 3 | 1000 | 25 | 60 | 3.1 | 500 |
| preloaded | 4 | 1 | 100 | 0 | 34 | 7.0 | 587 |
| preloaded | 4 | 1 | 100 | 25 | 34 | 7.0 | 588 |
| preloaded | 4 | 1 | 1000 | 0 | 56 | 3.1 | 511 |
| preloaded | 4 | 1 | 1000 | 25 | 58 | 3.1 | 514 |
| preloaded | 4 | 3 | 100 | 0 | 56 | 7.0 | 1828 |
| preloaded | 4 | 3 | 100 | 25 | 41 | 7.0 | 1064 |
| preloaded | 4 | 3 | 1000 | 0 | 62 | 3.1 | 626 |
| preloaded | 4 | 3 | 1000 | 25 | 68 | 3.1 | 747 |
| preloaded | 8 | 1 | 100 | 0 | 45 | 7.0 | 1667 |
| preloaded | 8 | 1 | 100 | 25 | 45 | 7.0 | 1589 |
| preloaded | 8 | 1 | 1000 | 0 | 61 | 3.1 | 698 |
| preloaded | 8 | 1 | 1000 | 25 | 58 | 3.1 | 563 |
| preloaded | 8 | 3 | 100 | 0 | 59 | 7.0 | 2000 |
| preloaded | 8 | 3 | 100 | 25 | 61 | 7.0 | 1948 |
| preloaded | 8 | 3 | 1000 | 0 | 57 | 3.1 | 714 |
| preloaded | 8 | 3 | 1000 | 25 | 65 | 3.1 | 854 |
| streaming | 4 | 1 | 50 | 5 | 161 | 6.4 | 20 |
| streaming | 4 | 3 | 50 | 5 | 500 | 3.3 | 12 |
| streaming | 8 | 1 | 50 | 5 | 410 | 3.2 | 23 |
| streaming | 8 | 3 | 50 | 5 | 1257 | 1.9 | 11 |

## Micro benchmarks

| Method | Mean | Median | Allocated |
| ------------------ | ------------: | ------------: | ----------: |
| CoalesceTenEvents | 16,250.4 ns | 16,202.5 ns | 25752 B |
| DiffNumber | 261.2 ns | 242.4 ns | 32 B |
| DiffDateTime | 486.6 ns | 476.2 ns | 192 B |

## Findings

1. **Correct under every combination.** No overlap, lost update or out-of-order write in 196 iterations.
2. **Few Salesforce calls.** A drain of 5,000 events costs tens of API calls, because each batch is one composite
   read plus one Composite Graph write and events for the same entity are merged.
3. **Time is dominated by Postgres claiming, not Salesforce.** Simulated latency of 0 vs 25 ms barely moves the
   result, and the slowest iterations made the same ~60 API calls as the fastest.
4. **Large run-to-run variance** (some means are several times the median). The cause is the claim query on the
   raw-JSON inbox, see below.
5. **More workers do not help a small, hot key set.** With 100 accounts, 3 pods × 8 workers is slower than
   1 pod × 4 workers: more claims compete for the same entities, coalescing drops and relationships retry more.
6. **Allocations scale with distinct entities** (about 96 MB at 100 accounts, about 420 MB at 1,000), from the
   per-entity diff and graph payloads.

## Known issue: claim cost on the raw-JSON inbox

`sf_inbox` stores only the raw event; type, key and order are computed from the JSON (`sf_event_type`,
`sf_event_key`, `sf_event_version`). One claim (`InboxSql.LockHeads`, 75 entities) measured on 5,000 pending rows
(`EXPLAIN ANALYZE`, Postgres 17):

| Data | Claim time |
| --- | ---: |
| 100 keys, right after a bulk load (no `ANALYZE`) | 58 ms |
| 100 keys, analyzed | 826 ms |
| 1,000 keys, right after a bulk load | 83 ms |
| 1,000 keys, analyzed | 159 ms |
| Earlier column-based inbox, for reference | about 0.6 ms |

Why: the "is there an older pending event for this entity" check compares event times for every pair of rows of
the same entity, each a JSON extraction and timestamp parse, and the claim orders by `received_at`, which no
index covers, so all candidates are sorted before the limit applies. Bulk-loaded rows (as in this benchmark) share
one `next_attempt_at`, the worst case for that sort.

This affects throughput, not correctness. **Follow-up:** rewrite the claim as a head-row lookup ordered by
`(next_attempt_at, id)` with expression indexes that cover it; a first prototype measured 1.1 to 60 ms on the same
data. These numbers should be re-run after that change.
