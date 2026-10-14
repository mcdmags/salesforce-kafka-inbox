# Benchmark report: rate limiting and concurrency slots

> **Note:** this report predates the inbox redesign (one inbox table per object type with an `entity_id` column, no
> SQL reading the payload, typed full-record events, newest-`meta.time` coalescing). The numbers below were measured on
> the earlier single `sf_inbox` table and will be re-run; they are kept unchanged for reference.

The same pipeline as `inbox-sync-core.md` **plus** the Salesforce rate limiter (Postgres token bucket, daily budget, `Sforce-Limit-Info`) and the global concurrency slots (Postgres advisory locks). The bucket is set to effectively unlimited and the daily budget is off, so the numbers show the limiter's overhead (one token acquire per Salesforce call, one slot per batch), not throttling. The slot cap is workers × pods.

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
| -------- | ----- | --------- | ---------- | ---------: | ---------: | ----------: |
| 1 | 1 | 100 | 0 | 1.558 s | 1.5473 s | 96.16 MB |
| 1 | 1 | 100 | 25 | 2.566 s | 2.2943 s | 95.62 MB |
| 1 | 1 | 1000 | 0 | 1.774 s | 1.7128 s | 415.73 MB |
| 1 | 1 | 1000 | 25 | 4.378 s | 4.2555 s | 421.85 MB |
| 1 | 3 | 100 | 0 | 1.005 s | 0.8705 s | 101.27 MB |
| 1 | 3 | 100 | 25 | 1.017 s | 0.9860 s | 101.51 MB |
| 1 | 3 | 1000 | 0 | 15.663 s | 1.2099 s | 423.66 MB |
| 1 | 3 | 1000 | 25 | 1.549 s | 1.5163 s | 421.99 MB |
| 4 | 1 | 100 | 0 | 1.066 s | 1.0470 s | 100.79 MB |
| 4 | 1 | 100 | 25 | 1.210 s | 1.1429 s | 101.67 MB |
| 4 | 1 | 1000 | 0 | 1.115 s | 1.1814 s | 423.27 MB |
| 4 | 1 | 1000 | 25 | 1.418 s | 1.4220 s | 422.74 MB |
| 4 | 3 | 100 | 0 | 2.563 s | 2.6743 s | 117.31 MB |
| 4 | 3 | 100 | 25 | 2.996 s | 2.9510 s | 113.98 MB |
| 4 | 3 | 1000 | 0 | 1.441 s | 1.3620 s | 429.08 MB |
| 4 | 3 | 1000 | 25 | 1.429 s | 1.3808 s | 429.7 MB |
| 8 | 1 | 100 | 0 | 1.814 s | 1.7281 s | 119.92 MB |
| 8 | 1 | 100 | 25 | 1.583 s | 1.5575 s | 118.76 MB |
| 8 | 1 | 1000 | 0 | 1.195 s | 1.1751 s | 427.76 MB |
| 8 | 1 | 1000 | 25 | 1.115 s | 1.1021 s | 427.73 MB |
| 8 | 3 | 100 | 0 | 5.377 s | 5.3402 s | 127.31 MB |
| 8 | 3 | 100 | 25 | 8.282 s | 7.1103 s | 135.3 MB |
| 8 | 3 | 1000 | 0 | 18.916 s | 6.2956 s | 429.51 MB |
| 8 | 3 | 1000 | 25 | 3.124 s | 1.8835 s | 428.59 MB |

## Drain while ingesting

| Workers | Pods | Accounts | LatencyMs | Mean | Median | Allocated |
| -------- | ----- | --------- | ---------- | ---------: | --------: | ----------: |
| 4 | 1 | 50 | 5 | 6.158 s | 7.159 s | 98.55 MB |
| 4 | 3 | 50 | 5 | 6.793 s | 4.426 s | 101.42 MB |
| 8 | 1 | 50 | 5 | 4.347 s | 4.025 s | 108.22 MB |
| 8 | 3 | 50 | 5 | 15.728 s | 2.364 s | 115.57 MB |

## Per-case behaviour (medians over the iterations)

`API calls` = Salesforce requests (composite read + graph write per batch). `Events/graph` = coalescing.
`Retries` = attempts beyond the first, almost all relationships claimed before their Accounts had synced
(`DEPENDENCY_NOT_SYNCED`, retried with backoff).

| Mode | Workers | Pods | Accounts | LatencyMs | API calls | Events/graph | Retries |
| --- | --- | --- | --- | --- | ---: | ---: | ---: |
| preloaded | 1 | 1 | 100 | 0 | 22 | 7.0 | 55 |
| preloaded | 1 | 1 | 100 | 25 | 22 | 7.0 | 55 |
| preloaded | 1 | 1 | 1000 | 0 | 58 | 3.1 | 486 |
| preloaded | 1 | 1 | 1000 | 25 | 58 | 3.1 | 485 |
| preloaded | 1 | 3 | 100 | 0 | 28 | 7.0 | 305 |
| preloaded | 1 | 3 | 100 | 25 | 28 | 7.0 | 305 |
| preloaded | 1 | 3 | 1000 | 0 | 58 | 3.1 | 505 |
| preloaded | 1 | 3 | 1000 | 25 | 60 | 3.1 | 502 |
| preloaded | 4 | 1 | 100 | 0 | 28 | 7.0 | 308 |
| preloaded | 4 | 1 | 100 | 25 | 29 | 7.0 | 308 |
| preloaded | 4 | 1 | 1000 | 0 | 56 | 3.1 | 519 |
| preloaded | 4 | 1 | 1000 | 25 | 56 | 3.1 | 537 |
| preloaded | 4 | 3 | 100 | 0 | 50 | 7.0 | 1432 |
| preloaded | 4 | 3 | 100 | 25 | 41 | 7.0 | 1212 |
| preloaded | 4 | 3 | 1000 | 0 | 60 | 3.1 | 612 |
| preloaded | 4 | 3 | 1000 | 25 | 64 | 3.1 | 647 |
| preloaded | 8 | 1 | 100 | 0 | 47 | 7.0 | 1428 |
| preloaded | 8 | 1 | 100 | 25 | 49 | 7.0 | 1583 |
| preloaded | 8 | 1 | 1000 | 0 | 60 | 3.1 | 538 |
| preloaded | 8 | 1 | 1000 | 25 | 63 | 3.1 | 533 |
| preloaded | 8 | 3 | 100 | 0 | 63 | 7.0 | 2167 |
| preloaded | 8 | 3 | 100 | 25 | 58 | 7.0 | 1781 |
| preloaded | 8 | 3 | 1000 | 0 | 59 | 3.1 | 754 |
| preloaded | 8 | 3 | 1000 | 25 | 67 | 3.1 | 752 |
| streaming | 4 | 1 | 50 | 5 | 147 | 7.2 | 36 |
| streaming | 4 | 3 | 50 | 5 | 180 | 6.3 | 49 |
| streaming | 8 | 1 | 50 | 5 | 168 | 6.4 | 30 |
| streaming | 8 | 3 | 50 | 5 | 233 | 5.8 | 104 |

## Micro benchmarks

| Method | Mean | Median | Allocated |
| ------------------ | ------------: | ------------: | ----------: |
| ParseLimitHeader | 1,103.8 ns | 1,056.9 ns | 2832 B |
| CoalesceTenEvents | 12,647.9 ns | 12,631.7 ns | 25752 B |
| DiffNumber | 243.6 ns | 243.2 ns | 32 B |
| DiffDateTime | 432.4 ns | 432.6 ns | 192 B |

## Cost of the limiter and slots

Compare with `inbox-sync-core.md` (same machine, same run session). The medians are within the run-to-run noise
of each other: the limiter adds one short Postgres transaction per Salesforce call (2 per batch) and the slots one
advisory-lock round trip per batch, which is small next to the claim and the Salesforce round trips. The micro
benchmark puts parsing the `Sforce-Limit-Info` header at about 1.1 µs per response.

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
