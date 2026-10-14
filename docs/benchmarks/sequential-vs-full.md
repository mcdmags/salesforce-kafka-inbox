# Sequential, limiter-only vs full: drain benchmark

The question: how does a sync with no parallel processing and no race-condition handling, only the rate limiter,
compare with the full version on `main`?

## The two versions

| | Sequential (this branch, `benchmark/sequential-limiter-only`) | Full (`main`, 01b53b3) |
|---|---|---|
| Worker loops | One per process (`InboxWorkerService`), one batch per entity type per pass | `Sync:Workers` parallel loops per process, several pods |
| Reading the inbox | Plain read in arrival order: up to `BatchSize` entities, each with its oldest pending rows; entities with a row not due yet (backoff, deferral, unknown-outcome hold) are skipped, so their newer events wait | Claim: `FOR UPDATE SKIP LOCKED` on each entity's head row, a per-entity advisory claim lock, leases (`locked_until`, `lock_token`) |
| Recording outcomes | One transaction per batch: done (+ watermark), superseded, retry with backoff, dead letter; rows are updated if still pending | The same outcomes, every statement conditional on the lease token; the Salesforce call is cut off at 80% of the lease |
| Concurrency cap | None needed (one request at a time) | Global advisory-lock slots (`Sync:MaxConcurrentRequests`) |
| Rate limiter | Token bucket, daily budget, `Sforce-Limit-Info`, `REQUEST_LIMIT_EXCEEDED` / 429 / 503 pauses, deferral: unchanged | The same |
| Everything else | Unchanged: EF Core repositories, FieldValues, read-diff-write, sObject Collections, parent links by `referenceId`, retries, dead letters, watermark, unknown-outcome hold | The same |
| Safe with more than one worker | No | Yes |

Removed in the sequential version: the leases and their columns and index, the claim lock and head-row locking,
`ReleaseLostClaims`, the lease cutoff, the concurrency slots, the schema's migration lock, `Sync:Workers`,
`Sync:MaxConcurrentRequests`, `Sync:Lease`, the test-only claim audit, and the tests that only covered those (claim
races, leases, claim retry, multi-pod concurrency). A row not attempted because of another entity's record simply
stays pending. `retry_count` now counts failed attempts (on `main` it is bumped at every claim).

## Setup

- `InboxDrainBenchmarks`: 5,000 events (about 13% relationship events) preloaded into the real Postgres inbox, then
  drained against the in-process mock org. Each iteration checks that every account and relationship ends at its
  newest version, that no record's write history goes backwards and that every row ends `done` or `superseded`.
  `main` also checks the claim audit for overlapping claims; this branch has no claims, so it checks the org's write
  history for the same version written twice (duplicates) instead.
- BenchmarkDotNet v0.14.0, the classes' own job: Monitoring, 1 warm-up and 5 measured iterations, 1 invocation each,
  Release, `MemoryDiagnoser`. Rate limiter opened up (`SetupAsync`), `SalesforceLimits:DailyApiAllocation`
  1,000,000,000, `BatchSize` 75.
- Apple M4 (10 cores), macOS 26.6.2, .NET SDK 10.0.100, runtime .NET 8.0.17 Arm64. PostgreSQL 17.11
  (`postgres:17`, `max_connections=300`) in Docker on colima, a fresh container per run.
- The machine was shared and busy. Load average (1 min) at the start and end of each run:

| Run | Time (UTC, 2026-10-14) | Load average |
|---|---|---|
| Sequential, run 1 | 22:29 to 22:30 | 9.40 to 8.68 |
| Full, Pods=1, Workers 1/4/8 | 22:30 to 22:35 | 8.43 to 18.57 |
| Full, Pods=3, Workers=4 | 22:35 to 22:37 | 20.78 to 19.99 |
| Race demo (sequential, Pods=3) | 22:37 to 22:40 | 19.81 to 18.80 |
| Race demo, streaming ingest (extra) | 22:40 to 22:41 | 16.77 to 19.14 |
| Sequential, run 2 (noise check) | 22:41 to 22:46 | 17.25 to 27.01 |

The two sequential runs of identical code differ by 1.1x to 6x in time. **Times are only indicative**; the work
counts (API calls, write requests, coalescing) were identical in every iteration of a configuration on this branch
and are the reliable part of the comparison.

## Results

Mean time per drain of 5,000 events; events/s = 5,000 / mean. API calls are the mock org's count for one drain
(steady iterations; the first iteration of a run adds 3 Describe calls). Allocated is per drain.

### Accounts = 100 (about 50 events per account)

| Version | Pods x workers | Latency | Mean | Events/s | API calls | API calls / event | Allocated |
|---|---|---|---:|---:|---:|---:|---:|
| Sequential, run 1 | 1 x 1 | 0 ms | 641.2 ms | 7,798 | 28 | 0.0056 | 103.26 MB |
| Sequential, run 2 | 1 x 1 | 0 ms | 3.823 s | 1,308 | 28 | 0.0056 | 103.66 MB |
| Full | 1 x 1 | 0 ms | 1,548.5 ms | 3,229 | 26 | 0.0052 | 104.97 MB |
| Full | 1 x 4 | 0 ms | 704.2 ms | 7,100 | 34 to 36 | 0.0068 to 0.0072 | 111.93 MB |
| Full | 1 x 8 | 0 ms | 939.9 ms | 5,320 | 38 to 44 | 0.0076 to 0.0088 | 127.43 MB |
| Full | 3 x 4 | 0 ms | 1,771.5 ms | 2,822 | 40 to 48 | 0.0080 to 0.0096 | 125.73 MB |
| Sequential, run 1 | 1 x 1 | 25 ms | 1,567.8 ms | 3,189 | 28 | 0.0056 | 103.8 MB |
| Sequential, run 2 | 1 x 1 | 25 ms | 4.934 s | 1,013 | 28 | 0.0056 | 103.52 MB |
| Full | 1 x 1 | 25 ms | 2,039.2 ms | 2,452 | 26 | 0.0052 | 104.6 MB |
| Full | 1 x 4 | 25 ms | 963.9 ms | 5,187 | 34 to 38 | 0.0068 to 0.0076 | 117 MB |
| Full | 1 x 8 | 25 ms | 1,013.4 ms | 4,934 | 38 to 43 | 0.0076 to 0.0086 | 130.34 MB |
| Full | 3 x 4 | 25 ms | 1,625.4 ms | 3,076 | 39 to 49 | 0.0078 to 0.0098 | 143.55 MB |

Coalescing, every configuration: 878 entity syncs (`done`) for 5,000 events, 4,122 superseded: **5.69 events per
entity sync**. Write requests: 14 sequential, 13 full 1 x 1, 15 to 22 with parallel workers.

### Accounts = 1,000 (about 5 events per account)

| Version | Pods x workers | Latency | Mean | Events/s | API calls | API calls / event | Allocated |
|---|---|---|---:|---:|---:|---:|---:|
| Sequential, run 1 | 1 x 1 | 0 ms | 1,521.2 ms | 3,287 | 56 | 0.0112 | 427.7 MB |
| Sequential, run 2 | 1 x 1 | 0 ms | 4.848 s | 1,031 | 56 | 0.0112 | 428.34 MB |
| Full | 1 x 1 | 0 ms | 3,519.0 ms | 1,421 | 58 | 0.0116 | 430.45 MB |
| Full | 1 x 4 | 0 ms | 1,372.4 ms | 3,643 | 56 | 0.0112 | 433.07 MB |
| Full | 1 x 8 | 0 ms | 1,026.2 ms | 4,872 | 56 to 62 | 0.0112 to 0.0124 | 437.17 MB |
| Full | 3 x 4 | 0 ms | 905.5 ms | 5,522 | 54 to 60 | 0.0108 to 0.0120 | 433.99 MB |
| Sequential, run 1 | 1 x 1 | 25 ms | 2,890.2 ms | 1,730 | 56 | 0.0112 | 427.68 MB |
| Sequential, run 2 | 1 x 1 | 25 ms | 3.269 s | 1,530 | 56 | 0.0112 | 427.08 MB |
| Full | 1 x 1 | 25 ms | 4,099.1 ms | 1,220 | 58 | 0.0116 | 429.81 MB |
| Full | 1 x 4 | 25 ms | 1,251.9 ms | 3,994 | 56 | 0.0112 | 434.1 MB |
| Full | 1 x 8 | 25 ms | 1,363.3 ms | 3,668 | 54 to 60 | 0.0108 to 0.0120 | 436.17 MB |
| Full | 3 x 4 | 25 ms | 1,086.6 ms | 4,602 | 56 to 60 | 0.0112 to 0.0120 | 441.28 MB |

Coalescing, every configuration: 1,618 entity syncs for 5,000 events, 3,382 superseded: **3.09 events per entity
sync**. Write requests: 28 sequential, 29 full 1 x 1, 24 to 30 with parallel workers.

Every iteration of every run above passed its correctness checks (0 wrong, 0 regressions, 0 rows not done; full: 0
overlapping claims; sequential: 0 duplicates).

Retries, for reference only (the two versions count differently): sequential 15 (100 accounts) and 463 (1,000
accounts) failed attempts, nearly all relationships read before their Accounts were in the org
(`DEPENDENCY_NOT_SYNCED`); full 1 x 1 reports 50 and 496 claims beyond the first per row, rising to 450 to 670 with
parallel workers.

## Race demo: three sequential workers on one inbox

`UnsafeMultiPodDrainBenchmarks`: the sequential version with Pods=3, nothing keeping the three workers off each
other's entities. A failing correctness check is the expected result.

| Accounts | Latency | Result | API calls | Records rejected | What the checks found |
|---|---|---|---:|---:|---|
| 100 | 0 ms | Failed in the warm-up iteration | 78 (incl. 9 Describe) | 633 | **396 duplicate writes** (a record received the same version again), 0 regressions, 0 entities at a wrong final version |
| 100 | 25 ms | Failed in the warm-up iteration | 87 (incl. 9 Describe) | 1,076 | **620 duplicate writes**, 0 regressions, 0 wrong |
| 1,000 | 0 ms | Passed, 6.960 s mean (718 events/s), 719.39 MB | 127 to 139 | 1,140 to 1,565 | 0 duplicates, 0 regressions, 0 wrong; 1,289 to 1,555 retries |
| 1,000 | 25 ms | Passed, 8.678 s mean (576 events/s), 903.59 MB | 145 to 161 | 2,059 to 2,760 | 0 duplicates, 0 regressions, 0 wrong; 1,249 to 1,858 retries |
| 100, streaming ingest (extra run) | 25 ms | Failed in the warm-up iteration | 562 | 1,344 | **630 duplicate writes**, 0 regressions, 0 wrong |

What happened:

- With 100 accounts, workers read the same entities at the same time, each read Salesforce before the other wrote,
  and each sent the update: the same version written two or three times (hundreds of extra updates firing triggers,
  flows and field history), plus records rejected outright.
- With 1,000 accounts, almost every write is a create. Two workers creating the same record collide on its external
  id (`DUPLICATE_VALUE`); the loser retries later, finds the record and writes nothing. The final state was correct,
  but it took 2.3x to 2.9x the API calls of one sequential worker (127 to 161 against 56), about 3x to 4x the retries, and
  more time than one worker alone.
- No regression or wrong final version appeared in these runs: workers that read the same rows also read the same
  newest event, the read-diff-write compares with Salesforce at write time, and the watermark only moves forward.
  The window for a regression (one worker writing v3 after another wrote v5 of the same entity) is real but narrow
  in this benchmark; nothing in the sequential version closes it.

## Conclusion

- **One sequential worker is enough** when the event rate is modest and Salesforce latency is low: it makes about
  the same API calls as the full version with one worker (28 against 26, 56 against 58; 0.0056 to 0.0112 per event)
  and fewer than with parallel workers, needs no locking code, and coalesces exactly as much (5.69 and 3.09 events
  per entity sync). Its throughput is bounded by one batch at a time: each
  batch waits for its read and its write in turn, so the time grows with Salesforce latency and with the number of
  distinct entities.
- **Parallel workers buy throughput, not efficiency.** On this busy machine the full version with 4 or 8 workers, or
  3 pods x 4, drained the inbox 1.25x to 3.9x faster than its own single worker (the one exception: 3 x 4 with 100
  accounts and 0 ms, 1,771.5 ms against 1,548.5 ms). Against the sequential version it depends on the run: the
  sequential version's quiet first run beat every full configuration at 100 accounts and 0 ms (641.2 ms), its loaded
  second run was slower than every full configuration with more than one worker. Parallelism costs slightly more API
  calls (more, smaller batches) and more allocation. With real Salesforce latency (hundreds of ms per call instead of 0 to 25) the gap grows with the
  number of workers.
- **The race handling is what makes parallelism safe.** Without it, a second worker or a second pod produced
  hundreds of duplicate writes or create collisions and up to 2.9x the API calls in every configuration measured,
  and leaves open older data overwriting newer. Leases, the claim lock and the slots exist so that more workers can
  be added without any of that; the sequential version must be run as exactly one worker.