# Benchmarks

Measurements of the Postgres inbox (claim and outcome SQL) and of the whole sync path (Postgres inbox, workers,
Salesforce client real; only the Salesforce HTTP endpoint mocked). How to run them is in the
[main README](../../README.md#benchmarks). The drain numbers below were measured with a shared Postgres rate limiter
in the path, opened up so it did not throttle; this version has none, so its per-call cost is no longer included.

These numbers come from the claim and outcome tuning merged in PR #13 and re-measured after the move to EF Core
(PR #12). Later changes (PR #14 formatting, PR #15 simplification of the write path) do not touch the inbox SQL;
they can change the API call counts of the drain benchmarks, so re-run them before comparing call counts.

[Latest run](#latest-run-main-at-2d20c16) re-measures everything on `main` at 2d20c16, with the `sf_sync_state`
claim, the rate limiter in C# and `Sync:CoalesceDelay` (PR #35 to #46). The
[run before it](#latest-run-main-at-7a145b0-after-the-simplification) measured `main` at 7a145b0, after the record
types, the shared Describe cache and the simplification into fewer files (PR #20 to #24).

**Since then the claim has changed.** Entities are now claimed by locking and leasing their row in one
`sf_sync_state` table (`FOR UPDATE SKIP LOCKED`), and the inboxes hold only events (see the
[main README](../../README.md#the-inbox)). The claim statements measured below (LockHeads, the claim lock, the
inbox's lease, due-time and `synced_time` columns and their indexes, `InboxSql.PinPlans`) no longer exist; their
numbers are kept as the record of that design, not as a description of the current one.

**And the inbox has changed since.** Every number on this page was measured with one inbox row per event. The
inboxes now keep one row per entity (migration `InboxOneRowPerRecord`): a newer event overwrites the entity's row, a
claim takes at most one row per entity, and `Sync:MaxEventsPerEntity` is gone (see the
[main README](../../README.md#the-inbox)). So the counts of `done` and `superseded` rows, the coalescing as events per
done row, the [leasing cost by rows leased](#leasing-cost-by-rows-leased) and the preloaded drains of 5,000 rows
describe that design. The drain benchmarks now preload each entity's newest event only (100 or 1,000 account rows,
plus relationships) and print `entities` (inbox rows) and the coalescing as events per entity claim.

[Sequential, limiter-only vs full](#sequential-limiter-only-vs-full) compares `main`'s version with one that has no
parallel processing or race-condition handling, measured on `main` at 01b53b3.

## Environment

| | |
|---|---|
| BenchmarkDotNet | v0.14.0 |
| OS | macOS 26.6.2 (25G83), Darwin 25.6.0 |
| CPU | Apple M4, 10 cores |
| .NET | SDK 10.0.100, runtime .NET 8.0.17 Arm64 (Release) |
| PostgreSQL | 17.11 in Docker (4 CPUs, 6 GB VM), `shared_buffers` 512MB, `max_connections` 300 |

The machine ran other workloads throughout (load average 6 to 54). Absolute times are therefore noisy, by up to
2x between two runs of the same code; the work counts (API calls, claims, retries) are not, and they are what shows
whether two runs did the same thing.

## What was measured

1. **SQL statements in isolation** (`EXPLAIN ANALYZE` in a rolled-back transaction, exactly the SQL the worker
   sends, with the plan pins it sends). 108 scenarios: inbox of 5,000 or 100,000 pending rows; 100, 5,000 or
   one-row-per-entity entities; event times equal or distinct; no, some or most (90%) entities leased by another
   worker; and three statistics states:
   - **fresh**: never analyzed after the load (the planner's defaults),
   - **analyzed**: `ANALYZE` after the load,
   - **stale**: analyzed while every row was finished, then the backlog loaded (what autoanalyze leaves behind
     after a quiet period followed by a burst).
2. **Truth checks**: for the same scenarios and two batch sizes, the rows each statement touches are compared with
   a plain reference query. All matched, before and after.
3. **Drain benchmarks** (BenchmarkDotNet, `InboxDrainBenchmarks`, `StreamingIngestBenchmarks`): P pods x W
   worker loops drain 5,000 events. Every iteration checks that each account ends at its newest version, no
   record's write history goes backwards, every inbox row is `done` or `superseded` (now also: every entity's row
   holds its newest event), and the claim audit shows no two overlapping claims of one entity.
4. **Micro benchmarks** (no database).

## Correctness

Every drain and streaming run: 196 iterations (warm-up included), 0 failures: 0 overlapping claims, 0 accounts not
at their newest version, 0 wrong values, 0 write-history regressions. Truth checks: all scenarios matched, before
and after the SQL changes.

Mutation check of the claim (integration tests, real pods): removing the lease conditions, the claim lock, the
lease re-check and the head's due check made `ConcurrencyTests` fail with 10,119 overlapping claims. With only the
due check kept, the multi-pod test passed but the hand-interleaved race test failed: the race needs a row committed out of id
order, which the hand-interleaved test produces and a load test rarely does. Removing only `SKIP LOCKED` passed
both (workers then block on each other and Postgres re-checks the locked row; slower, still correct).

Integration tests, two consecutive runs on the EF Core version: `Passed! - Failed: 0, Passed: 44, Skipped: 0,
Total: 44` (57 s and 56 s). Unit tests: 276 passed.

## SQL statements

What changed, and why:

- **Plan pins.** The claim and outcome transactions start with `InboxSql.PinPlans`: `SET LOCAL` disables bitmap
  scans, sorts, sequential scans and JIT. With stale statistics the planner chose bitmap or ready-index plans that
  read every pending row per entity (seconds per statement); with the pins every statement got the same plan in
  all 108 scenarios.
- **`CASE` for conditions a partial index also matches.** A plain `status = 'pending'` in LockHeads' head
  re-check, in ClaimEntities and in ReleaseLostClaims let the planner pick the ready/pending partial index and
  re-read it per entity (ClaimEntities went quadratic with stale statistics). Written as `CASE`, the condition is
  only a filter.
- **Fail and Dead** bind the error list once (`(select @errors::jsonb)`, an InitPlan) instead of a CTE the
  planner copied per row.
- **ReleaseLostClaims** finds the claim's rows by `entity_id = any(@entityIds)` on the leased index instead of
  scanning the ready index.
- **MarkSynced** probes one lease per entity (`order by l.entity_id, l.locked_until limit 1`).
- **Watermark in the inbox** (after these measurements). The shared `sf_sync_state` table is gone, so every table
  holds one type of event. `InboxSql.Done` completes the synced rows and sets their `synced_time` in one statement,
  found by primary key under the lease token like Fail and Dead (it replaces MarkSynced and the done Complete);
  `SyncedTimes` reads each claimed entity's watermark, its newest `synced_time`, with one backward probe of the
  partial index `(entity_id, synced_time) where status = 'done'`. The MarkSynced, Complete and SyncedTimes rows
  below were measured before this change and have not been re-measured.
- **Autovacuum** on the inbox tables: `autovacuum_vacuum_scale_factor` 0.02, threshold 1000. On 200,000 rows the
  first claim after 150,000 rows finished took 187 ms before vacuum, 5 to 7 ms once vacuum ran (2 ms fully
  vacuumed).
- **No index changes.** Every lease and outcome updates an indexed column, so updates are never HOT and each one
  adds an entry to all four indexes; a lower fillfactor does not help that. The cost is per row leased (below).
- **Claim lock** (correctness, not speed): `pg_try_advisory_xact_lock(hashtext('{inbox}'), hashtext(entity_id))`
  in LockHeads, plus ClaimEntities leasing nothing of an entity another token holds a live lease on. See the main
  README's claim section. It needs READ COMMITTED (each statement's snapshot is taken after the lock).

### Every statement over the 108 scenarios

Median of the per-scenario medians, the slowest single run, and how many different plans the statement got.

| Statement | Before: median | Before: slowest | Before: plans | After: median | After: slowest | After: plans |
|---|--:|--:|--:|--:|--:|--:|
| LockHeads | 3.1 | 14,515 | 4 | 2.1 | 21.5 | 1 |
| ClaimEntities | 37.1 | 6,964 | 3 | 25.4 | 244 | 1 |
| SyncedTimes | 0.3 | 1.2 | 2 | 0.3 | 1.6 | 1 |
| MarkSynced | 1.2 | 11.5 | 2 | 1.2 | 5.2 | 1 |
| Complete | 9.8 | 77.5 | 2 | 9.6 | 60.4 | 1 |
| Fail | 25.6 | 126 | 3 | 22.1 | 319 | 1 |
| Dead | 13.0 | 152 | 3 | 11.7 | 131 | 1 |
| Release | 19.3 | 94.0 | 2 | 19.3 | 203 | 1 |
| Hold | 24.1 | 133 | 2 | 24.1 | 192 | 1 |
| ReleaseLostClaims | 21.3 | 179 | 6 | 18.6 | 162 | 1 |
| Insert | 0.2 | 4.1 | 1 | 0.2 | 1.4 | 1 |

### Statistics: fresh, analyzed, stale

Slowest median per statistics state, over all shapes, times and lease mixes (ms).

| Statement | Stats | Before | After |
|---|---|--:|--:|
| LockHeads | fresh | 5.6 | 4.0 |
| LockHeads | analyzed | 5.1 | 4.1 |
| LockHeads | stale | 12,380 | 9.5 |
| ClaimEntities | fresh | 156 | 86.6 |
| ClaimEntities | analyzed | 52.7 | 57.5 |
| ClaimEntities | stale | 4,020 | 75.2 |
| MarkSynced | fresh | 6.3 | 3.0 |
| MarkSynced | analyzed | 4.0 | 5.2 |
| MarkSynced | stale | 11.5 | 4.9 |
| Fail | fresh | 75.8 | 242 |
| Fail | analyzed | 80.2 | 47.1 |
| Fail | stale | 122 | 103 |
| ReleaseLostClaims | fresh | 88.2 | 116 |
| ReleaseLostClaims | analyzed | 51.9 | 113 |
| ReleaseLostClaims | stale | 76.1 | 65.8 |

### The claim per scenario

LockHeads (75 entities) and ClaimEntities: median of 5 and 3 runs, slowest run in brackets, ms. Rows: rows leased by the claim (75 entities x up to 20 events).

| Scenario | Entities | Rows | LockHeads before | LockHeads after | ClaimEntities before | ClaimEntities after |
|---|--:|--:|--:|--:|--:|--:|
| 5k/100 same none fresh | 75 | 1500 | 3.3 (6.4) | 1.5 (5.5) | 34.8 (36.4) | 30.3 (33.4) |
| 5k/100 same none analyzed | 75 | 1500 | 1.7 (2.0) | 1.5 (2.2) | 33.1 (41.0) | 27.6 (30.4) |
| 5k/100 same none stale | 75 | 1500 | 64.6 (72.5) | 2.1 (2.8) | 121 (121) | 28.3 (28.4) |
| 5k/100 same some fresh | 75 | 1500 | 3.0 (4.0) | 1.5 (2.6) | 28.3 (28.9) | 30.3 (31.5) |
| 5k/100 same some analyzed | 75 | 1500 | 1.8 (2.3) | 2.5 (3.0) | 28.6 (29.1) | 28.4 (36.0) |
| 5k/100 same some stale | 75 | 1500 | 77.5 (86.8) | 1.8 (3.4) | 102 (159) | 31.1 (47.6) |
| 5k/100 same heavy fresh | 17 | 340 | 5.6 (6.7) | 2.9 (4.2) | 6.8 (7.0) | 7.5 (12.1) |
| 5k/100 same heavy analyzed | 17 | 340 | 3.5 (5.7) | 2.7 (4.8) | 9.3 (9.8) | 7.4 (8.1) |
| 5k/100 same heavy stale | 20 | 400 | 353 (484) | 3.0 (3.7) | 42.2 (43.3) | 9.4 (12.1) |
| 5k/100 distinct none fresh | 75 | 1500 | 3.4 (4.4) | 2.2 (3.1) | 44.9 (50.4) | 37.9 (47.7) |
| 5k/100 distinct none analyzed | 75 | 1500 | 1.9 (2.9) | 1.7 (2.7) | 31.9 (32.3) | 31.3 (32.4) |
| 5k/100 distinct none stale | 75 | 1500 | 65.7 (72.3) | 1.9 (2.6) | 122 (126) | 30.9 (34.0) |
| 5k/100 distinct some fresh | 75 | 1500 | 3.0 (3.5) | 2.4 (3.3) | 30.8 (35.2) | 30.9 (32.3) |
| 5k/100 distinct some analyzed | 75 | 1500 | 1.4 (1.8) | 1.8 (2.2) | 26.1 (31.0) | 26.7 (29.5) |
| 5k/100 distinct some stale | 75 | 1500 | 62.2 (71.5) | 2.4 (3.0) | 74.5 (77.6) | 28.6 (29.6) |
| 5k/100 distinct heavy fresh | 17 | 340 | 3.3 (3.9) | 2.6 (5.1) | 5.0 (5.3) | 6.3 (6.4) |
| 5k/100 distinct heavy analyzed | 17 | 340 | 2.1 (2.6) | 2.2 (3.3) | 5.0 (5.2) | 5.7 (7.0) |
| 5k/100 distinct heavy stale | 20 | 400 | 216 (229) | 1.7 (2.3) | 18.8 (21.5) | 5.6 (6.3) |
| 5k/1000 same none fresh | 75 | 375 | 3.0 (3.6) | 1.3 (1.9) | 5.4 (7.2) | 6.1 (7.2) |
| 5k/1000 same none analyzed | 75 | 375 | 1.7 (2.3) | 1.8 (3.7) | 7.8 (8.6) | 7.1 (7.2) |
| 5k/1000 same none stale | 75 | 375 | 46.6 (46.6) | 1.4 (2.3) | 49.8 (54.3) | 7.8 (8.1) |
| 5k/1000 same some fresh | 75 | 374 | 2.8 (3.3) | 2.0 (3.2) | 5.6 (6.0) | 8.1 (8.6) |
| 5k/1000 same some analyzed | 75 | 374 | 1.4 (1.6) | 3.1 (4.1) | 6.1 (8.0) | 10.9 (13.9) |
| 5k/1000 same some stale | 75 | 374 | 51.2 (54.4) | 2.0 (3.7) | 53.2 (56.7) | 8.5 (10.7) |
| 5k/1000 same heavy fresh | 75 | 374 | 2.8 (3.8) | 1.8 (3.2) | 6.3 (7.0) | 8.2 (8.7) |
| 5k/1000 same heavy analyzed | 75 | 374 | 1.3 (1.9) | 1.7 (2.8) | 6.0 (6.3) | 6.6 (6.8) |
| 5k/1000 same heavy stale | 75 | 375 | 85.3 (96.6) | 2.1 (2.4) | 89.5 (91.0) | 9.6 (9.8) |
| 5k/1000 distinct none fresh | 75 | 375 | 2.1 (3.0) | 1.5 (2.2) | 4.8 (5.4) | 7.3 (7.4) |
| 5k/1000 distinct none analyzed | 75 | 375 | 1.0 (1.2) | 1.4 (2.2) | 5.0 (5.5) | 7.7 (8.2) |
| 5k/1000 distinct none stale | 75 | 375 | 47.0 (54.9) | 2.3 (3.1) | 65.6 (72.2) | 17.3 (71.6) |
| 5k/1000 distinct some fresh | 75 | 374 | 2.5 (3.2) | 1.7 (2.5) | 5.7 (6.9) | 9.5 (9.7) |
| 5k/1000 distinct some analyzed | 75 | 374 | 1.3 (1.8) | 2.3 (4.0) | 6.2 (7.2) | 8.6 (11.0) |
| 5k/1000 distinct some stale | 75 | 374 | 51.9 (53.2) | 2.5 (2.9) | 56.9 (66.3) | 13.7 (14.5) |
| 5k/1000 distinct heavy fresh | 75 | 374 | 2.6 (3.4) | 3.2 (5.3) | 5.7 (6.5) | 8.7 (11.3) |
| 5k/1000 distinct heavy analyzed | 75 | 374 | 1.7 (2.1) | 1.8 (2.8) | 6.2 (6.3) | 8.8 (11.7) |
| 5k/1000 distinct heavy stale | 75 | 375 | 82.2 (87.9) | 2.3 (4.8) | 89.3 (90.4) | 10.9 (11.5) |
| 5k/5000 same none fresh | 75 | 75 | 3.4 (3.8) | 1.4 (2.2) | 1.6 (1.6) | 2.0 (2.1) |
| 5k/5000 same none analyzed | 75 | 75 | 1.3 (1.5) | 1.3 (2.4) | 1.5 (1.7) | 2.1 (2.5) |
| 5k/5000 same none stale | 75 | 75 | 45.6 (47.3) | 1.5 (2.3) | 40.4 (40.5) | 1.9 (2.3) |
| 5k/5000 same some fresh | 75 | 75 | 2.6 (3.4) | 1.7 (2.8) | 1.7 (1.9) | 2.1 (2.2) |
| 5k/5000 same some analyzed | 75 | 75 | 2.1 (2.2) | 1.6 (2.7) | 1.7 (3.0) | 2.3 (3.1) |
| 5k/5000 same some stale | 75 | 75 | 51.7 (55.8) | 2.1 (3.9) | 48.6 (51.0) | 2.1 (2.3) |
| 5k/5000 same heavy fresh | 75 | 75 | 3.1 (3.8) | 1.8 (3.1) | 1.5 (1.7) | 2.6 (2.7) |
| 5k/5000 same heavy analyzed | 75 | 75 | 1.5 (2.5) | 4.1 (4.6) | 1.6 (1.7) | 2.2 (2.7) |
| 5k/5000 same heavy stale | 75 | 75 | 96.3 (137) | 2.0 (3.1) | 87.2 (122) | 3.3 (3.6) |
| 5k/5000 distinct none fresh | 75 | 75 | 3.2 (4.1) | 2.1 (3.0) | 1.6 (2.7) | 2.7 (2.9) |
| 5k/5000 distinct none analyzed | 75 | 75 | 1.2 (1.3) | 2.4 (2.7) | 1.4 (1.6) | 3.2 (3.3) |
| 5k/5000 distinct none stale | 75 | 75 | 46.8 (49.2) | 2.5 (3.3) | 40.6 (41.0) | 3.2 (3.4) |
| 5k/5000 distinct some fresh | 75 | 75 | 2.6 (3.7) | 2.5 (3.7) | 1.7 (2.0) | 3.4 (3.7) |
| 5k/5000 distinct some analyzed | 75 | 75 | 1.5 (2.0) | 2.6 (3.9) | 1.7 (1.7) | 3.7 (3.7) |
| 5k/5000 distinct some stale | 75 | 75 | 53.2 (67.0) | 2.7 (3.9) | 46.8 (48.1) | 2.9 (4.4) |
| 5k/5000 distinct heavy fresh | 75 | 75 | 3.4 (4.5) | 3.2 (5.0) | 1.6 (1.7) | 3.4 (4.2) |
| 5k/5000 distinct heavy analyzed | 75 | 75 | 1.6 (2.2) | 3.1 (7.1) | 1.5 (1.7) | 3.6 (8.1) |
| 5k/5000 distinct heavy stale | 75 | 75 | 135 (145) | 2.2 (5.3) | 86.5 (115) | 2.8 (3.8) |
| 100k/100 same none fresh | 75 | 1500 | 1.7 (2.3) | 2.7 (5.8) | 82.5 (94.3) | 34.6 (38.3) |
| 100k/100 same none analyzed | 75 | 1500 | 1.2 (1.5) | 1.9 (2.7) | 23.5 (29.2) | 40.0 (40.4) |
| 100k/100 same none stale | 75 | 1500 | 1,685 (2,002) | 1.3 (2.2) | 1,576 (2,034) | 25.9 (26.1) |
| 100k/100 same some fresh | 75 | 1500 | 1.4 (1.8) | 1.4 (2.3) | 100 (109) | 23.3 (24.9) |
| 100k/100 same some analyzed | 75 | 1500 | 1.5 (1.8) | 1.4 (2.1) | 21.7 (21.8) | 23.2 (23.7) |
| 100k/100 same some stale | 75 | 1500 | 1,871 (1,910) | 1.5 (2.7) | 1,820 (2,256) | 24.8 (24.9) |
| 100k/100 same heavy fresh | 17 | 340 | 4.7 (6.4) | 2.1 (3.2) | 21.5 (23.7) | 4.8 (4.9) |
| 100k/100 same heavy analyzed | 17 | 340 | 2.2 (3.2) | 2.7 (5.1) | 5.2 (5.2) | 4.4 (4.9) |
| 100k/100 same heavy stale | 20 | 400 | 7,371 (8,297) | 2.1 (3.0) | 467 (467) | 7.0 (8.2) |
| 100k/100 distinct none fresh | 75 | 1500 | 1.3 (1.9) | 1.3 (2.3) | 91.2 (118) | 27.0 (28.3) |
| 100k/100 distinct none analyzed | 75 | 1500 | 1.3 (1.5) | 2.0 (2.8) | 21.9 (22.1) | 28.6 (29.4) |
| 100k/100 distinct none stale | 75 | 1500 | 2,197 (2,253) | 1.7 (3.0) | 1,644 (1,744) | 60.7 (89.9) |
| 100k/100 distinct some fresh | 75 | 1500 | 2.5 (3.1) | 2.6 (3.1) | 156 (238) | 38.8 (44.1) |
| 100k/100 distinct some analyzed | 75 | 1500 | 2.5 (3.0) | 1.5 (2.5) | 38.5 (39.3) | 32.0 (32.5) |
| 100k/100 distinct some stale | 75 | 1500 | 3,626 (6,150) | 1.4 (2.2) | 4,020 (6,964) | 25.7 (29.4) |
| 100k/100 distinct heavy fresh | 17 | 340 | 4.2 (6.1) | 2.4 (3.2) | 34.9 (38.8) | 5.0 (5.5) |
| 100k/100 distinct heavy analyzed | 17 | 340 | 5.1 (8.9) | 1.9 (2.7) | 7.9 (8.9) | 4.5 (4.7) |
| 100k/100 distinct heavy stale | 20 | 400 | 8,387 (9,637) | 4.4 (6.5) | 632 (801) | 12.2 (16.7) |
| 100k/1000 same none fresh | 75 | 1500 | 2.7 (5.5) | 2.0 (7.1) | 40.0 (50.4) | 42.6 (82.1) |
| 100k/1000 same none analyzed | 75 | 1500 | 2.1 (2.2) | 1.6 (2.3) | 31.7 (31.8) | 29.4 (29.8) |
| 100k/1000 same none stale | 75 | 1500 | 2,563 (2,983) | 1.6 (5.5) | 2,342 (3,138) | 40.8 (49.1) |
| 100k/1000 same some fresh | 75 | 1500 | 4.0 (4.7) | 4.0 (7.2) | 50.0 (51.8) | 37.6 (40.1) |
| 100k/1000 same some analyzed | 75 | 1500 | 2.5 (6.6) | 2.9 (3.4) | 41.3 (48.9) | 31.4 (35.9) |
| 100k/1000 same some stale | 75 | 1500 | 2,619 (4,104) | 2.5 (4.5) | 2,445 (2,608) | 48.5 (49.8) |
| 100k/1000 same heavy fresh | 75 | 1500 | 4.2 (5.6) | 1.7 (3.6) | 63.4 (69.8) | 27.6 (44.5) |
| 100k/1000 same heavy analyzed | 75 | 1500 | 3.3 (6.7) | 1.7 (3.5) | 41.3 (45.4) | 25.1 (29.2) |
| 100k/1000 same heavy stale | 75 | 1500 | 11,546 (12,637) | 9.5 (17.2) | 3,102 (4,028) | 46.4 (51.6) |
| 100k/1000 distinct none fresh | 75 | 1500 | 3.2 (7.9) | 2.4 (4.1) | 70.4 (99.3) | 33.4 (34.5) |
| 100k/1000 distinct none analyzed | 75 | 1500 | 2.3 (2.6) | 1.4 (2.1) | 34.3 (36.7) | 26.6 (30.4) |
| 100k/1000 distinct none stale | 75 | 1500 | 2,125 (2,380) | 3.0 (5.9) | 2,025 (2,392) | 75.2 (84.0) |
| 100k/1000 distinct some fresh | 75 | 1500 | 2.8 (3.7) | 2.4 (5.5) | 41.2 (46.4) | 42.5 (77.5) |
| 100k/1000 distinct some analyzed | 75 | 1500 | 3.1 (4.0) | 1.9 (2.7) | 43.3 (49.3) | 49.6 (93.1) |
| 100k/1000 distinct some stale | 75 | 1500 | 2,616 (2,739) | 3.0 (5.8) | 2,223 (2,323) | 44.5 (47.7) |
| 100k/1000 distinct heavy fresh | 75 | 1500 | 3.6 (4.1) | 3.0 (5.5) | 52.4 (56.6) | 37.8 (44.5) |
| 100k/1000 distinct heavy analyzed | 75 | 1500 | 2.1 (4.0) | 1.9 (3.6) | 39.7 (43.7) | 30.8 (31.3) |
| 100k/1000 distinct heavy stale | 75 | 1500 | 9,498 (9,962) | 4.8 (7.4) | 1,760 (1,882) | 27.7 (29.3) |
| 100k/5000 same none fresh | 75 | 1500 | 1.6 (2.0) | 1.4 (2.6) | 23.3 (24.2) | 24.4 (25.2) |
| 100k/5000 same none analyzed | 75 | 1500 | 1.3 (1.9) | 1.4 (2.0) | 23.9 (24.0) | 24.2 (27.9) |
| 100k/5000 same none stale | 75 | 1500 | 1,498 (1,605) | 1.9 (3.2) | 1,389 (1,572) | 28.1 (34.4) |
| 100k/5000 same some fresh | 75 | 1500 | 1.5 (3.0) | 2.9 (4.4) | 30.4 (32.3) | 38.3 (42.3) |
| 100k/5000 same some analyzed | 75 | 1500 | 2.3 (3.1) | 2.8 (5.3) | 29.6 (34.6) | 40.9 (103) |
| 100k/5000 same some stale | 75 | 1499 | 2,269 (2,436) | 2.4 (7.6) | 1,526 (1,606) | 33.3 (36.1) |
| 100k/5000 same heavy fresh | 75 | 1500 | 2.7 (5.1) | 2.4 (5.4) | 41.3 (50.5) | 30.4 (44.6) |
| 100k/5000 same heavy analyzed | 75 | 1500 | 1.7 (4.2) | 4.1 (11.6) | 31.5 (41.8) | 57.5 (73.9) |
| 100k/5000 same heavy stale | 75 | 1499 | 6,970 (8,138) | 7.9 (11.5) | 1,690 (1,749) | 56.0 (62.8) |
| 100k/5000 distinct none fresh | 75 | 1500 | 1.3 (1.9) | 3.3 (4.6) | 26.1 (27.2) | 37.0 (38.4) |
| 100k/5000 distinct none analyzed | 75 | 1500 | 1.1 (1.6) | 3.6 (4.7) | 21.3 (23.2) | 46.4 (55.2) |
| 100k/5000 distinct none stale | 75 | 1500 | 1,814 (2,220) | 2.7 (4.6) | 1,478 (1,687) | 72.6 (244) |
| 100k/5000 distinct some fresh | 75 | 1500 | 1.5 (2.2) | 2.9 (7.3) | 35.8 (40.2) | 86.6 (104) |
| 100k/5000 distinct some analyzed | 75 | 1500 | 1.6 (2.3) | 3.0 (4.9) | 30.3 (33.9) | 39.6 (47.1) |
| 100k/5000 distinct some stale | 75 | 1500 | 2,012 (3,412) | 3.3 (10.6) | 1,810 (2,210) | 44.4 (57.0) |
| 100k/5000 distinct heavy fresh | 75 | 1500 | 2.8 (6.9) | 4.0 (9.0) | 40.4 (42.9) | 40.3 (50.0) |
| 100k/5000 distinct heavy analyzed | 75 | 1500 | 3.7 (10.6) | 3.7 (10.5) | 52.7 (56.5) | 51.1 (60.7) |
| 100k/5000 distinct heavy stale | 75 | 1500 | 12,380 (14,515) | 7.8 (21.5) | 2,112 (2,354) | 70.8 (149) |

### Outcome statements by number of ids

100,000-row tables (100 or 5,000 entities, no other leases or 90% leased), fresh / analyzed / stale statistics. Slowest median over those 12 tables, ms, and the scans that drove the plans.

| Statement | Ids | Before | Before: driven by | After | After: driven by |
|---|--:|--:|---|--:|---|
| MarkSynced | 75 | 11.8 | Index Scan leased (7), Bitmap Heap Scan (5) | 4.7 | Index Scan leased (12) |
| MarkSynced | 1,000 | 917 | Index Scan leased (7), Bitmap Heap Scan (5) | 26.2 | Index Scan leased (12) |
| MarkSynced | 5,000 | 463 | Index Scan leased (7), Bitmap Heap Scan (5) | 103 | Index Scan leased (12) |
| Complete | 75 | 2.6 | Index Scan pkey (7), Bitmap Heap Scan (5) | 1.5 | Index Scan pkey (12) |
| Complete | 1,000 | 112 | Index Scan pkey (7), Bitmap Heap Scan (5) | 30.1 | Index Scan pkey (12) |
| Complete | 5,000 | 309 | Index Scan pkey (7), Bitmap Heap Scan (5) | 93.8 | Index Scan pkey (12) |
| Fail | 75 | 7.2 | Index Scan pkey (7), Bitmap Heap Scan (5) | 3.1 | Index Scan pkey (12) |
| Fail | 1,000 | 168 | Index Scan pkey (7), Bitmap Heap Scan (5) | 58.0 | Index Scan pkey (12) |
| Fail | 5,000 | 832 | Index Scan pkey (7), Bitmap Heap Scan (5) | 246 | Index Scan pkey (12) |
| Dead | 75 | 1.8 | Index Scan pkey (7), Bitmap Heap Scan (5) | 1.9 | Index Scan pkey (12) |
| Dead | 1,000 | 128 | Index Scan pkey (7), Bitmap Heap Scan (5) | 34.5 | Index Scan pkey (12) |
| Dead | 5,000 | 998 | Index Scan pkey (7), Bitmap Heap Scan (5) | 141 | Index Scan pkey (12) |
| Release | 75 | 3.8 | Index Scan pkey (7), Bitmap Heap Scan (5) | 9.0 | Index Scan pkey (12) |
| Release | 1,000 | 166 | Index Scan pkey (7), Bitmap Heap Scan (5) | 66.7 | Index Scan pkey (12) |
| Release | 5,000 | 712 | Index Scan pkey (7), Bitmap Heap Scan (5) | 207 | Index Scan pkey (12) |
| Hold | 75 | 3.0 | Index Scan pkey (7), Bitmap Heap Scan (5) | 5.2 | Index Scan pkey (12) |
| Hold | 1,000 | 119 | Index Scan pkey (7), Bitmap Heap Scan (5) | 74.7 | Index Scan pkey (12) |
| Hold | 5,000 | 960 | Index Scan pkey (7), Bitmap Heap Scan (5) | 335 | Index Scan pkey (12) |
| ReleaseLostClaims | 75 | 53.8 | Bitmap Heap Scan (5), Index Scan ready (4), Index Scan leased (2), Seq Scan (1) | 5.0 | Index Scan leased (12) |
| ReleaseLostClaims | 1,000 | 128 | Bitmap Heap Scan (5), Index Scan ready (4), Index Scan leased (2), Seq Scan (1) | 91.1 | Index Scan leased (12) |
| ReleaseLostClaims | 5,000 | 1,521 | Bitmap Heap Scan (5), Index Scan ready (4), Index Scan leased (2), Seq Scan (1) | 501 | Index Scan leased (12) |

### Leasing cost by rows leased

Deep backlog (100 entities x 1,000 pending rows, analyzed), 75 entities per claim, `Sync:MaxEventsPerEntity` (since
removed: a claim now takes one row per entity, the first line below) 1 to 40; median of 5 runs, measured under load (load average about 25). On a quiet machine leasing costs about 11 µs
per row.

| Events per entity | Rows leased | ClaimEntities median ms | slowest | µs per row |
|--:|--:|--:|--:|--:|
| 1 | 75 | 4.2 | 5.5 | 55.4 |
| 5 | 375 | 12.4 | 16.6 | 33.0 |
| 10 | 750 | 32.7 | 51.2 | 43.6 |
| 20 | 1500 | 53.7 | 76.3 | 35.8 |
| 40 | 3000 | 86.8 | 103.8 | 28.9 |

A claim of 75 entities locks them (LockHeads) in about 2 ms (median over the 108 scenarios); the slowest scenario
median is 9.5 ms (stale statistics, 90% of entities leased) and the slowest single run 21.5 ms, under load. Leasing
dominates the claim for deep backlogs: 1,500 rows is 20 to 40 ms quiet, about 54 ms under load. With one row per
entity a deep backlog no longer exists: whatever the number of events, a claim of 75 entities leases 75 rows.

## Latest run: main at 2d20c16

All 37 benchmarks (`--run full`) on `main` at 2d20c16 (PR #46), on 2026-10-16. It is the first full run since the
claim moved to `sf_sync_state` (PR #35), and it includes the rate limiter in C# (PR #37, #38), the query URL limits
(PR #39) and `Sync:CoalesceDelay` 50 ms (PR #41, #42). The environment is the same as above. The load average was
4.8 at the start and 7 to 10 during the run. The run took 14 minutes.

**Correctness**: 196 iterations (warm-up included), 0 failures: 0 overlapping claims, 0 accounts not at their newest
version, 0 wrong values, 0 write-history regressions, 0 failed records.

**Same work**: every event still ends in one of 878 (100 accounts) or 1,618 (1,000 accounts) entity syncs, with
coalescing 5.69 and 3.09. With one worker the run is still deterministic, but the counts differ from the
[7a145b0 run](#latest-run-main-at-7a145b0-after-the-simplification) because the claim changed:

| Accounts | API calls | Write req | Claims | Retries |
|--:|--:|--:|--:|--:|
| 100 | 30 (was 26) | 15 (13) | 938 (928) | 60 (50) |
| 1,000 | 60 (58) | 29 (29) | 2,256 (2,114) | 638 (496) |

Preloaded drain, 5,000 events. The medians are BenchmarkDotNet's. API calls, write requests, claims and retries
are the medians of the iterations without a Describe, at 0 ms latency:

| W | P | Accounts | Median 0 ms | Median 25 ms | Allocated | API calls | Write req | Claims | Retries | 7a145b0 mean 0 ms |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 | 1 | 100 | 689.9 ms | 1,371.4 ms | 47.7 MB | 30 | 15 | 938 | 60 | 704.5 ms |
| 1 | 1 | 1,000 | 971.2 ms | 3,107.0 ms | 93.1 MB | 60 | 29 | 2,256 | 638 | 1,668.1 ms |
| 1 | 3 | 100 | 351.5 ms | 579.2 ms | 51.6 MB | 33 | 16 | 953 | 75 | 490.9 ms |
| 1 | 3 | 1,000 | 580.0 ms | 1,038.1 ms | 95.4 MB | 60 | 30 | 2,249 | 631 | 969.0 ms |
| 4 | 1 | 100 | 235.3 ms | 548.1 ms | 54.8 MB | 35 | 17 | 1,026 | 148 | 557.2 ms |
| 4 | 1 | 1,000 | 535.8 ms | 900.5 ms | 95.4 MB | 60 | 30 | 2,250 | 632 | 782.3 ms |
| 4 | 3 | 100 | 464.7 ms | 709.6 ms | 85.7 MB | 44 | 18 | 1,632 | 754 | 847.6 ms |
| 4 | 3 | 1,000 | 482.8 ms | 744.2 ms | 98.5 MB | 57 | 27 | 1,989 | 371 | 823.7 ms |
| 8 | 1 | 100 | 397.2 ms | 665.6 ms | 70.0 MB | 39 | 17 | 1,327 | 449 | 648.2 ms |
| 8 | 1 | 1,000 | 544.5 ms | 920.8 ms | 97.3 MB | 57 | 27 | 2,184 | 566 | 830.7 ms |
| 8 | 3 | 100 | 705.9 ms | 891.9 ms | 112.3 MB | 47 | 18 | 1,661 | 783 | 1,255.3 ms |
| 8 | 3 | 1,000 | 893.5 ms | 923.8 ms | 138.0 MB | 75 | 34 | 2,605 | 987 | 1,115.3 ms |

Streaming (50 accounts, 5 ms latency, `CoalesceDelay` 50 ms):

| W | P | Mean | Allocated | API calls | Write req | Coalescing | Claims | Retries | 7a145b0 mean / API calls |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 4 | 1 | 1.139 s | 104.1 MB | 216 | 107 | 4.75 | 1,073 | 22 | 1.249 s / 201 |
| 4 | 3 | 1.587 s | 152.3 MB | 310 | 154 | 4.05 | 1,262 | 26 | 1.302 s / 292 |
| 8 | 1 | 1.812 s | 148.6 MB | 332 | 165 | 3.62 | 1,409 | 26 | 1.188 s / 272 |
| 8 | 3 | 1.951 s | 168.5 MB | 307 | 150 | 5.05 | 1,054 | 54 | 1.808 s / 328 |

- **4 workers x 3 pods, 100 accounts, 0 ms**: median 464.7 ms (mean 473.8 ms), against about 410 to 420 ms before
  the rate limiter moved to C# (PR #37). That matches the 457 to 474 ms of the [sweep](#coalescedelay-sweep)'s quick
  drains and is within the noise of this machine, so the slowdown measured after PR #37 is gone with PR #38.
- **Memory**: about half of the [EF Core run](#current-ef-core-before-and-after-the-sql-changes) with 100 accounts
  (48 to 112 MB, against 106 to 177 MB) and about a quarter with 1,000 accounts (93 to 138 MB, against 434 to
  519 MB). Much of the drop is the fake org's JSON (PR #33), which allocates in the benchmark process.
- **Streaming**: 4 x 1 made 216 calls (coalescing 4.75), close to the sweep's 205. 8 x 1 made more calls than at
  7a145b0 (332 against 272) with lower coalescing (3.62 against 3.86). Eight loops on one pod each claim small
  batches of the 50 accounts as they arrive. The 3-pod cases are about the same.
- Times are lower than at 7a145b0 in every preloaded case. Some of that is the code (1 x 1 with 1,000 accounts:
  971 ms against 1,668 ms, with 2 more calls) and some is the machine, so compare work counts first.

Micro benchmarks:

| Method | Mean | Allocated |
|---|--:|--:|
| ParseLimitHeader | 860.08 ns | 2832 B |
| CoalesceTenEvents | 27.56 ns | - |
| PrepareTenEvents | 21,000.02 ns | 20664 B |
| ReadAndValidateEvent | 1,523.39 ns | 1616 B |
| MapAccountEvent | 2,557.03 ns | 4000 B |
| MapPayloadToSObject | 31.72 ns | 200 B |
| MapSObjectToRecord | 377.69 ns | 1000 B |
| DiffNumber | 150.14 ns | 32 B |
| DiffDateTime | 343.87 ns | 192 B |

## Latest run: main at 7a145b0, after the simplification

Everything re-run on `main` at 7a145b0 (PR #24): the record types and the Describe cache shared through Redis
(PR #20), the record type rejection fixes (PR #21, #22) and the simplification into fewer files and types (PR #23,
no behaviour change). Same machine and Postgres settings as above (Postgres 17 in its own container), load average 4 to 8,
quieter than the runs below, so the times are lower mainly because of the machine. 37 benchmarks, 11 minutes.

**Correctness**: 196 iterations, 0 failures: 0 overlapping claims, 0 accounts not at their newest version, 0 wrong
values, 0 write-history regressions, 0 failed records.

**Same work**: with one worker the run is deterministic, and the counts equal the EF Core run below exactly
(100 accounts: 26 API calls, 1,138 records, 878 entity syncs, 928 claims, 50 retries; 1,000 accounts: 58, 2,592,
1,618, 2,114, 496). With several workers the counts depend on timing and are in the same ranges.

Preloaded drain, 5,000 events, 0 ms latency (API calls, write requests, claims and retries are the median over
both latencies of the iterations without a Describe):

| W | P | Accounts | Mean | API calls | Write req | Claims | Retries | EF Core run mean |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 | 1 | 100 | 704.5 ms | 26 | 13 | 928 | 50 | 1,590.0 ms |
| 1 | 1 | 1,000 | 1,668.1 ms | 58 | 29 | 2,114 | 496 | 3,726.7 ms |
| 1 | 3 | 100 | 490.9 ms | 35 | 17 | 987 | 109 | 458.0 ms |
| 1 | 3 | 1,000 | 969.0 ms | 56 | 28 | 2,099 | 481 | 1,724.9 ms |
| 4 | 1 | 100 | 557.2 ms | 37 | 17 | 1,097 | 219 | 1,178.8 ms |
| 4 | 1 | 1,000 | 782.3 ms | 56 | 27 | 2,150 | 532 | 1,989.7 ms |
| 4 | 3 | 100 | 847.6 ms | 46 | 18 | 1,524 | 646 | 1,130.5 ms |
| 4 | 3 | 1,000 | 823.7 ms | 60 | 27 | 2,202 | 584 | 1,502.9 ms |
| 8 | 1 | 100 | 648.2 ms | 40 | 17 | 1,328 | 450 | 837.2 ms |
| 8 | 1 | 1,000 | 830.7 ms | 58 | 27 | 2,138 | 520 | 1,934.9 ms |
| 8 | 3 | 100 | 1,255.3 ms | 62 | 26 | 1,623 | 745 | 1,750.5 ms |
| 8 | 3 | 1,000 | 1,115.3 ms | 74 | 32 | 2,682 | 1,064 | 2,138.4 ms |

With 25 ms latency the means were 1,407.7 / 3,159.9 ms (1×1), 754.0 / 1,280.8 ms (1×3), 771.3 / 1,131.9 ms (4×1),
897.1 / 869.8 ms (4×3), 881.2 / 858.2 ms (8×1) and 1,328.4 / 1,116.6 ms (8×3) for 100 / 1,000 accounts.

Streaming (50 accounts, 5 ms latency): 1.249 s (4×1), 1.302 s (4×3), 1.188 s (8×1), 1.808 s (8×3); API calls 201,
292, 272 and 328.

Micro benchmarks:

| Method | Mean | Allocated |
|---|--:|--:|
| ParseLimitHeader | 1,134.14 ns | 2832 B |
| CoalesceTenEvents | 32.91 ns | - |
| PrepareTenEvents | 23,753.68 ns | 20368 B |
| ReadAndValidateEvent | 2,132.39 ns | 1624 B |
| MapAccountEvent | 2,721.06 ns | 3632 B |
| MapPayloadToSObject | 41.43 ns | 200 B |
| MapSObjectToRecord | 423.85 ns | 896 B |
| DiffNumber | 206.45 ns | 32 B |
| DiffDateTime | 413.62 ns | 192 B |

## Drain benchmarks

### Current (EF Core) before and after the SQL changes

"Before" is the EF Core version with the original claim and outcome SQL (plus the claim race fix), "after" the
tuned SQL; same benchmark code, run one after the other. Times are BenchmarkDotNet's; the work columns are medians of
the per-iteration counters (API calls, write requests, records written, entity syncs, events per entity sync,
claims, relationship retries).

After run: 196 iterations (warm-up included), 0 with a failed check. Before run: the same.

| Mode | W | P | Accounts | Lat | Mean | Median | Min–Max | Allocated | API calls | Write req | Records ok | Entity syncs | Coalescing | Claims | Retries | Before mean | Before median |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| preloaded | 1 | 1 | 100 | 0 | 1,590.0 ms | 1.330 s | 0.872 s–2.674 s | 106.57 MB | 26 | 13 | 1138 | 878 | 5.69 | 928 | 50 | 1,763.9 ms | 1.527 s |
| preloaded | 1 | 1 | 100 | 25 | 2,087.9 ms | 1.986 s | 1.859 s–2.406 s | 106.62 MB | 26 | 13 | 1138 | 878 | 5.69 | 928 | 50 | 2,817.8 ms | 2.866 s |
| preloaded | 1 | 1 | 1000 | 0 | 3,726.7 ms | 3.366 s | 2.842 s–4.828 s | 434 MB | 58 | 29 | 2592 | 1618 | 3.09 | 2114 | 496 | 2,078.0 ms | 1.917 s |
| preloaded | 1 | 1 | 1000 | 25 | 4,605.9 ms | 4.685 s | 3.467 s–5.585 s | 433.75 MB | 58 | 29 | 2592 | 1618 | 3.09 | 2114 | 496 | 5,441.1 ms | 5.966 s |
| preloaded | 1 | 3 | 100 | 0 | 458.0 ms | 440.893 ms | 415.212 ms–558.804 ms | 109.92 MB | 35 | 17 | 1138 | 878 | 5.69 | 953 | 75 | 727.7 ms | 649.383 ms |
| preloaded | 1 | 3 | 100 | 25 | 711.1 ms | 702.821 ms | 672.488 ms–778.615 ms | 110.83 MB | 35 | 17 | 1138 | 878 | 5.69 | 953 | 75 | 903.9 ms | 922.593 ms |
| preloaded | 1 | 3 | 1000 | 0 | 1,724.9 ms | 1.087 s | 0.992 s–4.381 s | 436.67 MB | 56 | 28 | 2592 | 1618 | 3.09 | 2099 | 481 | 1,572.5 ms | 1.791 s |
| preloaded | 1 | 3 | 1000 | 25 | 2,597.8 ms | 2.728 s | 1.503 s–3.609 s | 435.37 MB | 56 | 28 | 2592 | 1618 | 3.09 | 2099 | 481 | 2,273.9 ms | 2.434 s |
| preloaded | 4 | 1 | 100 | 0 | 1,178.8 ms | 0.989 s | 0.758 s–2.005 s | 114.34 MB | 34 | 16 | 1138 | 878 | 5.69 | 1028 | 150 | 2,738.7 ms | 2.724 s |
| preloaded | 4 | 1 | 100 | 25 | 2,000.1 ms | 2.101 s | 1.768 s–2.233 s | 117.1 MB | 34 | 16 | 1138 | 878 | 5.69 | 1028 | 150 | 2,329.3 ms | 1.714 s |
| preloaded | 4 | 1 | 1000 | 0 | 1,989.7 ms | 2.084 s | 1.528 s–2.505 s | 439.76 MB | 56 | 27 | 2592 | 1618 | 3.09 | 2138 | 520 | 1,351.5 ms | 1.205 s |
| preloaded | 4 | 1 | 1000 | 25 | 1,906.2 ms | 1.725 s | 1.507 s–2.710 s | 439 MB | 56 | 27 | 2592 | 1618 | 3.09 | 2156 | 538 | 1,237.5 ms | 1.252 s |
| preloaded | 4 | 3 | 100 | 0 | 1,130.5 ms | 1.134 s | 0.991 s–1.233 s | 152.87 MB | 48 | 21 | 1138 | 878 | 5.69 | 1496 | 618 | 5,321.4 ms | 5.887 s |
| preloaded | 4 | 3 | 100 | 25 | 2,163.7 ms | 2.093 s | 1.483 s–2.930 s | 149.02 MB | 45 | 18 | 1138 | 878 | 5.69 | 1569 | 691 | 1,924.5 ms | 1.396 s |
| preloaded | 4 | 3 | 1000 | 0 | 1,502.9 ms | 1.409 s | 1.310 s–1.834 s | 453.67 MB | 62 | 28 | 2592 | 1618 | 3.09 | 2196 | 578 | 1,152.5 ms | 1.176 s |
| preloaded | 4 | 3 | 1000 | 25 | 1,440.0 ms | 1.325 s | 1.247 s–1.786 s | 456.02 MB | 64 | 29 | 2592 | 1618 | 3.09 | 2243 | 625 | 1,331.0 ms | 1.333 s |
| preloaded | 8 | 1 | 100 | 0 | 837.2 ms | 842.810 ms | 802.307 ms–853.310 ms | 135.61 MB | 40 | 17 | 1138 | 878 | 5.69 | 1328 | 450 | 3,877.1 ms | 4.195 s |
| preloaded | 8 | 1 | 100 | 25 | 1,045.6 ms | 1.100 s | 0.819 s–1.138 s | 137.92 MB | 40 | 17 | 1138 | 878 | 5.69 | 1328 | 450 | 4,514.9 ms | 3.776 s |
| preloaded | 8 | 1 | 1000 | 0 | 1,934.9 ms | 2.015 s | 1.243 s–3.032 s | 447.64 MB | 60 | 28 | 2592 | 1618 | 3.09 | 2181 | 563 | 9,392.5 ms | 3.752 s |
| preloaded | 8 | 1 | 1000 | 25 | 2,157.4 ms | 1.678 s | 1.095 s–3.882 s | 455.21 MB | 60 | 28 | 2592 | 1618 | 3.09 | 2190 | 572 | 5,972.0 ms | 4.715 s |
| preloaded | 8 | 3 | 100 | 0 | 1,750.5 ms | 1.821 s | 1.489 s–1.915 s | 176.62 MB | 56 | 22 | 1138 | 878 | 5.69 | 1625 | 747 | 21,879.8 ms | 16.137 s |
| preloaded | 8 | 3 | 100 | 25 | 1,972.7 ms | 1.862 s | 1.800 s–2.207 s | 195.35 MB | 55 | 22 | 1138 | 878 | 5.69 | 1708 | 830 | 6,712.2 ms | 5.498 s |
| preloaded | 8 | 3 | 1000 | 0 | 2,138.4 ms | 2.121 s | 1.848 s–2.466 s | 519.18 MB | 76 | 34 | 2592 | 1618 | 3.09 | 2707 | 1089 | 4,952.8 ms | 5.014 s |
| preloaded | 8 | 3 | 1000 | 25 | 1,648.5 ms | 1.617 s | 1.556 s–1.846 s | 482.45 MB | 71 | 31 | 2592 | 1618 | 3.09 | 2673 | 1055 | 16,877.9 ms | 8.031 s |
| streaming | 4 | 1 | 50 | 5 | 1.497 s | 1.461 s | 1.357 s–1.638 s | 142.33 MB | 205 | 102 | 1402 | 998 | 5.01 | 1011 | 23 | 2.917 s | 2.436 s |
| streaming | 4 | 3 | 50 | 5 | 3.432 s | 4.046 s | 1.739 s–4.855 s | 179.51 MB | 287 | 142 | 1744 | 1185 | 4.22 | 1223 | 35 | 3.356 s | 3.442 s |
| streaming | 8 | 1 | 50 | 5 | 2.777 s | 2.896 s | 1.546 s–3.380 s | 176.29 MB | 278 | 138 | 1961 | 1294 | 3.86 | 1322 | 24 | 3.499 s | 3.522 s |
| streaming | 8 | 3 | 50 | 5 | 2.948 s | 2.716 s | 2.451 s–3.561 s | 207.52 MB | 315 | 151 | 1381 | 1004 | 4.98 | 1094 | 102 | 5.067 s | 4.738 s |

Same work on both sides (identical API calls, claims and retries per case); the differences in time are mostly
machine load (load average 15 to 35 during these runs). The before side's long runs (8 workers x 3 pods, 16 s and
8 s medians) look like the stale-statistics effect described below.

### Earlier run (Dapper version, quieter machine)

The same comparison on the Dapper version (before: untuned SQL; after: tuned), preloaded cases only:

| W | P | Accounts | Lat | Median | API calls | Claims | Retries | Before median |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 | 1 | 100 | 0 | 783.833 ms | 26 | 928 | 50 | 1.123 s |
| 1 | 1 | 100 | 25 | 1.443 s | 26 | 928 | 50 | 1.824 s |
| 1 | 1 | 1000 | 0 | 1.877 s | 58 | 2114 | 496 | 2.557 s |
| 1 | 1 | 1000 | 25 | 3.217 s | 58 | 2114 | 496 | 3.696 s |
| 1 | 3 | 100 | 0 | 564.262 ms | 37 | 1022 | 144 | 606.330 ms |
| 1 | 3 | 100 | 25 | 791.986 ms | 37 | 1016 | 138 | 932.375 ms |
| 1 | 3 | 1000 | 0 | 793.299 ms | 56 | 2093 | 475 | 1.005 s |
| 1 | 3 | 1000 | 25 | 1.277 s | 56 | 2099 | 481 | 1.375 s |
| 4 | 1 | 100 | 0 | 521.478 ms | 37 | 1096 | 218 | 551.635 ms |
| 4 | 1 | 100 | 25 | 758.144 ms | 37 | 1096 | 218 | 799.665 ms |
| 4 | 1 | 1000 | 0 | 780.743 ms | 56 | 2128 | 510 | 907.149 ms |
| 4 | 1 | 1000 | 25 | 0.952 s | 56 | 2156 | 538 | 1.084 s |
| 4 | 3 | 100 | 0 | 859.884 ms | 51 | 1571 | 693 | 845.496 ms |
| 4 | 3 | 100 | 25 | 867.654 ms | 46 | 1565 | 687 | 0.968 s |
| 4 | 3 | 1000 | 0 | 852.864 ms | 62 | 2211 | 593 | 8.319 s |
| 4 | 3 | 1000 | 25 | 885.883 ms | 60 | 2239 | 621 | 0.857 s |
| 8 | 1 | 100 | 0 | 658.402 ms | 43 | 1328 | 450 | 653.485 ms |
| 8 | 1 | 100 | 25 | 879.461 ms | 42 | 1397 | 519 | 798.892 ms |
| 8 | 1 | 1000 | 0 | 806.941 ms | 60 | 2133 | 515 | 775.099 ms |
| 8 | 1 | 1000 | 25 | 827.352 ms | 60 | 2150 | 532 | 903.088 ms |
| 8 | 3 | 100 | 0 | 1.561 s | 64 | 1768 | 890 | 1.412 s |
| 8 | 3 | 100 | 25 | 1.779 s | 69 | 1774 | 896 | 1.463 s |
| 8 | 3 | 1000 | 0 | 1.236 s | 76 | 2622 | 1004 | 1.122 s |
| 8 | 3 | 1000 | 25 | 1.113 s | 73 | 2558 | 940 | 1.143 s |

The before side had 8 to 10 s iterations (4 workers x 3 pods x 1,000 accounts: median 8.3 s, 0.97 to 9.9 s;
the 25 ms case up to 10.2 s). They coincide with autoanalyze running between iterations (21 autoanalyzes in the
run) and leaving statistics that describe a finished inbox while the next iteration loads a backlog: the stale
case above, where the claim took seconds. With the pins they are gone (0.85 s and 0.98 s).

### Streaming

Four alternating after/before runs (A to C on the Dapper version, D on EF Core), median per case:

| Run | W | P | After median | Before median |
|---|--:|--:|--:|--:|
| A | 4 | 1 | 1.526 s | 1.117 s |
| A | 4 | 3 | 2.055 s | 1.332 s |
| A | 8 | 1 | 1.454 s | 1.250 s |
| A | 8 | 3 | 2.756 s | 3.986 s |
| B | 4 | 1 | 1.941 s | 1.509 s |
| B | 4 | 3 | 1.751 s | 1.601 s |
| B | 8 | 1 | 1.266 s | 1.351 s |
| B | 8 | 3 | 2.010 s | 2.121 s |
| C | 4 | 1 | 1.391 s | 1.346 s |
| C | 4 | 3 | 1.802 s | 1.889 s |
| C | 8 | 1 | 1.281 s | 2.666 s |
| C | 8 | 3 | 2.068 s | 3.858 s |
| D (EF) | 4 | 1 | 1.461 s | 2.436 s |
| D (EF) | 4 | 3 | 4.046 s | 3.442 s |
| D (EF) | 8 | 1 | 2.896 s | 3.522 s |
| D (EF) | 8 | 3 | 2.716 s | 4.738 s |

Within noise: streaming keeps the inbox small, so the claim is cheap either way.

### Observations

- More workers on 100 accounts is slower, not faster: Profiles wait for their related Accounts
  (`DEPENDENCY_NOT_SYNCED`, 450 to 990 retries with 8 workers) and idle workers poll every 2 ms. A property of
  the benchmark's data (100 accounts, many relationships), not of the claim.
- Allocation grows with retries and claims, not with pods.

## Sequential, limiter-only vs full

This inbox sync core branch has no rate limiter; both versions compared here are `main`'s, which has one, and the
numbers were measured on `main`.

Is the parallel processing and race-condition handling worth it? The branch `benchmark/sequential-limiter-only` has
a version with one worker loop per process and no leases, claim lock or concurrency slots, only the rate limiter.
Everything else is the same as `main` (01b53b3). Both versions drained the same 5,000 events with the same
correctness checks, back to back, on 2026-10-14. The full report, with every configuration, allocations and load
averages, is [on that branch](https://github.com/mcdmags/salesforce-kafka-inbox/blob/benchmark/sequential-limiter-only/docs/benchmarks/sequential-vs-full.md).

**Times are only indicative.** The machine was busy (load average 9 to 27 during the runs), and two runs of the same
sequential code differed by up to 6x. The work counts (API calls, write requests, coalescing) were identical in every
iteration of a configuration and are the reliable part.

Mean time per drain of 5,000 events, and API calls per drain (latency 0 ms / 25 ms per simulated Salesforce call):

| Version | Pods x workers | 100 accounts | 1,000 accounts | API calls (100 / 1,000 accounts) |
|---|---|---|---|---|
| Sequential | 1 x 1 | 0.64 s / 1.57 s (second run: 3.82 s / 4.93 s) | 1.52 s / 2.89 s (second run: 4.85 s / 3.27 s) | 28 / 56 |
| Full | 1 x 1 | 1.55 s / 2.04 s | 3.52 s / 4.10 s | 26 / 58 |
| Full | 1 x 4 | 0.70 s / 0.96 s | 1.37 s / 1.25 s | 34 to 38 / 56 |
| Full | 1 x 8 | 0.94 s / 1.01 s | 1.03 s / 1.36 s | 38 to 44 / 54 to 62 |
| Full | 3 x 4 | 1.77 s / 1.63 s | 0.91 s / 1.09 s | 39 to 49 / 54 to 60 |

Coalescing was the same in both versions: 5.69 events per entity sync with 100 accounts, 3.09 with 1,000. Every
iteration above passed its correctness checks.

**Race demo.** The sequential version with 3 pods on one inbox, nothing keeping the workers off each other's
entities:

| Accounts | Latency | Result |
|---|---|---|
| 100 | 0 ms | Check failed: **396 duplicate writes**, 633 records rejected |
| 100 | 25 ms | Check failed: **620 duplicate writes**, 1,076 records rejected |
| 1,000 | 0 ms | Final data correct, but 127 to 139 API calls (56 for one worker), 1,140 to 1,565 rejected creates, 6.96 s |
| 1,000 | 25 ms | Final data correct, but 145 to 161 API calls, 2,059 to 2,760 rejected creates, 8.68 s |

No older version overwrote a newer one in these runs, but nothing in the sequential version prevents it.

What it means:

- **One sequential worker is enough** for a single process at a modest event rate: it makes the same API calls as
  the full version with one worker and coalesces as much, with no locking code. Its throughput is one batch at a
  time, so it slows down as Salesforce latency and the number of distinct entities grow.
- **Parallel workers buy throughput, not efficiency.** The full version with 4 or 8 workers, or 3 pods x 4, drained
  1.25x to 3.9x faster than its own single worker (one exception: 3 x 4 with 100 accounts at 0 ms), at the cost of a
  few more API calls (more, smaller batches). With real Salesforce latency (hundreds of ms per call) the gain grows.
- **The race handling is what makes more than one worker safe.** Without it, a second worker or pod caused hundreds
  of duplicate writes or create collisions and up to 2.9x the API calls. The sequential version must run as exactly
  one worker.

## CoalesceDelay sweep

2026-10-16, `main` at 4036d77 (PR #41), the environment above; load average 8.6 to 15 during the streaming runs (one
spike to 23 in a drain run). `StreamingIngestBenchmarks` (50 accounts, 5,000 events, 5 ms latency) with
`CoalesceDelayMs` swept, 3 measured iterations per run, each setting run twice in alternating order (ascending, then
descending) on a fresh Postgres. Cells are run 1 / run 2: the median `api=` (calls to the org) and `coalescing=`
(events per entity sync) of the measured iterations, the BenchmarkDotNet mean and allocated. Every iteration of every
run: 0 overlapping claims, 0 not done, 0 wrong, 0 regressions.

| Delay | 4 workers x 1 pod: api | coalescing | mean | allocated |
|---|---|---|---|---|
| 0 | 230 / 233 | 4.50 / 4.51 | 2.04 s / 1.44 s | 105.6 / 107.4 MB |
| 25 ms | 231 / 223 | 4.44 / 4.60 | 3.15 s / 1.23 s | 105.5 / 106.4 MB |
| **50 ms** | **203 / 206** | **5.02 / 5.02** | 1.07 s / 0.99 s | 99.9 / 104.7 MB |
| 100 ms | 201 / 201 | 5.04 / 5.12 | 1.21 s / 1.10 s | 100.5 / 101.9 MB |
| 250 ms | 181 / 214 | 5.81 / 5.09 | 1.14 s / 1.52 s | 96.6 / 109.4 MB |
| 500 ms | 171 / 173 | 6.31 / 6.22 | 1.62 s / 1.62 s | 103.0 / 100.0 MB |

| Delay | 4 workers x 3 pods: api | coalescing | 8 workers x 3 pods: api | coalescing |
|---|---|---|---|---|
| 0 | 303 / 294 | 4.01 / 4.14 | 310 / 314 | 4.99 / 5.02 |
| 25 ms | 312 / 305 | 4.08 / 4.05 | 313 / 319 | 5.00 / 4.97 |
| **50 ms** | 311 / 308 | 4.10 / 4.13 | 307 / 313 | 5.14 / 4.95 |
| 100 ms | 290 / 300 | 4.30 / 4.25 | 314 / 310 | 5.10 / 5.18 |
| 250 ms | 294 / 303 | 4.64 / 4.45 | 307 / 295 | 5.34 / 5.45 |
| 500 ms | 276 / 311 | 5.48 / 5.14 | 332 / 328 | 5.51 / 5.73 |

Means for 3 pods: 1.2 to 2.8 s at every delay, with no trend (noise); allocated 139 to 160 MB (4 x 3) and 170 to
188 MB (8 x 3) up to 250 ms, 167 to 194 MB and 238 MB at 500 ms.

- **4 x 1**: 50 ms takes the calls from about 231 to 205 (11% fewer) and the coalescing from 4.5 to 5.0; 100 ms is
  the same, 250 ms is noisy (181 and 214), and only 500 ms goes further (172, 26% fewer).
- **3 pods**: no delay changes the calls beyond the run-to-run spread (about 3%), though entity syncs fall (coalescing
  rises) from 250 ms. A call is per batch, not per entity: with 12 or 24 loops on 50 accounts the batches are small,
  and a delay makes them smaller rather than fewer. At 500 ms the 8 x 3 case made 6% more calls and allocated a third
  more.
- The benchmark streams 5,000 events in one to two seconds (50 to 100 events per account per second), so 500 ms is a
  large share of a run; real bursts are slower, and a delay costs every entity up to that much sync latency.

**Chosen default: 50 ms** (`Sync:CoalesceDelay` `00:00:00.050`): the smallest delay with the 4 x 1 reduction, as
good as 100 ms, no worse with 3 pods, and 50 ms of added latency at most. 500 ms would save more calls in one case
but costs ten times the latency and more calls and memory with 8 x 3.

Preloaded drains (the quick `InboxDrainBenchmarks`, 100 accounts, no latency), 0 vs 50 ms, twice each, alternating:
the same work, as expected (the whole inbox is due 50 ms later, then drains as before). Medians (pods x workers):

| Delay | 1 x 1: api | 1 x 4 | 3 x 1 | 3 x 4 | coalescing | mean 1 x 1 / 1 x 4 / 3 x 1 / 3 x 4 |
|---|---|---|---|---|---|---|
| 0, run 1 | 30 | 35 | 33 | 41 | 5.69 | 546 / 377 / 323 / 474 ms |
| 50 ms, run 1 | 30 | 34 | 33 | 43 | 5.69 | 1,385 / 740 / 902 / 3,957 ms (load spike to 23) |
| 0, run 2 | 30 | 34 | 33 | 42 | 5.69 | 613 / 354 / 356 / 470 ms |
| 50 ms, run 2 | 30 | 34 | 33 | 44 | 5.69 | 731 / 347 / 288 / 457 ms |

Allocated 48 to 84 MB in all four runs; 0 overlapping claims, not done, wrong or regressions.

## Micro benchmarks

| Method               | Mean         | Error        | StdDev       | Gen0   | Gen1   | Allocated |
|---|---|---|---|---|---|---|
| CoalesceTenEvents    |     46.64 ns |     2.159 ns |     6.090 ns |      - |      - |         - |
| PrepareTenEvents     | 36,475.42 ns | 1,810.361 ns | 5,135.692 ns | 2.4414 | 0.0916 |   20480 B |
| ReadAndValidateEvent |  3,156.44 ns |   110.016 ns |   317.421 ns | 0.1945 |      - |    1632 B |
| MapAccountEvent      |  5,065.07 ns |   333.269 ns |   961.557 ns | 0.4272 |      - |    3624 B |
| MapPayloadToSObject  |     55.37 ns |     2.642 ns |     7.708 ns | 0.0238 |      - |     200 B |
| MapSObjectToRecord   |    609.29 ns |    13.783 ns |    37.731 ns | 0.1059 |      - |     888 B |
| DiffNumber           |    309.02 ns |     6.753 ns |    19.267 ns | 0.0038 |      - |      32 B |
| DiffDateTime         |    625.71 ns |    16.431 ns |    48.188 ns | 0.0229 |      - |     192 B |

## Remaining risks

(As of the `sf_sync_state` claim and one inbox row per entity.)

- **Overwritten rows**: every event of an entity rewrites its one inbox row (a new row version and index entries,
  but no new row), and a claimed row overwritten meanwhile makes the outcome transaction skip it and the entity due
  again: one more claim, not one more write of the old event. Not measured yet; re-run the streaming benchmark
  before comparing its claims with the numbers above.
- **Statistics**: no planner settings are pinned; every statement is written so that its only sensible plan is an
  index lookup by key. A new statement must be checked with stale statistics too (idle-then-backlog and
  backlog-then-drained), or it can reintroduce a plan that scans a whole table or index.
- **Isolation**: the claim relies on READ COMMITTED: a state row leased by another worker after the claim's snapshot
  is re-checked on its new version when it is locked. Under REPEATABLE READ that would be a serialization error
  instead.
- **Noise**: the drain numbers above were measured on a busy machine; compare work counts first and times only
  across alternating runs.
