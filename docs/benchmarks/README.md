# Benchmarks

Measurements of the Postgres inbox (claim and outcome SQL) and of the whole sync path (Postgres inbox, workers,
Salesforce client and rate limiter real; only the Salesforce HTTP endpoint mocked). How to run them is in the
[main README](../../README.md#benchmarks).

These numbers come from the claim and outcome tuning merged in PR #13, re-measured after the move to EF Core
(PR #12). [Latest run](#latest-run-main-at-9a61674) re-measures everything on `main` at 9a61674 (the API path and version in
`appsettings.json`);
the [run before it](#latest-run-main-at-f844961) measured `main` at f844961 (PR #72, rate limits as a deploy SQL script);
the [run before that](#latest-run-main-at-f80334e) measured `main` at f80334e (PR #71, rate limits applied by the migrate
step);
the [run before that](#latest-run-main-at-df2c269) measured `main` at df2c269 (PR #70, migrations once per deploy);
the [run before that](#latest-run-main-at-3ff8ae1) measured `main` at 3ff8ae1 (PR #67 to #69, the split into projects);
the [run before that](#latest-run-main-at-93b4ace) measured `main` at 93b4ace (PR #67);
the [run before that](#latest-run-main-at-3f68779) measured `main` at 3f68779 (PR #66);
the [run before that](#latest-run-main-at-b5950c1) measured `main` at b5950c1 (PR #47 to #65);
the [run before that](#latest-run-main-at-2d20c16) measured `main` at 2d20c16 (PR #35 to #46);
the [run before that](#latest-run-main-at-7a145b0-after-the-simplification) measured `main` at 7a145b0 (PR #20 to #24).

**Every number on this page describes an earlier design**, kept as its record. Since it was measured:

- **The claim moved into the inbox row.** Entities were claimed by locking and leasing their row in one
  `sf_sync_state` table; that table is gone (migration `StateInInboxRow`), and an entity is claimed by locking and
  leasing its own inbox row (see the [main README](../../README.md#the-inbox)). The claim statements measured below
  (LockHeads, the claim lock, `InboxSql.PinPlans`, the lease and due-time columns of `sf_sync_state` and their
  indexes) no longer exist.
- **One row per entity, not per event** (migration `InboxOneRowPerRecord`): a newer event overwrites the entity's row
  and `Sync:MaxEventsPerEntity` is gone. The counts of `done` and `superseded` rows, the coalescing as events per done
  row, the [leasing cost by rows leased](#leasing-cost-by-rows-leased) and the preloaded drains of 5,000 rows describe
  the old design. The drain benchmarks now preload each entity's newest event only (100 or 1,000 account rows, plus
  relationships) and print `entities` (inbox rows) and the coalescing as events per entity claim.
- **The lease is the row's `next_attempt_at`** (migration `LeaseInNextAttemptAt`, no `locked_until`).
- **No watermark and no `superseded` status** (migration `DropSyncedTime`, no `synced_time`): an event not newer
  than a synced one is never stored, so every row of a drained inbox ends `done`.
- **No `Sync:CoalesceDelay`**: a stored event makes its entity due at once (see the
  [realistic-ingest sweep](#realistic-ingest-sweep-2026-10-16-main-at-85d1de7)).
- **The drain workload is account aggregates, with deletes** (see [below](#the-drain-workload)): every number
  below was measured with per-entity Account and relationship events built by hand, no contacts and no deletes.

Re-run the benchmarks before comparing any of these numbers with the current code.

[Sequential, limiter-only vs full](#sequential-limiter-only-vs-full) compares this version with one that has no parallel
processing or race-condition handling, measured on `main` at 01b53b3.

## The drain workload

Since the deletes (PR #65) the drain and streaming benchmarks generate what the upstream publishes, **account
aggregates** (`AggregateWorkload`: `{ meta, before, after }`, the Account with its Profile, relationships and
contacts), and store what `AccountAggregate.TrySplit` makes of each, read and stored through each type's
`ISalesforceEntitySync` exactly as the Kafka consumer does (streaming) or as storing them in order would leave the
inboxes (preloaded). `Events` (5,000) counts aggregates. Each one changes its Account (Description and Profile score
`vN`); 15% also touch one of the account's relationships and 15% one of its 3 contacts, a touched child it already
has being removed (a delete) 30% of the time and changed otherwise, so removed children come back later as new
records; a tenth of the accounts, never named by another account's relationship, may be deleted with all their
children (2% of their aggregates once they have two versions; no aggregate follows a delete). Seeded (42): every
iteration gets the same aggregates. 5,000 aggregates split into these entity events (counted from the generator,
not a benchmark run), about a quarter more inbox work than the 5,000 hand-built events before:

| Accounts | Entity events | Account | Contact | Relationship | Deletes | Accounts deleted | Entities absent at the end |
|--:|--:|--:|--:|--:|--:|--:|--:|
| 50 (streaming) | 6,448 | 5,000 | 751 | 697 | 198 | 5 | 92 of 762 |
| 100 | 6,453 | 5,000 | 777 | 676 | 179 | 6 | 98 of 990 |
| 1,000 | 6,227 | 5,000 | 730 | 497 | 36 | 4 | 33 of 2,113 |

The check, every iteration: every account, relationship and contact the aggregates end with is in the org at its
newest version (a relationship or contact naming the right Accounts), every removed or deleted one is absent (an
Account with its Profile), no record's write history goes backwards (a re-added child is a new record with a
history of its own), every entity's one inbox row holds its newest event and is `done`, and no two claims of one
entity overlapped. The `//` line adds `entityEvents`, `contactEvents`, `deleteEvents` and `deletedEntities`;
the org's counters print `deletes=N (requests=M)`. A unit test (`AggregateWorkloadTests`) checks that the expected
end state is what the split events lead to, and that the check passes an org that got the events in order and
fails a stale record and a missed delete.

**Results pending**: the benchmarks have not been run on this workload yet; the numbers on this page are from the
earlier workload.

## Environment

| | |
|---|---|
| BenchmarkDotNet | v0.14.0 |
| OS | macOS 26.6.2 (25G83), Darwin 25.6.0 |
| CPU | Apple M4, 10 cores |
| .NET | SDK 10.0.100, runtime .NET 10.0.2 Arm64 (Release); runs before the .NET 10 upgrade used the .NET 8.0.17 runtime |
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
- **Fail and Dead** bind the error list once (`(SELECT @errors::jsonb)`, an InitPlan) instead of a CTE the
  planner copied per row.
- **ReleaseLostClaims** finds the claim's rows by `entity_id = ANY(@entityIds)` on the leased index instead of
  scanning the ready index.
- **MarkSynced** probes one lease per entity (`ORDER BY l.entity_id, l.locked_until LIMIT 1`).
- **Watermark in the inbox** (after these measurements, and since removed with `synced_time`). `InboxSql.Done`
  completed the synced rows and set their `synced_time` in one statement, found by primary key under the lease token
  like Fail and Dead (it replaced MarkSynced and the done Complete); `SyncedTimes` read each claimed entity's
  watermark with one backward probe of a partial index. The MarkSynced, Complete and SyncedTimes rows below were
  measured before this change.
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
| Defer | 21.3 | 107 | 2 | 21.6 | 324 | 1 |
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
| Defer | 75 | 3.0 | Index Scan pkey (7), Bitmap Heap Scan (5) | 2.2 | Index Scan pkey (12) |
| Defer | 1,000 | 249 | Index Scan pkey (7), Bitmap Heap Scan (5) | 55.5 | Index Scan pkey (12) |
| Defer | 5,000 | 757 | Index Scan pkey (7), Bitmap Heap Scan (5) | 420 | Index Scan pkey (12) |
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

## Latest run: plain-crud with the record type cache on HybridCache, at 69f53f7

All 36 benchmarks (`--run full`) on the `plain-crud` branch at 69f53f7, on 2026-10-19, against the
[run before it](#latest-run-plain-crud-without-describe-at-54c47ac). Since then: the record type cache is a `HybridCache`
(in memory over the shared `IDistributedCache`, the memory one in the benchmarks), the `RecordType` rows are read into a
typed row, `last_error_code` and `status` are gone from the inbox, and the inbox statements are in the methods that run
them (`ReleaseAsync` replaces hold, defer and halt). Same workload and environment; the database was recreated.

**Correctness**: 196 iterations (warm-up included), 0 failures.

Preloaded drain, 5,000 events (medians of the iterations without a `RecordType` query, at 0 ms latency), and streaming
(50 accounts, 5 ms latency, 762 entities):

| W | P | Accounts | Entities | Mean 0 ms | Mean 25 ms | Allocated | API calls | Write req | Coalescing | Claims | Retries |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 | 1 | 100 | 990 | 151.71 ms | 1,576.12 ms | 26.01 MB | 47 | 17 | 6.28 | 1,028 | 38 |
| 1 | 1 | 1,000 | 2,113 | 542.35 ms | 5,608.60 ms | 80.79 MB | 151 | 47 | 1.90 | 3,279 | 1166 |
| 1 | 3 | 100 | 990 | 66.87 ms | 546.84 ms | 26.16 MB | 47 | 17 | 6.52 | 990 | 0 |
| 1 | 3 | 1,000 | 2,113 | 348.07 ms | 1,850.47 ms | 82.49 MB | 157 | 49 | 1.90 | 3,279 | 1166 |
| 4 | 1 | 100 | 990 | 64.41 ms | 442.92 ms | 28.11 MB | 51 | 17 | 5.78 | 1,117 | 127 |
| 4 | 1 | 1,000 | 2,113 | 312.82 ms | 1,242.22 ms | 82.19 MB | 155 | 49 | 1.90 | 3,268 | 1155 |
| 4 | 3 | 100 | 990 | 96.70 ms | 264.11 ms | 43.75 MB | 51 | 17 | 5.77 | 1,119 | 129 |
| 4 | 3 | 1,000 | 2,113 | 316.54 ms | 545.00 ms | 91.73 MB | 166 | 54 | 1.98 | 3,146 | 1033 |
| 8 | 1 | 100 | 990 | 71.94 ms | 278.73 ms | 27.23 MB | 49 | 17 | 5.88 | 1,097 | 107 |
| 8 | 1 | 1,000 | 2,113 | 343.00 ms | 746.66 ms | 89.11 MB | 166 | 51 | 1.86 | 3,346 | 1233 |
| 8 | 3 | 100 | 990 | 168.83 ms | 325.45 ms | 62.87 MB | 58 | 18 | 4.99 | 1,323 | 333 |
| 8 | 3 | 1,000 | 2,113 | 563.46 ms | 699.58 ms | 117.71 MB | 166 | 52 | 1.90 | 3,275 | 1162 |

| W | P | Mean | Allocated | API calls | Write req | Coalescing | Claims | Retries |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 4 | 1 | 1.742 s | 175.48 MB | 1,158 | 526 | 2.54 | 2,543 | 6 |
| 4 | 3 | 2.436 s | 368.87 MB | 4,482 | 1,950 | 1.50 | 4,298 | 4 |
| 8 | 1 | 2.064 s | 276.83 MB | 2,626 | 1,153 | 1.69 | 3,801 | 4 |
| 8 | 3 | 5.369 s | 1250.13 MB | 15,041 | 6,891 | 1.21 | 5,327 | 3 |

- **No change from the cache or the inbox columns.** The API calls and write requests are the same as in the run before
  (47 and 17 with one worker and one pod, 100 accounts), and the means are within the noise of three iterations each
  (152 ms against 159 ms, 1.58 s against 1.64 s at 25 ms). Allocations are about 1 MB higher in the smaller drains, which
  is `HybridCache`'s bookkeeping.
- **Streaming with 8 workers on 3 pods is still the outlier** (5.4 s, 15,041 API calls, coalescing 1.21), as in the run
  before; the other three configurations are 1.7 to 2.4 s. See that run for the guess about the per-process limiter.

## Latest run: plain-crud without Describe, at 54c47ac

All 36 benchmarks (`--run full`) on the `plain-crud` branch at 54c47ac, on 2026-10-19, against the
[run before it](#latest-run-plain-crud-with-the-mapping-error-halt-at-15a8e22). Since then: the rate limiter and its
Postgres tables are gone (each process has its own token bucket, pause and throttle from `Sforce-Limit-Info`), Describe
is gone (record type Ids come from a cached `RecordType` query, values are compared by their .NET type), and the inbox
lost its `status` and `last_error_code` columns. Same workload and environment; the database was recreated.

**Correctness**: 196 iterations (warm-up included), 0 failures.

Preloaded drain, 5,000 events (medians of the iterations without a `RecordType` query, at 0 ms latency):

| W | P | Accounts | Entities | Mean 0 ms | Mean 25 ms | Allocated | API calls | Write req | Coalescing | Claims | Retries |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 | 1 | 100 | 990 | 159.0 ms | 1,638.7 ms | 24.84 MB | 47 | 17 | 6.28 | 1,028 | 38 |
| 1 | 1 | 1,000 | 2,113 | 666.2 ms | 5,766.4 ms | 75.12 MB | 151 | 47 | 1.90 | 3,279 | 1166 |
| 1 | 3 | 100 | 990 | 64.4 ms | 534.6 ms | 24.94 MB | 47 | 17 | 6.52 | 990 | 0 |
| 1 | 3 | 1,000 | 2,113 | 252.4 ms | 1,822.3 ms | 76.3 MB | 157 | 49 | 1.90 | 3,274 | 1161 |
| 4 | 1 | 100 | 990 | 65.7 ms | 440.7 ms | 26.82 MB | 51 | 17 | 5.78 | 1,117 | 127 |
| 4 | 1 | 1,000 | 2,113 | 334.0 ms | 1,258.0 ms | 78.13 MB | 155 | 49 | 1.88 | 3,311 | 1198 |
| 4 | 3 | 100 | 990 | 85.5 ms | 261.5 ms | 29.63 MB | 49 | 17 | 6.08 | 1,061 | 71 |
| 4 | 3 | 1,000 | 2,113 | 372.5 ms | 559.3 ms | 87.73 MB | 155 | 52 | 2.05 | 3,036 | 923 |
| 8 | 1 | 100 | 990 | 71.8 ms | 286.7 ms | 34.23 MB | 53 | 17 | 5.53 | 1,183 | 193 |
| 8 | 1 | 1,000 | 2,113 | 350.8 ms | 717.4 ms | 81.79 MB | 163 | 50 | 1.86 | 3,342 | 1229 |
| 8 | 3 | 100 | 990 | 186.4 ms | 333.8 ms | 60.22 MB | 57 | 19 | 5.41 | 1,192 | 202 |
| 8 | 3 | 1,000 | 2,113 | 435.8 ms | 649.7 ms | 108.47 MB | 163 | 52 | 1.92 | 3,245 | 1132 |

Streaming (50 accounts, 5 ms latency, 762 entities):

| W | P | Mean | Allocated | API calls | Write req | Coalescing | Claims | Retries |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 4 | 1 | 1.777 s | 173.83 MB | 1,181 | 536 | 2.52 | 2,554 | 7 |
| 4 | 3 | 2.495 s | 361.82 MB | 4,563 | 1,986 | 1.49 | 4,319 | 4 |
| 8 | 1 | 2.081 s | 271.4 MB | 2,695 | 1,185 | 1.69 | 3,825 | 4 |
| 8 | 3 | 5.404 s | 1238.92 MB | 15,071 | 6,909 | 1.21 | 5,317 | 3 |

- **Preloaded drain is the same or faster.** The one-worker, one-pod run with 100 accounts makes the same 47 API calls as
  before and took 159 ms against 259 ms; the other configurations are within the noise of three iterations each. The
  `RecordType` query adds 4 calls to a first iteration (one per object that has a record type) and none after that.
- **The streaming run with 8 workers on 3 pods is much worse**: 5.4 s here and 6.4 s in a second run of the streaming
  benchmarks alone (about 16,000 API calls in both), against 3.6 s and 2,344 calls before. The other three streaming
  configurations are in line with the run before (1.8 to 2.5 s). This looks like the limiter, not Describe: the three
  pods of the benchmark used to share one bucket in Postgres, which held their total call rate down, and now each pod has
  its own, so 24 workers go at 50 hot accounts together. Coalescing falls to 1.21 events per claim, so nearly every event
  is its own claim, read and write. That is a guess from the counters; it was not tested by putting a shared limit back.
  With the production sizing (`Sync:Workers` x pods under 25, and Salesforce's own limits) the number to watch is the
  calls per event, not this benchmark's wall time.
- Allocations in the preloaded drain are 10 to 25% lower with one pod (no Describe, no limiter rows).

## Latest run: plain-crud with the mapping-error halt, at 15a8e22

All 36 benchmarks (`--run full`) on the `plain-crud` branch at 15a8e22, on 2026-10-19, against the
[run before it](#latest-run-plain-crud-with-lease-slots-at-aff3247). Since then: a mapping error stops the inbox (its rows
are held with the error on them), which adds nothing to the normal path, and the write path was trimmed. Same workload
and environment; the database was recreated.

**Correctness**: 196 iterations (warm-up included), 0 failures.

Preloaded drain, 5,000 events (medians of the iterations without a Describe, at 0 ms latency):

| W | P | Accounts | Entities | Mean 0 ms | Mean 25 ms | Allocated | API calls | Write req | Coalescing | Claims | Retries |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 | 1 | 100 | 990 | 258.7 ms | 1,741.2 ms | 29.34 MB | 47 | 17 | 6.28 | 1,028 | 38 |
| 1 | 1 | 1,000 | 2,113 | 813.7 ms | 6,290.9 ms | 89.9 MB | 151 | 47 | 1.90 | 3,279 | 1166 |
| 1 | 3 | 100 | 990 | 109.8 ms | 565.1 ms | 30.76 MB | 49 | 17 | 6.07 | 1,063 | 73 |
| 1 | 3 | 1,000 | 2,113 | 439.5 ms | 1,863.9 ms | 91.73 MB | 155 | 49 | 1.90 | 3,274 | 1161 |
| 4 | 1 | 100 | 990 | 115.5 ms | 464.4 ms | 31.68 MB | 51 | 17 | 5.77 | 1,119 | 129 |
| 4 | 1 | 1,000 | 2,113 | 398.9 ms | 1,307.1 ms | 94.86 MB | 155 | 49 | 1.91 | 3,260 | 1147 |
| 4 | 3 | 100 | 990 | 165.7 ms | 276.2 ms | 37.87 MB | 60 | 17 | 4.29 | 1,505 | 515 |
| 4 | 3 | 1,000 | 2,113 | 442.8 ms | 589.5 ms | 97.24 MB | 165 | 54 | 1.89 | 3,293 | 1180 |
| 8 | 1 | 100 | 990 | 144.0 ms | 314.0 ms | 38.06 MB | 56 | 16 | 4.64 | 1,392 | 402 |
| 8 | 1 | 1,000 | 2,113 | 366.3 ms | 824.3 ms | 94.59 MB | 161 | 49 | 1.85 | 3,363 | 1250 |
| 8 | 3 | 100 | 990 | 255.0 ms | 318.6 ms | 40.68 MB | 71 | 19 | 3.71 | 1,740 | 750 |
| 8 | 3 | 1,000 | 2,113 | 516.2 ms | 563.9 ms | 93.86 MB | 163 | 51 | 1.94 | 3,219 | 1106 |

Streaming (50 accounts, 5 ms latency, 762 entities):

| W | P | Mean | Allocated | API calls | Write req | Coalescing | Claims | Retries |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 4 | 1 | 2.322 s | 248.3 MB | 1,119 | 507 | 2.60 | 2,477 | 11 |
| 4 | 3 | 3.293 s | 444.45 MB | 2,481 | 1,083 | 1.85 | 3,494 | 6 |
| 8 | 1 | 3.183 s | 440.17 MB | 2,456 | 1,076 | 1.78 | 3,628 | 4 |
| 8 | 3 | 3.628 s | 414.7 MB | 2,344 | 1,009 | 2.22 | 2,907 | 8 |

- **No change from the halt.** With one worker and one pod the API calls are the same as before (47 and 151) and the
  means are within noise (259 and 814 ms at 0 ms latency, 1.74 s and 6.29 s at 25 ms).
- The 8 x 1 streaming run, which had a wide spread last time (4.965 s), is 3.183 s here, in line with the other
  configurations.

## Latest run: plain-crud with lease slots, at aff3247

All 36 benchmarks (`--run full`) on the `plain-crud` branch at aff3247, on 2026-10-19, against the
[run before it](#latest-run-plain-crud-no-composite-request). Two changes since: the concurrency slots are leases on
the `sf_rate_limit` row instead of session advisory locks, and the write path's classes are members of
`SalesforceBatchSyncService`. Same workload, environment and runtime; the database was recreated.

**Correctness**: 196 iterations (warm-up included), 0 failures.

Preloaded drain, 5,000 events (medians of the iterations without a Describe, at 0 ms latency):

| W | P | Accounts | Entities | Mean 0 ms | Mean 25 ms | Allocated | API calls | Write req | Coalescing | Claims | Retries |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 | 1 | 100 | 990 | 252.1 ms | 1,851.1 ms | 29.35 MB | 47 | 17 | 6.28 | 1,028 | 38 |
| 1 | 1 | 1,000 | 2,113 | 864.2 ms | 6,460.4 ms | 90.39 MB | 151 | 47 | 1.90 | 3,279 | 1166 |
| 1 | 3 | 100 | 990 | 115.7 ms | 575.9 ms | 30.84 MB | 49 | 17 | 6.07 | 1,063 | 73 |
| 1 | 3 | 1,000 | 2,113 | 439.2 ms | 1,814.4 ms | 93.74 MB | 156 | 49 | 1.90 | 3,274 | 1161 |
| 4 | 1 | 100 | 990 | 135.2 ms | 473.6 ms | 31.66 MB | 51 | 17 | 5.77 | 1,119 | 129 |
| 4 | 1 | 1,000 | 2,113 | 444.3 ms | 1,304.2 ms | 94.05 MB | 153 | 48 | 1.91 | 3,257 | 1144 |
| 4 | 3 | 100 | 990 | 159.7 ms | 281.5 ms | 32.72 MB | 59 | 17 | 4.48 | 1,439 | 449 |
| 4 | 3 | 1,000 | 2,113 | 430.0 ms | 568.3 ms | 99.98 MB | 170 | 57 | 2.01 | 3,097 | 984 |
| 8 | 1 | 100 | 990 | 136.7 ms | 307.9 ms | 35.68 MB | 54 | 16 | 4.81 | 1,342 | 352 |
| 8 | 1 | 1,000 | 2,113 | 435.7 ms | 832.4 ms | 95.58 MB | 161 | 50 | 1.83 | 3,396 | 1283 |
| 8 | 3 | 100 | 990 | 255.8 ms | 304.7 ms | 40.62 MB | 66 | 17 | 4.44 | 1,466 | 476 |
| 8 | 3 | 1,000 | 2,113 | 520.6 ms | 545.2 ms | 101.06 MB | 162 | 53 | 1.94 | 3,211 | 1098 |

Streaming (50 accounts, 5 ms latency, 762 entities):

| W | P | Mean | Allocated | API calls | Write req | Coalescing | Claims | Retries |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 4 | 1 | 2.298 s | 249.05 MB | 1,118 | 508 | 2.59 | 2,487 | 13 |
| 4 | 3 | 3.267 s | 442.56 MB | 2,477 | 1,081 | 1.86 | 3,458 | 4 |
| 8 | 1 | 4.965 s | 437.79 MB | 2,433 | 1,071 | 1.74 | 3,697 | 4 |
| 8 | 3 | 3.527 s | 418.58 MB | 2,359 | 1,016 | 2.21 | 2,923 | 6 |

- **Same work, same speed.** With one worker and one pod the API calls are identical (47 and 151), and the means are
  within run-to-run noise (235 to 252 ms and 822 to 864 ms at 0 ms latency; 1.80 to 1.85 s and 6.47 to 6.46 s at 25 ms).
  The two slot statements per batch are not visible next to the batch's Salesforce calls.
- **Less memory with several workers and pods**: 4 x 3 at 100 accounts allocates 32.72 MB against 45.5 MB, 8 x 3
  40.62 MB against 100.72 MB, and the retries drop (449 against 497, 476 against 798): a worker that finds every slot
  busy no longer opens a connection to try each advisory-lock key.
- **Streaming 8 x 1 has a wide spread** (4.965 s, standard deviation 5.5 s), so its mean is not a comparison; the
  others are 2.298 s against 2.276 s, 3.267 s against 3.745 s and 3.527 s against 4.476 s.

## Latest run: plain-crud, no Composite request

All 36 benchmarks (`--run full`) on the `plain-crud` branch at 57aef62, on 2026-10-19, against the
[drop-inbox-id run](#latest-run-branch-drop-inbox-id-at-30de64f) of the Composite design (the same code up to the write
path). The client has three calls (`SelectAsync`, `UpsertAsync`, `DeleteAsync`) and no Composite request: a read is a
query per object, a write an upsert per object with the parents first. Same workload, environment and runtime
(.NET 10.0.2); the database was recreated. The run took 7.4 minutes.

**Correctness**: 196 iterations (warm-up included), 0 failures: 0 overlapping claims, 0 entities not `done`, 0 stale
rows, 0 wrong values, 0 write-history regressions, 0 failed records.

Preloaded drain, 5,000 events (work counts are the medians of the iterations without a Describe, at 0 ms latency):

| W | P | Accounts | Entities | Mean 0 ms | Mean 25 ms | Allocated | API calls | Write req | Coalescing | Claims | Retries |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 | 1 | 100 | 990 | 234.8 ms | 1,799.7 ms | 29.36 MB | 47 | 17 | 6.28 | 1,028 | 38 |
| 1 | 1 | 1,000 | 2,113 | 822.1 ms | 6,473.1 ms | 88.81 MB | 151 | 47 | 1.90 | 3,279 | 1166 |
| 1 | 3 | 100 | 990 | 111.9 ms | 577.2 ms | 31.04 MB | 49 | 17 | 6.07 | 1,063 | 73 |
| 1 | 3 | 1,000 | 2,113 | 488.6 ms | 1,823.9 ms | 94.03 MB | 157 | 49 | 1.90 | 3,274 | 1161 |
| 4 | 1 | 100 | 990 | 111.4 ms | 465.0 ms | 32.37 MB | 51 | 17 | 5.77 | 1,119 | 129 |
| 4 | 1 | 1,000 | 2,113 | 426.6 ms | 1,323.2 ms | 95.75 MB | 155 | 49 | 1.90 | 3,288 | 1175 |
| 4 | 3 | 100 | 990 | 184.0 ms | 288.6 ms | 45.5 MB | 62 | 17 | 4.35 | 1,487 | 497 |
| 4 | 3 | 1,000 | 2,113 | 425.3 ms | 603.6 ms | 113.72 MB | 154 | 52 | 2.03 | 3,063 | 950 |
| 8 | 1 | 100 | 990 | 128.8 ms | 309.6 ms | 40.86 MB | 55 | 16 | 4.76 | 1,356 | 366 |
| 8 | 1 | 1,000 | 2,113 | 383.6 ms | 837.4 ms | 101.16 MB | 159 | 49 | 1.86 | 3,347 | 1234 |
| 8 | 3 | 100 | 990 | 288.7 ms | 321.8 ms | 100.72 MB | 73 | 17 | 3.61 | 1,788 | 798 |
| 8 | 3 | 1,000 | 2,113 | 540.9 ms | 587.0 ms | 138.78 MB | 167 | 54 | 1.90 | 3,262 | 1149 |

Streaming (50 accounts, 5 ms latency, 762 entities):

| W | P | Mean | Allocated | API calls | Write req | Coalescing | Claims | Retries |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 4 | 1 | 2.276 s | 256.71 MB | 1,154 | 524 | 2.56 | 2,523 | 11 |
| 4 | 3 | 3.745 s | 620.92 MB | 3,206 | 1,390 | 1.71 | 3,768 | 5 |
| 8 | 1 | 3.373 s | 528.03 MB | 2,806 | 1,232 | 1.66 | 3,886 | 4 |
| 8 | 3 | 4.476 s | 733.82 MB | 3,229 | 1,349 | 2.21 | 2,915 | 11 |

- **More calls on a backlog.** With one worker and one pod, 47 API calls against 30 at 100 accounts (+57%) and 151
  against 78 at 1,000 (+94%): the reads are a call per object instead of up to 5 in one, and the Accounts' and the
  Profiles' upserts are two calls instead of one. At 25 ms latency that is 1.80 s against 1.19 s and 6.47 s against
  3.83 s (+51% and +69%); at 0 ms 235 ms against 205 ms and 822 ms against 752 ms.
- **Streaming is about the same.** Batches are small there, so a Composite request combined little: 8 workers x 1 pod
  made 2,806 calls against 2,889, 4 x 1 1,154 against 1,097.
- **A little less memory**: 29.36 MB against 30.61 MB per drain at 100 accounts, 88.81 MB against 93.45 MB at 1,000.
- The extra calls are the price of the simpler client and write path; the daily allocation (115,000 by default) pays for
  them.

| Method | Mean | Allocated |
|---|--:|--:|
| ParseLimitHeader | 658.31 ns | 2832 B |
| PrepareTenEntities | 15,865.56 ns | 39472 B |
| ReadAndValidateEvent | 1,131.88 ns | 1616 B |
| MapAccountEvent | 1,415.10 ns | 3824 B |
| MapPayloadToSObject | 18.69 ns | 200 B |
| MapSObjectToRecord | 170.34 ns | 912 B |
| DiffNumber | 112.97 ns | 32 B |
| DiffDateTime | 234.50 ns | 192 B |

## Latest run: branch drop-inbox-id at 30de64f

All 36 benchmarks (`--run full`) on the `drop-inbox-id` branch at 30de64f, on 2026-10-18, after three changes since the
[9a61674 run](#latest-run-main-at-9a61674): every project targets .NET 10 (the earlier runs used the .NET 8 runtime),
the inbox row has no surrogate `Id` (the entity id is the primary key), and the `Sync/Write` folder was simplified
(reference lookups removed; the diff, packing and interpreting are separate small classes). Same workload and
environment as before, except the runtime; the database was recreated. The run took 7.4 minutes.

**Correctness**: 196 iterations (warm-up included), 0 failures: 0 overlapping claims, 0 entities not `done`, 0 stale
rows, 0 wrong values, 0 write-history regressions, 0 failed records.

Preloaded drain, 5,000 events (work counts are the medians of the iterations without a Describe, at 0 ms latency):

| W | P | Accounts | Entities | Mean 0 ms | Mean 25 ms | Allocated | API calls | Write req | Coalescing | Claims | Retries |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 | 1 | 100 | 990 | 205.35 ms | 1,190.51 ms | 30.61 MB | 30 | 15 | 6.28 | 1,028 | 38 |
| 1 | 1 | 1,000 | 2,113 | 752.22 ms | 3,831.74 ms | 93.45 MB | 78 | 33 | 1.90 | 3,279 | 1166 |
| 1 | 3 | 100 | 990 | 98.35 ms | 399.80 ms | 31.55 MB | 31 | 15 | 6.07 | 1,063 | 73 |
| 1 | 3 | 1,000 | 2,113 | 343.04 ms | 1,100.28 ms | 96.74 MB | 82 | 35 | 1.90 | 3,282 | 1169 |
| 4 | 1 | 100 | 990 | 94.61 ms | 322.07 ms | 33.75 MB | 32 | 15 | 5.77 | 1,119 | 129 |
| 4 | 1 | 1,000 | 2,113 | 383.17 ms | 800.49 ms | 98.86 MB | 80 | 34 | 1.90 | 3,274 | 1161 |
| 4 | 3 | 100 | 990 | 145.89 ms | 216.82 ms | 44.82 MB | 39 | 17 | 4.88 | 1,320 | 330 |
| 4 | 3 | 1,000 | 2,113 | 360.77 ms | 560.03 ms | 111.36 MB | 86 | 38 | 1.90 | 3,262 | 1149 |
| 8 | 1 | 100 | 990 | 109.91 ms | 223.85 ms | 38.19 MB | 32 | 14 | 5.33 | 1,211 | 221 |
| 8 | 1 | 1,000 | 2,113 | 358.27 ms | 572.95 ms | 105.56 MB | 87 | 36 | 1.78 | 3,501 | 1388 |
| 8 | 3 | 100 | 990 | 228.66 ms | 254.87 ms | 90.6 MB | 48 | 19 | 3.59 | 1,799 | 809 |
| 8 | 3 | 1,000 | 2,113 | 476.91 ms | 540.40 ms | 136.29 MB | 106 | 45 | 1.67 | 3,711 | 1598 |

Streaming (50 accounts, 5 ms latency, 762 entities):

| W | P | Mean | Allocated | API calls | Write req | Coalescing | Claims | Retries |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 4 | 1 | 2.453 s | 347.87 MB | 1,097 | 546 | 1.87 | 3,449 | 7 |
| 4 | 3 | 4.512 s | 912.03 MB | 3,265 | 1,626 | 1.35 | 4,767 | 3 |
| 8 | 1 | 4.074 s | 766.97 MB | 2,889 | 1,441 | 1.33 | 4,864 | 4 |
| 8 | 3 | 5.197 s | 1066.67 MB | 3,268 | 1,621 | 1.63 | 3,956 | 11 |

- **Same work**: 990 and 2,113 entities, and with one worker and one pod the same 30 and 78 API calls and 15 and 33
  write requests as the 9a61674 run. Runs with more workers or pods differ by a few calls between any two runs.
- **Same speed within noise, less memory**: 1 x 1 at 100 accounts, 205.4 ms against 209.6 ms; at 1,000 accounts, 752.2 ms
  against 754.3 ms. Allocation per drain is about 8% lower (30.61 MB against 33.23 MB for 1 x 1 at 100 accounts; 93.45
  MB against 101.98 MB at 1,000 accounts), from the removed lookup bookkeeping.
- **Micro benchmarks**: `PrepareTenEntities` allocates 39,472 B against 41,472 B and `MapAccountEvent` 3,824 B against
  4,016 B; the times are within noise.

| Method | Mean | Allocated |
|---|--:|--:|
| ParseLimitHeader | 644.41 ns | 2832 B |
| PrepareTenEntities | 15,781.53 ns | 39472 B |
| ReadAndValidateEvent | 1,134.32 ns | 1616 B |
| MapAccountEvent | 1,422.76 ns | 3824 B |
| MapPayloadToSObject | 18.76 ns | 200 B |
| MapSObjectToRecord | 171.77 ns | 912 B |
| DiffNumber | 115.00 ns | 32 B |
| DiffDateTime | 233.58 ns | 192 B |

## Latest run: main at 9a61674

All 36 benchmarks (`--run full`) on `main` at 9a61674, after the Salesforce API path and version moved to
`appsettings.json` (`Salesforce:BaseApiPath`, `Salesforce:ApiVersion`, no default in code) and the REST client was
cleaned up (one constructor, its models and parsing in their own files), on 2026-10-18. It measures the same workload
and environment as the [f844961 run](#latest-run-main-at-f844961), so the two compare. The database was recreated in a
new container. The run took 7.8 minutes. The URLs the client builds are the same strings as before, so no difference is
expected.

**Correctness**: 196 iterations (warm-up included), 0 failures: 0 overlapping claims, 0 entities not `done`, 0 stale
rows, 0 wrong values, 0 write-history regressions, 0 failed records.

Preloaded drain, 5,000 events. The means are BenchmarkDotNet's; the work counts are the medians of the iterations
without a Describe, at 0 ms latency:

| W | P | Accounts | Entities | Mean 0 ms | Mean 25 ms | Allocated | API calls | Write req | Coalescing | Claims | Retries |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 | 1 | 100 | 990 | 209.6 ms | 1,266.8 ms | 33.23 MB | 30 | 15 | 6.28 | 1,028 | 38 |
| 1 | 1 | 1,000 | 2,113 | 754.3 ms | 3,844.8 ms | 101.98 MB | 78 | 33 | 1.90 | 3,279 | 1166 |
| 1 | 3 | 100 | 990 | 107.6 ms | 390.3 ms | 35.28 MB | 31 | 15 | 6.08 | 1,062 | 72 |
| 1 | 3 | 1,000 | 2,113 | 362.5 ms | 1,082.7 ms | 106.07 MB | 82 | 35 | 1.90 | 3,283 | 1170 |
| 4 | 1 | 100 | 990 | 107.9 ms | 316.7 ms | 36.4 MB | 32 | 15 | 5.77 | 1,118 | 128 |
| 4 | 1 | 1,000 | 2,113 | 386.6 ms | 788.2 ms | 106.39 MB | 79 | 34 | 1.90 | 3,270 | 1157 |
| 4 | 3 | 100 | 990 | 166.9 ms | 217.8 ms | 61.32 MB | 39 | 15 | 4.04 | 1,599 | 609 |
| 4 | 3 | 1,000 | 2,113 | 370.9 ms | 618.9 ms | 114.37 MB | 86 | 38 | 1.94 | 3,217 | 1104 |
| 8 | 1 | 100 | 990 | 126.9 ms | 245.1 ms | 45.26 MB | 34 | 14 | 4.87 | 1,328 | 338 |
| 8 | 1 | 1,000 | 2,113 | 304.4 ms | 543.2 ms | 124 MB | 85 | 36 | 1.81 | 3,435 | 1322 |
| 8 | 3 | 100 | 990 | 262.2 ms | 275.7 ms | 75.26 MB | 45 | 17 | 3.83 | 1,687 | 697 |
| 8 | 3 | 1,000 | 2,113 | 559.3 ms | 507.3 ms | 166 MB | 98 | 43 | 1.79 | 3,483 | 1370 |

Streaming (50 accounts, 5 ms latency, 762 entities):

| W | P | Mean | Allocated | API calls | Write req | Coalescing | Claims | Retries |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 4 | 1 | 3.057 s | 428.82 MB | 1,349 | 671 | 1.66 | 3,880 | 3 |
| 4 | 3 | 4.493 s | 849.41 MB | 3,034 | 1,511 | 1.38 | 4,661 | 5 |
| 8 | 1 | 4.320 s | 801.34 MB | 2,944 | 1,467 | 1.33 | 4,868 | 3 |
| 8 | 3 | 5.009 s | 1024.18 MB | 3,127 | 1,553 | 1.65 | 3,900 | 11 |

- **Same work as f844961**: the entity counts (990 and 2,113 preloaded, 762 streaming) and the call counts with one
  worker and one pod (30 and 78 API calls) are the same; streaming call counts are within 2% (1,349 against 1,323 with
  4 x 1). Runs with more workers or pods differ by a few calls and claims between any two runs.
- **Same speed within noise**: 1 x 1 at 100 accounts, 209.6 ms against 209.7 ms; 4 workers x 1 pod, 107.9 ms against
  110.5 ms; streaming 4 x 1, 3.057 s against 3.010 s. The larger cases moved both ways by up to 20% (1 x 3 with 1,000
  accounts: 362.5 ms against 439.1 ms; 1 x 1 with 1,000 accounts: 754.3 ms against 661.6 ms), the same spread as between
  the earlier runs. Memory is the same (33.23 MB against 33.04 MB for 1 x 1 at 100 accounts).

Micro benchmarks (match the earlier runs within noise):

| Method | Mean | Allocated |
|---|--:|--:|
| ParseLimitHeader | 671.25 ns | 2832 B |
| PrepareTenEntities | 19,067.29 ns | 41472 B |
| ReadAndValidateEvent | 1,246.10 ns | 1616 B |
| MapAccountEvent | 1,690.08 ns | 4016 B |
| MapPayloadToSObject | 21.58 ns | 200 B |
| MapSObjectToRecord | 276.20 ns | 1008 B |
| DiffNumber | 128.65 ns | 32 B |
| DiffDateTime | 268.35 ns | 192 B |

## Latest run: main at f844961

All 36 benchmarks (`--run full`) on `main` at f844961, after PR #72 replaced the C# that applied the rate limits with
`sql/apply-rate-limits.sql` (run by the deploy after the EF migrations bundle, and by `--migrate` in the AppHost), on
2026-10-18. It measures the same workload and environment as the [f80334e run](#latest-run-main-at-f80334e), so the two
compare. The database was recreated. The run took 7.7 minutes. The change only affects how the limits are written at
deploy time, which the benchmarks don't time (they configure the limiter per iteration themselves), so no difference is
expected.

**Correctness**: 196 iterations (warm-up included), 0 failures: 0 overlapping claims, 0 entities not `done`, 0 stale
rows, 0 wrong values, 0 write-history regressions, 0 failed records.

Preloaded drain, 5,000 events. The means are BenchmarkDotNet's; the work counts are the medians of the iterations
without a Describe, at 0 ms latency:

| W | P | Accounts | Entities | Mean 0 ms | Mean 25 ms | Allocated | API calls | Write req | Coalescing | Claims | Retries |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 | 1 | 100 | 990 | 209.7 ms | 1,220.2 ms | 33.04 MB | 30 | 15 | 6.28 | 1,028 | 38 |
| 1 | 1 | 1,000 | 2,113 | 661.6 ms | 3,898.7 ms | 102.5 MB | 78 | 33 | 1.90 | 3,279 | 1166 |
| 1 | 3 | 100 | 990 | 108.4 ms | 385.5 ms | 35.36 MB | 31 | 15 | 6.08 | 1,062 | 72 |
| 1 | 3 | 1,000 | 2,113 | 439.1 ms | 1,115.0 ms | 106.75 MB | 82 | 35 | 1.90 | 3,285 | 1172 |
| 4 | 1 | 100 | 990 | 110.5 ms | 318.0 ms | 36.47 MB | 32 | 15 | 5.77 | 1,118 | 128 |
| 4 | 1 | 1,000 | 2,113 | 464.2 ms | 804.1 ms | 107.22 MB | 81 | 35 | 1.90 | 3,274 | 1161 |
| 4 | 3 | 100 | 990 | 152.7 ms | 213.0 ms | 49.79 MB | 35 | 16 | 5.25 | 1,230 | 240 |
| 4 | 3 | 1,000 | 2,113 | 445.7 ms | 530.6 ms | 119.79 MB | 89 | 39 | 1.90 | 3,287 | 1174 |
| 8 | 1 | 100 | 990 | 133.6 ms | 235.8 ms | 45.76 MB | 34 | 14 | 4.64 | 1,391 | 401 |
| 8 | 1 | 1,000 | 2,113 | 309.7 ms | 631.7 ms | 115.62 MB | 86 | 36 | 1.83 | 3,397 | 1284 |
| 8 | 3 | 100 | 990 | 267.3 ms | 272.7 ms | 78.23 MB | 48 | 18 | 3.46 | 1,867 | 877 |
| 8 | 3 | 1,000 | 2,113 | 544.6 ms | 600.4 ms | 147.08 MB | 104 | 45 | 1.69 | 3,676 | 1563 |

Streaming (50 accounts, 5 ms latency, 762 entities):

| W | P | Mean | Allocated | API calls | Write req | Coalescing | Claims | Retries |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 4 | 1 | 3.010 s | 421.21 MB | 1,323 | 658 | 1.68 | 3,841 | 3 |
| 4 | 3 | 4.406 s | 879.29 MB | 3,063 | 1,525 | 1.38 | 4,680 | 5 |
| 8 | 1 | 4.273 s | 784.24 MB | 2,949 | 1,469 | 1.33 | 4,864 | 4 |
| 8 | 3 | 4.909 s | 1003.83 MB | 3,070 | 1,524 | 1.65 | 3,901 | 9 |

- **Same work as f80334e**: the entity counts (990 and 2,113 preloaded, 762 streaming) and the call counts with one
  worker and one pod (30 and 78 API calls) are the same; streaming call counts are within 1% (1,323 against 1,312 with
  4 x 1). Runs with more workers or pods differ by a few calls and claims between any two runs.
- **Same speed within noise**: 1 x 1 at 100 accounts, 209.7 ms against 199.6 ms; 4 workers x 1 pod, 110.5 ms against
  115.9 ms; streaming 4 x 1, 3.010 s against 2.963 s. The larger cases moved both ways by up to 47% (4 x 1 with 1,000
  accounts: 464.2 ms against 316.0 ms; 8 x 1 with 1,000 accounts: 309.7 ms against 439.0 ms), the same spread as
  between the earlier runs. Memory is the same (33.04 MB against 33.05 MB for 1 x 1 at 100 accounts).

Micro benchmarks (match the earlier runs within noise):

| Method | Mean | Allocated |
|---|--:|--:|
| ParseLimitHeader | 647.08 ns | 2832 B |
| PrepareTenEntities | 18,879.50 ns | 41472 B |
| ReadAndValidateEvent | 1,228.43 ns | 1616 B |
| MapAccountEvent | 1,684.05 ns | 4016 B |
| MapPayloadToSObject | 21.22 ns | 200 B |
| MapSObjectToRecord | 272.63 ns | 1008 B |
| DiffNumber | 128.17 ns | 32 B |
| DiffDateTime | 227.93 ns | 192 B |

## Latest run: main at f80334e

All 36 benchmarks (`--run full`) on `main` at f80334e, after PR #71 moved the rate-limit configuration out of the pods
(`RateLimitInitializer` is gone; `SalesforceSync --migrate` writes the configured limits to the shared row, only when
they changed), on 2026-10-18. It measures the same workload and environment as the
[df2c269 run](#latest-run-main-at-df2c269), so the two compare. The database was recreated. The run took 7.5 minutes.
The change only affects process startup, which the benchmarks don't time (they configure the limiter per iteration
themselves), so no difference is expected.

**Correctness**: 196 iterations (warm-up included), 0 failures: 0 overlapping claims, 0 entities not `done`, 0 stale
rows, 0 wrong values, 0 write-history regressions, 0 failed records.

Preloaded drain, 5,000 events. The means are BenchmarkDotNet's; the work counts are the medians of the iterations
without a Describe, at 0 ms latency:

| W | P | Accounts | Entities | Mean 0 ms | Mean 25 ms | Allocated | API calls | Write req | Coalescing | Claims | Retries |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 | 1 | 100 | 990 | 199.6 ms | 1,217.5 ms | 33.05 MB | 30 | 15 | 6.28 | 1,028 | 38 |
| 1 | 1 | 1,000 | 2,113 | 744.9 ms | 3,872.8 ms | 101.75 MB | 78 | 33 | 1.90 | 3,279 | 1166 |
| 1 | 3 | 100 | 990 | 105.8 ms | 390.5 ms | 35.3 MB | 31 | 15 | 6.08 | 1,062 | 72 |
| 1 | 3 | 1,000 | 2,113 | 460.1 ms | 1,124.0 ms | 106.9 MB | 81 | 35 | 1.90 | 3,282 | 1169 |
| 4 | 1 | 100 | 990 | 115.9 ms | 314.7 ms | 36.65 MB | 32 | 15 | 5.77 | 1,118 | 128 |
| 4 | 1 | 1,000 | 2,113 | 316.0 ms | 778.5 ms | 107.57 MB | 81 | 35 | 1.88 | 3,316 | 1203 |
| 4 | 3 | 100 | 990 | 157.8 ms | 200.3 ms | 54.71 MB | 41 | 17 | 3.93 | 1,643 | 653 |
| 4 | 3 | 1,000 | 2,113 | 467.3 ms | 552.6 ms | 122.64 MB | 90 | 41 | 1.87 | 3,329 | 1216 |
| 8 | 1 | 100 | 990 | 129.8 ms | 233.7 ms | 44.68 MB | 34 | 15 | 4.97 | 1,299 | 309 |
| 8 | 1 | 1,000 | 2,113 | 439.0 ms | 531.0 ms | 114.46 MB | 86 | 36 | 1.80 | 3,459 | 1346 |
| 8 | 3 | 100 | 990 | 255.5 ms | 277.6 ms | 94.25 MB | 44 | 17 | 4.08 | 1,582 | 592 |
| 8 | 3 | 1,000 | 2,113 | 537.1 ms | 542.2 ms | 145.54 MB | 105 | 45 | 1.71 | 3,646 | 1533 |

Streaming (50 accounts, 5 ms latency, 762 entities):

| W | P | Mean | Allocated | API calls | Write req | Coalescing | Claims | Retries |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 4 | 1 | 2.963 s | 418.68 MB | 1,312 | 654 | 1.67 | 3,858 | 4 |
| 4 | 3 | 4.402 s | 873.39 MB | 3,052 | 1,520 | 1.38 | 4,676 | 4 |
| 8 | 1 | 4.257 s | 811.47 MB | 2,957 | 1,473 | 1.32 | 4,880 | 3 |
| 8 | 3 | 5.008 s | 1036.38 MB | 3,123 | 1,553 | 1.63 | 3,941 | 8 |

- **Same work as df2c269**: the entity counts (990 and 2,113 preloaded, 762 streaming) and the call counts with one
  worker and one pod (30 and 78 API calls) are the same; streaming call counts are within 2% (1,312 against 1,290 with
  4 x 1). Runs with more workers or pods differ by a few calls and claims between any two runs.
- **Same speed within noise**: 1 x 1 at 100 accounts, 199.6 ms against 205.8 ms; 4 workers x 1 pod, 115.9 ms against
  108.1 ms; streaming 4 x 1, 2.963 s against 2.913 s. The larger cases moved both ways by up to 40% (8 x 1 with 1,000
  accounts: 439.0 ms against 315.9 ms; 8 x 3 with 100 accounts: 255.5 ms against 274.6 ms), the same spread as between
  the earlier runs. Memory is the same (33.05 MB against 33.27 MB for 1 x 1 at 100 accounts).

Micro benchmarks (match the earlier runs within noise):

| Method | Mean | Allocated |
|---|--:|--:|
| ParseLimitHeader | 644.40 ns | 2832 B |
| PrepareTenEntities | 19,132.82 ns | 41472 B |
| ReadAndValidateEvent | 1,242.70 ns | 1616 B |
| MapAccountEvent | 1,695.75 ns | 4016 B |
| MapPayloadToSObject | 21.55 ns | 200 B |
| MapSObjectToRecord | 273.55 ns | 1008 B |
| DiffNumber | 129.83 ns | 32 B |
| DiffDateTime | 245.34 ns | 192 B |

## Latest run: main at df2c269

All 36 benchmarks (`--run full`) on `main` at df2c269, after PR #70 moved the schema migration out of the pods (the
`SyncSchemaInitializer` hosted service is gone; `SalesforceSync --migrate` runs once per deploy), on 2026-10-18. It
measures the same workload and environment as the [3ff8ae1 run](#latest-run-main-at-3ff8ae1), so the two compare. The
database was recreated. The run took 7 minutes. The change only affects process startup, which the benchmarks don't
time (they migrate once themselves, before measuring), so no difference is expected.

**Correctness**: 196 iterations (warm-up included), 0 failures: 0 overlapping claims, 0 entities not `done`, 0 stale
rows, 0 wrong values, 0 write-history regressions, 0 failed records.

Preloaded drain, 5,000 events. The means are BenchmarkDotNet's; the work counts are the medians of the iterations
without a Describe, at 0 ms latency:

| W | P | Accounts | Entities | Mean 0 ms | Mean 25 ms | Allocated | API calls | Write req | Coalescing | Claims | Retries |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 | 1 | 100 | 990 | 205.8 ms | 1,219.0 ms | 33.27 MB | 30 | 15 | 6.28 | 1,028 | 38 |
| 1 | 1 | 1,000 | 2,113 | 732.0 ms | 3,856.0 ms | 101.95 MB | 78 | 33 | 1.90 | 3,279 | 1166 |
| 1 | 3 | 100 | 990 | 107.4 ms | 389.1 ms | 35.4 MB | 31 | 15 | 6.08 | 1,062 | 72 |
| 1 | 3 | 1,000 | 2,113 | 421.9 ms | 1,125.8 ms | 107.46 MB | 83 | 36 | 1.90 | 3,282 | 1169 |
| 4 | 1 | 100 | 990 | 108.1 ms | 316.0 ms | 36.46 MB | 32 | 15 | 5.77 | 1,118 | 128 |
| 4 | 1 | 1,000 | 2,113 | 370.5 ms | 788.0 ms | 107.89 MB | 81 | 35 | 1.90 | 3,288 | 1175 |
| 4 | 3 | 100 | 990 | 175.2 ms | 221.4 ms | 53.29 MB | 39 | 17 | 4.52 | 1,428 | 438 |
| 4 | 3 | 1,000 | 2,113 | 417.6 ms | 488.4 ms | 130.89 MB | 89 | 39 | 1.84 | 3,384 | 1271 |
| 8 | 1 | 100 | 990 | 132.3 ms | 238.9 ms | 46.92 MB | 34 | 15 | 4.88 | 1,320 | 330 |
| 8 | 1 | 1,000 | 2,113 | 315.9 ms | 548.6 ms | 115.81 MB | 86 | 36 | 1.81 | 3,445 | 1332 |
| 8 | 3 | 100 | 990 | 274.6 ms | 276.8 ms | 84.43 MB | 45 | 17 | 3.63 | 1,779 | 789 |
| 8 | 3 | 1,000 | 2,113 | 480.2 ms | 527.7 ms | 161.71 MB | 100 | 43 | 1.74 | 3,581 | 1468 |

Streaming (50 accounts, 5 ms latency, 762 entities):

| W | P | Mean | Allocated | API calls | Write req | Coalescing | Claims | Retries |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 4 | 1 | 2.913 s | 413.07 MB | 1,290 | 643 | 1.70 | 3,796 | 3 |
| 4 | 3 | 4.459 s | 875.99 MB | 3,075 | 1,531 | 1.38 | 4,682 | 4 |
| 8 | 1 | 4.267 s | 792.91 MB | 2,952 | 1,470 | 1.32 | 4,876 | 3 |
| 8 | 3 | 4.948 s | 1040.72 MB | 3,078 | 1,530 | 1.67 | 3,881 | 9 |

- **Same work as 3ff8ae1**: the entity counts (990 and 2,113 preloaded, 762 streaming) and the call counts with one
  worker and one pod (30 and 78 API calls) are the same; streaming call counts are within 3% (1,290 against 1,325 with
  4 x 1). Runs with more workers or pods differ by a few calls and claims between any two runs.
- **Same speed within noise**: 1 x 1 at 100 accounts, 205.8 ms against 208.2 ms; 4 workers x 1 pod, 108.1 ms against
  108.4 ms; streaming 4 x 1, 2.913 s against 2.976 s. The larger cases moved both ways by up to 20% (4 x 3 with 100
  accounts: 175.2 ms against 160.0 ms; 8 x 3 with 1,000 accounts: 480.2 ms against 567.6 ms), the same spread as
  between the earlier runs. Memory is the same (33.27 MB against 33.07 MB for 1 x 1 at 100 accounts).

Micro benchmarks (match the earlier runs within noise):

| Method | Mean | Allocated |
|---|--:|--:|
| ParseLimitHeader | 630.48 ns | 2832 B |
| PrepareTenEntities | 18,829.47 ns | 41472 B |
| ReadAndValidateEvent | 1,224.31 ns | 1616 B |
| MapAccountEvent | 1,689.40 ns | 4016 B |
| MapPayloadToSObject | 21.52 ns | 200 B |
| MapSObjectToRecord | 273.02 ns | 1008 B |
| DiffNumber | 127.24 ns | 32 B |
| DiffDateTime | 244.04 ns | 192 B |

## Latest run: main at 3ff8ae1

All 36 benchmarks (`--run full`) on `main` at 3ff8ae1, after PR #68 and #69 finished the project split: the startup
hosted services moved to the entry project, `SalesforceSync.Infrastructure` and `SalesforceSync.Data` reference no other
project, and the rate limiter split into a Postgres repository (`RateLimitRepository`, in Data) and its decisions
(`SalesforceRateLimiter`, in the entry project), on 2026-10-18. It measures the same workload and environment as the
[93b4ace run](#latest-run-main-at-93b4ace), so the two compare. The database was recreated. The run took 7 minutes.

**Correctness**: 196 iterations (warm-up included), 0 failures: 0 overlapping claims, 0 entities not `done`, 0 stale
rows, 0 wrong values, 0 write-history regressions, 0 failed records.

Preloaded drain, 5,000 events. The means are BenchmarkDotNet's; the work counts are the medians of the iterations
without a Describe, at 0 ms latency:

| W | P | Accounts | Entities | Mean 0 ms | Mean 25 ms | Allocated | API calls | Write req | Coalescing | Claims | Retries |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 | 1 | 100 | 990 | 208.2 ms | 1,210.5 ms | 33.07 MB | 30 | 15 | 6.28 | 1,028 | 38 |
| 1 | 1 | 1,000 | 2,113 | 700.6 ms | 3,903.0 ms | 102.73 MB | 78 | 33 | 1.90 | 3,279 | 1166 |
| 1 | 3 | 100 | 990 | 109.2 ms | 385.6 ms | 35.27 MB | 31 | 15 | 6.08 | 1,062 | 72 |
| 1 | 3 | 1,000 | 2,113 | 436.9 ms | 1,068.5 ms | 105.79 MB | 81 | 35 | 1.90 | 3,278 | 1165 |
| 4 | 1 | 100 | 990 | 108.4 ms | 316.5 ms | 36.8 MB | 32 | 15 | 5.77 | 1,118 | 128 |
| 4 | 1 | 1,000 | 2,113 | 367.2 ms | 780.0 ms | 105.85 MB | 81 | 35 | 1.88 | 3,311 | 1198 |
| 4 | 3 | 100 | 990 | 160.0 ms | 214.3 ms | 54.95 MB | 39 | 16 | 4.29 | 1,503 | 513 |
| 4 | 3 | 1,000 | 2,113 | 440.2 ms | 498.8 ms | 119.8 MB | 88 | 39 | 1.94 | 3,209 | 1096 |
| 8 | 1 | 100 | 990 | 128.1 ms | 239.9 ms | 45.29 MB | 35 | 15 | 4.58 | 1,410 | 420 |
| 8 | 1 | 1,000 | 2,113 | 323.1 ms | 528.8 ms | 114.43 MB | 87 | 36 | 1.83 | 3,408 | 1295 |
| 8 | 3 | 100 | 990 | 264.2 ms | 276.8 ms | 91.05 MB | 51 | 18 | 3.21 | 2,013 | 1023 |
| 8 | 3 | 1,000 | 2,113 | 567.6 ms | 484.5 ms | 155.26 MB | 105 | 45 | 1.71 | 3,626 | 1513 |

Streaming (50 accounts, 5 ms latency, 762 entities):

| W | P | Mean | Allocated | API calls | Write req | Coalescing | Claims | Retries |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 4 | 1 | 2.976 s | 421.9 MB | 1,325 | 660 | 1.67 | 3,868 | 5 |
| 4 | 3 | 4.415 s | 862.7 MB | 3,050 | 1,520 | 1.38 | 4,676 | 5 |
| 8 | 1 | 4.233 s | 790.0 MB | 2,929 | 1,461 | 1.32 | 4,868 | 4 |
| 8 | 3 | 4.908 s | 1,002.8 MB | 3,066 | 1,521 | 1.66 | 3,877 | 12 |

- **Same work as 93b4ace**: the entity counts (990 and 2,113 preloaded, 762 streaming) and the call counts with one
  worker and one pod (30 and 78 API calls) are the same, and the streaming call counts are within 1% of the earlier
  run (1,325 against 1,328 with 4 x 1). Runs with more workers or pods differ by a few calls and claims between any
  two runs.
- **Same speed within noise**: 1 x 1 at 100 accounts, 208.2 ms against 207.1 ms; 4 workers x 1 pod, 108.4 ms against
  108.7 ms; streaming 4 x 1, 2.976 s against 2.997 s. The larger cases moved both ways by up to 35% (4 x 3 with 1,000
  accounts: 440.2 ms against 330.4 ms; 8 x 3 with 100 accounts: 264.2 ms against 296.6 ms), which is what this machine
  does between two runs of the same code (the Error column is as wide).
- **The limiter's extra layer costs nothing visible**: each acquire now runs its decision as a function inside
  `RateLimitRepository.DecideAsync`, with the same four round trips as before (the lock with the clock, the usage, the
  write, the commit). Memory is the same (33.07 MB against 33.06 MB for 1 x 1 at 100 accounts).

Micro benchmarks (they don't touch the limiter, and match the earlier runs within noise):

| Method | Mean | Allocated |
|---|--:|--:|
| ParseLimitHeader | 629.18 ns | 2832 B |
| PrepareTenEntities | 18,849.35 ns | 41472 B |
| ReadAndValidateEvent | 1,219.67 ns | 1616 B |
| MapAccountEvent | 1,691.05 ns | 4016 B |
| MapPayloadToSObject | 21.23 ns | 200 B |
| MapSObjectToRecord | 272.04 ns | 1008 B |
| DiffNumber | 126.15 ns | 32 B |
| DiffDateTime | 242.22 ns | 192 B |

## Latest run: main at 93b4ace

All 36 benchmarks (`--run full`) on `main` at 93b4ace, after PR #67 split the worker into `SalesforceSync.Data`,
`SalesforceSync.Infrastructure` and the `SalesforceSync` entry project (with a regenerated `InitialSchema` migration),
on 2026-10-18. It measures the same workload and environment as the [3f68779 run](#latest-run-main-at-3f68779), so the
two compare: the split moved code between assemblies and changed no logic. The database was recreated for the new
migration. The run took 7 minutes.

**Correctness**: 196 iterations (warm-up included), 0 failures: 0 overlapping claims, 0 entities not `done`, 0 stale
rows, 0 wrong values, 0 write-history regressions, 0 failed records.

Preloaded drain, 5,000 events. The means are BenchmarkDotNet's; the work counts are the medians of the iterations
without a Describe, at 0 ms latency:

| W | P | Accounts | Entities | Mean 0 ms | Mean 25 ms | Allocated | API calls | Write req | Coalescing | Claims | Retries |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 | 1 | 100 | 990 | 207.1 ms | 1,220.9 ms | 33.06 MB | 30 | 15 | 6.28 | 1,028 | 38 |
| 1 | 1 | 1,000 | 2,113 | 697.5 ms | 3,945.1 ms | 101.96 MB | 78 | 33 | 1.90 | 3,279 | 1166 |
| 1 | 3 | 100 | 990 | 110.4 ms | 381.6 ms | 35.54 MB | 31 | 15 | 6.08 | 1,062 | 72 |
| 1 | 3 | 1,000 | 2,113 | 379.3 ms | 1,140.0 ms | 107.02 MB | 83 | 36 | 1.90 | 3,282 | 1169 |
| 4 | 1 | 100 | 990 | 108.7 ms | 317.5 ms | 36.47 MB | 32 | 15 | 5.77 | 1,118 | 128 |
| 4 | 1 | 1,000 | 2,113 | 383.0 ms | 797.6 ms | 106.97 MB | 81 | 35 | 1.90 | 3,273 | 1160 |
| 4 | 3 | 100 | 990 | 171.1 ms | 217.1 ms | 56.68 MB | 43 | 17 | 4.27 | 1,511 | 521 |
| 4 | 3 | 1,000 | 2,113 | 330.4 ms | 554.6 ms | 122.64 MB | 92 | 41 | 1.87 | 3,328 | 1215 |
| 8 | 1 | 100 | 990 | 120.3 ms | 232.7 ms | 45.71 MB | 33 | 14 | 4.87 | 1,326 | 336 |
| 8 | 1 | 1,000 | 2,113 | 317.3 ms | 545.1 ms | 114.88 MB | 87 | 37 | 1.85 | 3,366 | 1253 |
| 8 | 3 | 100 | 990 | 296.6 ms | 270.7 ms | 83.05 MB | 45 | 18 | 3.80 | 1,753 | 763 |
| 8 | 3 | 1,000 | 2,113 | 559.0 ms | 527.1 ms | 146.06 MB | 100 | 42 | 1.79 | 3,491 | 1378 |

Streaming (50 accounts, 5 ms latency, 762 entities):

| W | P | Mean | Allocated | API calls | Write req | Coalescing | Claims | Retries |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 4 | 1 | 2.997 s | 425.2 MB | 1,328 | 661 | 1.67 | 3,863 | 4 |
| 4 | 3 | 4.383 s | 857.2 MB | 3,030 | 1,509 | 1.39 | 4,655 | 5 |
| 8 | 1 | 4.297 s | 801.2 MB | 2,974 | 1,482 | 1.32 | 4,880 | 4 |
| 8 | 3 | 4.967 s | 1,045.3 MB | 3,123 | 1,552 | 1.66 | 3,894 | 10 |

- **Same work as 3f68779**: the entity counts (990 and 2,113 preloaded, 762 streaming) and the single-worker call
  counts (30 and 78 API calls with one worker and one pod) are the same, and the streaming call counts are within 2% of
  the earlier run (1,328 against 1,322 with 4 x 1). Runs with more workers or pods differ by a few calls and claims
  between any two runs, as before.
- **Same speed within noise**: with 1 x 1 at 100 accounts, 207.1 ms against 212.7 ms; with 4 workers x 1 pod,
  108.7 ms against 112.5 ms; streaming 4 x 1, 2.997 s against 3.043 s. The larger cases moved both ways by up to
  25%, which is what this machine does between two runs of the same code (the bands in the Error column are as wide).
- **Memory** is the same (33.06 MB against 33.09 MB for 1 x 1 at 100 accounts), as are the micro benchmarks below.

Micro benchmarks:

| Method | Mean | Allocated |
|---|--:|--:|
| ParseLimitHeader | 651.66 ns | 2832 B |
| PrepareTenEntities | 18,868.31 ns | 41472 B |
| ReadAndValidateEvent | 1,222.80 ns | 1616 B |
| MapAccountEvent | 1,703.16 ns | 4016 B |
| MapPayloadToSObject | 21.12 ns | 200 B |
| MapSObjectToRecord | 271.84 ns | 1008 B |
| DiffNumber | 125.95 ns | 32 B |
| DiffDateTime | 223.78 ns | 192 B |

## Latest run: main at 3f68779

All 36 benchmarks (`--run full`) on `main` at 3f68779 (PR #66, the mock org aligned with the Salesforce API guides, and
`Sync:MaxEntitiesPerBatch` as an option), on 2026-10-18. The environment is the same as in the
[b5950c1 run](#latest-run-main-at-b5950c1) (Docker Postgres 17 with its defaults); the load average was 2 to 3, lower
than then. The run took 7 minutes.

**Correctness**: 196 iterations (warm-up included), 0 failures: 0 overlapping claims, 0 entities not `done`, 0 stale
rows, 0 wrong values, 0 write-history regressions, 0 failed records.

**The workload changed, so the work counts and times are not comparable with the b5950c1 run.** Since PR #65 and #66
the drain benchmarks also load contact events and account deletes (the first iteration: 6,453 account events, 676
relationship events, 777 contact events, 179 delete events for 98 deleted accounts), so 100 accounts end as 990
entities (was 718) and 1,000 accounts as 2,113 (was 1,618). The mock org also checks more of the API's rules. Compare
the columns below with each other, not with the earlier tables.

Preloaded drain, 5,000 events. The means are BenchmarkDotNet's; the work counts are the medians of the iterations
without a Describe, at 0 ms latency:

| W | P | Accounts | Entities | Mean 0 ms | Mean 25 ms | Allocated | API calls | Write req | Coalescing | Claims | Retries |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 | 1 | 100 | 990 | 212.7 ms | 1,184.7 ms | 33.09 MB | 30 | 15 | 6.28 | 1,028 | 38 |
| 1 | 1 | 1,000 | 2,113 | 667.3 ms | 3,935.1 ms | 101.9 MB | 78 | 33 | 1.90 | 3,279 | 1166 |
| 1 | 3 | 100 | 990 | 104.1 ms | 388.3 ms | 35.11 MB | 31 | 15 | 6.08 | 1,062 | 72 |
| 1 | 3 | 1,000 | 2,113 | 394.0 ms | 1,077.1 ms | 107.23 MB | 82 | 35 | 1.90 | 3,277 | 1164 |
| 4 | 1 | 100 | 990 | 112.5 ms | 315.8 ms | 37.21 MB | 32 | 15 | 5.77 | 1,118 | 128 |
| 4 | 1 | 1,000 | 2,113 | 321.9 ms | 783.6 ms | 106.51 MB | 80 | 34 | 1.90 | 3,278 | 1165 |
| 4 | 3 | 100 | 990 | 153.1 ms | 236.8 ms | 50.6 MB | 37 | 15 | 4.64 | 1,390 | 400 |
| 4 | 3 | 1,000 | 2,113 | 461.9 ms | 453.3 ms | 126.93 MB | 91 | 40 | 1.88 | 3,318 | 1205 |
| 8 | 1 | 100 | 990 | 127.9 ms | 232.2 ms | 45.59 MB | 35 | 15 | 4.73 | 1,363 | 373 |
| 8 | 1 | 1,000 | 2,113 | 317.9 ms | 541.3 ms | 115.51 MB | 85 | 35 | 1.83 | 3,409 | 1296 |
| 8 | 3 | 100 | 990 | 266.2 ms | 282.1 ms | 88.59 MB | 46 | 17 | 3.57 | 1,810 | 820 |
| 8 | 3 | 1,000 | 2,113 | 450.6 ms | 572.3 ms | 147.61 MB | 104 | 46 | 1.75 | 3,574 | 1461 |

Streaming (50 accounts, 5 ms latency, 762 entities):

| W | P | Mean | Allocated | API calls | Write req | Coalescing | Claims | Retries |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 4 | 1 | 3.043 s | 418.4 MB | 1,322 | 658 | 1.65 | 3,898 | 5 |
| 4 | 3 | 4.473 s | 887.3 MB | 3,089 | 1,537 | 1.38 | 4,688 | 4 |
| 8 | 1 | 4.320 s | 809.8 MB | 2,995 | 1,493 | 1.32 | 4,889 | 3 |
| 8 | 3 | 5.047 s | 1,037.3 MB | 3,148 | 1,563 | 1.65 | 3,912 | 9 |

- **Same shape as b5950c1**: one pod with 4 workers is the fastest streaming case, and more workers or pods make more
  calls at the same coalescing (1.3 to 1.7), because an event makes its entity due at once (no `CoalesceDelay`).
- **Preloaded drains** are fastest with 4 workers x 1 pod (112.5 ms) or 3 pods x 1 worker (104.1 ms) at 100 accounts
  and 0 ms; 8 workers x 3 pods is the slowest (266.2 ms) with the most claims and retries (1,810 and 820).
- **Latency** (25 ms per call) costs most where one worker makes the calls: 1 x 1 with 1,000 accounts goes from
  667 ms to 3,935 ms, against 322 ms to 784 ms with 4 workers.

Micro benchmarks (they don't touch the workload, and match the b5950c1 run within noise):

| Method | Mean | Allocated |
|---|--:|--:|
| ParseLimitHeader | 663.30 ns | 2832 B |
| PrepareTenEntities | 18,631.41 ns | 41472 B |
| ReadAndValidateEvent | 1,217.56 ns | 1616 B |
| MapAccountEvent | 1,690.94 ns | 4016 B |
| MapPayloadToSObject | 21.17 ns | 200 B |
| MapSObjectToRecord | 270.70 ns | 1008 B |
| DiffNumber | 127.34 ns | 32 B |
| DiffDateTime | 245.82 ns | 192 B |

## Latest run: main at b5950c1

The quick run (`--run quick`) and all 36 benchmarks (`--run full`) on `main` at b5950c1 (PR #65, account aggregates and
deletes), on 2026-10-18. It is the first full run on the inbox with one row per entity, the lease in `next_attempt_at`
and no `Sync:CoalesceDelay`. The environment differs from the table above: macOS 26.2 (Darwin 25.2.0), .NET SDK
10.0.102, runtime .NET 8.0.11, and PostgreSQL 17 in a fresh Docker container with its defaults (`shared_buffers`
128MB, `max_connections` 100), not the tuned one. The load average was 6 to 9. The full run took 6 minutes, the quick
run 27 seconds.

**Correctness**: 196 iterations (warm-up included), 0 failures: 0 overlapping claims, 0 entities not `done`, 0 stale
rows, 0 wrong values, 0 write-history regressions, 0 failed records.

Quick run (3 iterations, 100 accounts, 0 ms):

| Case | Mean |
|---|--:|
| Drain, 1 worker x 1 pod | 211.6 ms |
| Drain, 4 workers x 1 pod | 108.5 ms |
| Drain, 1 worker x 3 pods | 102.3 ms |
| Drain, 4 workers x 3 pods | 209.4 ms |
| Drain while ingesting, 4 workers x 1 pod | 2.359 s |

Preloaded drain, 5,000 events, 718 (100 accounts) or 1,618 (1,000 accounts) entities. The medians are
BenchmarkDotNet's; the work counts are the medians of the iterations without a Describe, at 0 ms latency:

| W | P | Accounts | Median 0 ms | Median 25 ms | Allocated | API calls | Write req | Coalescing | Claims | Retries |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 | 1 | 100 | 195.22 ms | 842.06 ms | 27.71 MB | 22 | 11 | 6.65 | 752 | 34 |
| 1 | 1 | 1,000 | 528.90 ms | 2,906.95 ms | 81.72 MB | 56 | 25 | 2.17 | 2,299 | 681 |
| 1 | 3 | 100 | 119.01 ms | 277.10 ms | 30.16 MB | 23 | 11 | 6.31 | 793 | 75 |
| 1 | 3 | 1,000 | 266.80 ms | 786.92 ms | 83.15 MB | 58 | 27 | 2.19 | 2,278 | 660 |
| 4 | 1 | 100 | 95.30 ms | 262.76 ms | 31.42 MB | 24 | 11 | 5.76 | 868 | 150 |
| 4 | 1 | 1,000 | 253.33 ms | 572.41 ms | 84.43 MB | 56 | 25 | 2.20 | 2,271 | 653 |
| 4 | 3 | 100 | 258.94 ms | 314.68 ms | 69.91 MB | 30 | 13 | 4.49 | 1,123 | 405 |
| 4 | 3 | 1,000 | 308.52 ms | 380.17 ms | 95.74 MB | 64 | 30 | 2.38 | 2,096 | 478 |
| 8 | 1 | 100 | 129.70 ms | 277.50 ms | 42.93 MB | 28 | 11 | 4.28 | 1,168 | 450 |
| 8 | 1 | 1,000 | 241.94 ms | 384.02 ms | 90.6 MB | 59 | 26 | 2.23 | 2,242 | 624 |
| 8 | 3 | 100 | 458.61 ms | 439.19 ms | 114.73 MB | 37 | 13 | 3.03 | 1,657 | 939 |
| 8 | 3 | 1,000 | 681.87 ms | 692.52 ms | 187.51 MB | 69 | 28 | 1.78 | 2,810 | 1192 |

Streaming (50 accounts, 5 ms latency, 600 entities):

| W | P | Mean | Allocated | API calls | Write req | Coalescing | Claims | Retries |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 4 | 1 | 2.243 s | 321.0 MB | 893 | 445 | 1.65 | 3,017 | 8 |
| 4 | 3 | 3.557 s | 794.9 MB | 2,268 | 1,133 | 1.42 | 3,519 | 7 |
| 8 | 1 | 3.270 s | 641.7 MB | 2,080 | 1,039 | 1.37 | 3,647 | 8 |
| 8 | 3 | 4.120 s | 949.9 MB | 2,340 | 1,164 | 1.78 | 2,818 | 19 |

- **Streaming makes several times the calls of the [2d20c16 run](#latest-run-main-at-2d20c16)** (893 to 2,340, against
  216 to 332) with coalescing 1.4 to 1.8 (against 3.6 to 5.1). That is `Sync:CoalesceDelay` gone: an event makes its
  entity due at once, so a worker claims it before the next events for the same account arrive. The
  [realistic-ingest sweep](#realistic-ingest-sweep-2026-10-16-main-at-85d1de7) measured this trade.
- **Preloaded drains** have the work counts of the earlier runs (22 to 69 calls) and finish in 95 to 682 ms (median,
  0 ms). More workers or pods help up to 4 workers x 1 pod and then cost: 8 workers x 3 pods makes more claims and
  retries (939 to 1,192) than 1 x 1 (34 to 681), because the workers contend for the same few entities.
- Times are not comparable with the 2d20c16 run (different machine state, Postgres settings and design); compare work
  counts first.

Micro benchmarks:

| Method | Mean | Allocated |
|---|--:|--:|
| ParseLimitHeader | 637.48 ns | 2832 B |
| PrepareTenEntities | 18,504.78 ns | 41472 B |
| ReadAndValidateEvent | 1,198.87 ns | 1616 B |
| MapAccountEvent | 1,656.39 ns | 4016 B |
| MapPayloadToSObject | 20.55 ns | 200 B |
| MapSObjectToRecord | 266.32 ns | 1008 B |
| DiffNumber | 126.46 ns | 32 B |
| DiffDateTime | 247.01 ns | 192 B |

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

### Realistic-ingest sweep (2026-10-16, main at 85d1de7)

Streaming, 4 workers x 1 pod, the events stored one at a time like the Kafka consumer, every delay run in two passes
(pass 1 ascending, pass 2 descending). API calls per pass and averaged, and the claims averaged; every iteration
passed every check. Pass 2 ran at a load average of about 16, so its times are noise.

| Delay | api, pass 1 | api, pass 2 | api, average | claims, average |
|---|---|---|---|---|
| 0 | 1,303 | 1,403 | 1,353 | 3,549 |
| 25 ms | 1,420 | 1,447 | 1,434 | 3,245 |
| 50 ms | 1,289 | 1,406 | 1,347 | 2,951 |
| 100 ms | 1,252 | 1,356 | 1,304 | 2,452 |
| 250 ms | 1,254 | 1,496 | 1,375 | 2,044 |
| 500 ms | 1,112 | 1,191 | 1,151 | 1,538 |

The run-to-run spread is about ±4%. A longer delay means fewer claims, but a call is per batch, and with small
batches fewer claims barely means fewer calls: up to 250 ms no delay saves calls beyond the spread, and only 500 ms
saved about 15%, at half a second of added sync latency. **Decision: `Sync:CoalesceDelay` removed**; a stored event
makes its entity due at once.

## Micro benchmarks

Measured with the earlier design: `CoalesceTenEvents` no longer exists (an inbox holds one event per entity), and
`PrepareTenEvents` is now `PrepareTenEntities`.

| Method               | Mean         | Error        | StdDev       | Gen0   | Gen1   | Allocated |
|---|---|---|---|---|---|---|
| ParseLimitHeader     |  1,341.70 ns |    26.347 ns |    38.619 ns | 0.3376 | 0.0019 |    2832 B |
| CoalesceTenEvents    |     46.64 ns |     2.159 ns |     6.090 ns |      - |      - |         - |
| PrepareTenEvents     | 36,475.42 ns | 1,810.361 ns | 5,135.692 ns | 2.4414 | 0.0916 |   20480 B |
| ReadAndValidateEvent |  3,156.44 ns |   110.016 ns |   317.421 ns | 0.1945 |      - |    1632 B |
| MapAccountEvent      |  5,065.07 ns |   333.269 ns |   961.557 ns | 0.4272 |      - |    3624 B |
| MapPayloadToSObject  |     55.37 ns |     2.642 ns |     7.708 ns | 0.0238 |      - |     200 B |
| MapSObjectToRecord   |    609.29 ns |    13.783 ns |    37.731 ns | 0.1059 |      - |     888 B |
| DiffNumber           |    309.02 ns |     6.753 ns |    19.267 ns | 0.0038 |      - |      32 B |
| DiffDateTime         |    625.71 ns |    16.431 ns |    48.188 ns | 0.0229 |      - |     192 B |

## Remaining risks

(As of the state in the inbox row: one row per entity, claimed and leased itself.)

- **Overwritten rows**: every event of an entity rewrites its one inbox row (a new row version and index entries,
  but no new row), and a claimed row overwritten meanwhile gets no outcome, only the end of its lease, and stays
  due: one more claim, not one more write of the old event. Not measured yet; re-run the streaming benchmark
  before comparing its claims with the numbers above.
- **One hot row per entity**: the consumer, the claim and the outcomes all update the entity's inbox row, so a store
  waits for an outcome transaction that has the row locked (milliseconds), and a claim skips a row being stored.
  Every claim, outcome and lease change is a new row version that is never HOT (`next_attempt_at` is indexed, and
  the claim moves it to the lease's end since `LeaseInNextAttemptAt`); not re-measured since `sf_sync_state` was
  removed.
- **Statistics**: no planner settings are pinned; every statement is written so that its only sensible plan is an
  index lookup by key. A new statement must be checked with stale statistics too (idle-then-backlog and
  backlog-then-drained), or it can reintroduce a plan that scans a whole table or index.
- **Isolation**: the claim and the outcomes rely on READ COMMITTED: a row leased by another worker after the claim's
  snapshot is re-checked on its new version when it is locked, and an outcome that waited for a store re-checks its
  WHERE (the claimed `event_time`) on the row the store left. Under REPEATABLE READ both would be serialization
  errors instead.
- **Noise**: the drain numbers above were measured on a busy machine; compare work counts first and times only
  across alternating runs.
