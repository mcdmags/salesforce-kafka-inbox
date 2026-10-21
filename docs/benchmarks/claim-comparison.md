# Claim comparison: our inbox repository against a simple claim

The "simple claim" repository (`SimpleClaimRepository`) is PartySyncRepository's four statements, copied unchanged:
Upsert, ClaimNextJobs, MarkJobComplete and MarkJobFailed. It sits behind our `IRepository<TRow>`, and the same worker
loop drives both repositories: claim, "send" (a delay, then a result per entity), record the results. Salesforce is
faked, so only the repository differs.

Run: `dotnet run -c Release --project benchmarks/SalesforceSync.Benchmarks -- --claims`. It uses its own database,
`claimsdb`, created if missing. Code: `benchmarks/SalesforceSync.Benchmarks/ClaimComparison/`.

Changes made so it could run here:
- The lease is a TimeSpan instead of whole minutes, so scenarios can wait it out in seconds.
- The app clock can be passed in, so one worker can run with a skewed clock.
- When MarkJobComplete or MarkJobFailed finds no row, the InvalidOperationException is counted and the loop moves on.
- `last_synced_event_timestamp` starts at '-infinity'. The screenshots don't show the column's default. If it is a
  nullable column with no default, `NULL < event_timestamp` is never true, so a new row would never be claimed.

What it shows:
- **The claim statement itself is not the problem.** With a partial index on the rows still to sync, the simple
  claim is as fast as ours (1). Without that index, it reads every synced row on each claim, and gets slower as
  history grows.
- **What is missing is around the claim:**
  - One result statement per entity instead of one per batch (2).
  - No backoff (3, 4).
  - No check that the row is still this worker's: a result is accepted from a worker whose lease ran out (7).
  - A newer event arriving mid-send waits for the lease to run out (5).
  - The lease uses the app's clock, not the database's (6).
- **How much each costs in production:**
  - The throughput gap (2) is small next to real Salesforce call times: about 0.13 ms per entity.
  - The others decide whether events are synced at all (3, 4), synced late (5: a whole lease; that is minutes with
    whole-minute timeouts), or sent twice at once (6, 7).
- Scenario 6 needs workers whose calls take different times. With equal call times, the workers run in lockstep,
  and the first run showed no overlap.

## Run: Claim comparison, 2026-10-21 23:25 UTC

### 1. Claim time as synced rows pile up

180 due rows per claim, 20 claims (each batch marked done between claims, not timed). Synced rows are the table's history: entities already in Salesforce.

| Synced rows | Repository | Claim median (ms) | Claim max (ms) |
|---:|---|---:|---:|
| 0 | Ours (InboxRepository) | 1.3 | 84.9 |
| 0 | Simple (primary key only) | 1.6 | 10.2 |
| 0 | Simple + partial index | 1.5 | 1.9 |
| 100,000 | Ours (InboxRepository) | 1.0 | 1.9 |
| 100,000 | Simple (primary key only) | 3.8 | 9.1 |
| 100,000 | Simple + partial index | 1.3 | 2.2 |
| 1,000,000 | Ours (InboxRepository) | 1.1 | 2.1 |
| 1,000,000 | Simple (primary key only) | 29.1 | 30.2 |
| 1,000,000 | Simple + partial index | 1.3 | 2.1 |

### 2. Drain: every send succeeds

20,000 due rows, no synced history, 4 workers, batch 180, Salesforce answers at once.

| Repository | Time (s) | Rows/s | Outcome statements |
|---|---:|---:|---:|
| Ours (InboxRepository) | 0.2 | 119,533 | 112 (one per batch) |
| Simple (primary key only) | 2.6 | 7,652 | 20,000 (one per entity) |
| Simple + partial index | 2.6 | 7,811 | 20,000 (one per entity) |

### 3. Rows that always fail

100 rows that always fail (oldest events) and 5,000 good rows. 4 workers, batch 180, 20 ms per Salesforce call, 8 attempts. Ours backs off 30 s after the first failure (production default).

| Repository | Good rows synced (s) | Sends of failing rows | Failing rows given up | Failing rows show their error |
|---|---:|---:|---:|---|
| Ours (InboxRepository) | 0.2 | 100 | 0 | yes (last_error; retried after the backoff) |
| Simple + partial index | 0.9 | 800 | 100 | no (nothing stored) |

### 4. Salesforce down for 10 seconds

2,000 due rows, 4 workers, batch 180, 50 ms per call, every send fails for the first 10 s, then succeeds. 8 attempts. Ours backs off 1 s, doubling (scaled down from 30 s).

| Repository | Synced | Given up (never claimed again) | Time to settle (s) |
|---|---:|---:|---:|
| Ours (InboxRepository) | 2,000 | 0 | 15.5 |
| Simple + partial index | 0 | 2,000 | 10.0 |

### 5. A newer event arrives while the older one is being sent

20 entities, 1 worker, batch 20, 500 ms per call, lease 20 s. 200 ms into the first send, a newer event is stored for each entity.

| Repository | Newer event synced after (median, s) | Results that found no matching row |
|---|---:|---:|
| Ours (InboxRepository) | 0.8 | 0 |
| Simple + partial index | 20.3 | 20 |

### 6. A pod whose clock runs 30 s ahead

2,000 due rows, 2 workers, batch 50, lease 20 s. One worker's calls take 2 s; the other's take 500 ms and its clock runs 30 s ahead. Ours has no app clock: both workers are the same.

| Repository | Entities sent by two workers at once | Sends | Results that found no matching row |
|---|---:|---:|---:|
| Ours (InboxRepository) | 0 | 2,000 | 0 |
| Simple + partial index | 550 | 2,550 | 0 |

### 7. A late result after the lease ran out

100 rows, lease 2 s. Worker A claims them and needs 3 s; at 2.2 s worker B claims them again (lease over) and starts a 3 s send. At 3 s A reports a failure. At 3.3 s worker C claims.

| Repository | Rows B took over | Rows C claimed while B was still sending |
|---|---:|---:|
| Ours (InboxRepository) | 100 | 0 |
| Simple + partial index | 100 | 100 |
