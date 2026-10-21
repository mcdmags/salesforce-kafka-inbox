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

## The improved simple claim

`ImprovedClaimRepository` is the simple claim with the gaps the first run showed fixed. Its four methods keep their
signatures (`Upsert`, `ClaimNextJobs(int timeoutMinutes, int batchSize, int maxAttempts, ct)`, `MarkJobComplete`,
`MarkJobFailed`):

| Gap (scenario) | Fix |
|---|---|
| Claim reads every synced row (1) | Partial index: `(event_timestamp) WHERE last_synced_event_timestamp < event_timestamp` |
| No backoff: attempts used up in seconds (3, 4) | `MarkJobFailed` sets `processing_timeout` to the next try: base × 2^(attempts − 1), up to a maximum, instead of NULL |
| App clock: a pod running ahead takes rows still leased (6) | The claim uses the database's `now()` |
| A result from a worker whose lease ran out frees or finishes the row (7) | The claim keeps each entity's lease end (`processing_timeout`) in the repository instance; `MarkJobComplete`/`MarkJobFailed` update the row only while it still has that value, and throw otherwise, as before |
| A newer event stored mid-send waits for the lease (5) | The result no longer needs the same `event_timestamp`. `MarkJobComplete` sets `last_synced_event_timestamp` to the sent event, which is older than the new one, so the row is due at once. `MarkJobFailed` skips the backoff when a newer event is waiting |

One rule comes with keeping the signatures: **a claim and its results must go through the same repository instance,
used by one worker at a time.** A scoped repository in each worker's scope does that. Passing the lease end
(`processing_timeout` from the claimed row) to the Mark methods would remove the rule, but changes their signatures.

The improved version matches ours in scenarios 1 and 3 to 7. Scenario 2 still differs: one result statement per entity.

### What else it should have (most need a signature change or a new method)

1. **Results for a whole batch in one statement** (`MarkJobsComplete(list)`, `MarkJobsFailed(list)`): the rest of the
   gap in scenario 2, and one round trip per batch instead of per entity.
2. **The error on the row** (`last_error`): a row that used up its attempts is otherwise invisible and unexplained. It
   needs an error parameter on `MarkJobFailed`, and a query or alert on the rows given up.
3. **Give up at once on errors a retry can't fix** (validation errors, bad data): a `permanent` flag on `MarkJobFailed`.
4. **The lease end passed to the Mark methods**: removes the one-instance rule above.
5. **A release that spends no attempt**, for rate limits and lost answers. A call whose answer was lost (timeout) may
   still be applied by Salesforce: hold the entity for the API timeout before sending a newer event, so the late write
   can't overwrite newer data.
6. **Count attempts on failure, not on claim**: `process_count` goes up at the claim, so a pod crash or shutdown mid-batch
   uses up attempts. This one needs no signature change.
7. **Keep given-up rows out of the claim's index**: they stay in it (oldest first) and every claim reads past them. Add
   `process_count < N` to the index predicate (N fixed), or clear them out to another table.

## Run: Claim comparison, 2026-10-21 23:43 UTC

### 1. Claim time as synced rows pile up

180 due rows per claim, 20 claims (each batch marked done between claims, not timed). Synced rows are the table's history: entities already in Salesforce.

| Synced rows | Repository | Claim median (ms) | Claim max (ms) |
|---:|---|---:|---:|
| 0 | Ours (InboxRepository) | 2.6 | 97.3 |
| 0 | Simple (primary key only) | 1.6 | 12.5 |
| 0 | Simple + partial index | 1.5 | 1.7 |
| 0 | Simple, improved | 1.3 | 5.8 |
| 100,000 | Ours (InboxRepository) | 1.3 | 2.4 |
| 100,000 | Simple (primary key only) | 3.8 | 4.3 |
| 100,000 | Simple + partial index | 1.2 | 2.1 |
| 100,000 | Simple, improved | 1.3 | 2.0 |
| 1,000,000 | Ours (InboxRepository) | 1.1 | 1.9 |
| 1,000,000 | Simple (primary key only) | 29.5 | 30.7 |
| 1,000,000 | Simple + partial index | 1.2 | 2.2 |
| 1,000,000 | Simple, improved | 1.3 | 2.1 |

### 2. Drain: every send succeeds

20,000 due rows, no synced history, 4 workers, batch 180, Salesforce answers at once.

| Repository | Time (s) | Rows/s | Outcome statements |
|---|---:|---:|---:|
| Ours (InboxRepository) | 0.2 | 116,322 | 112 (one per batch) |
| Simple (primary key only) | 2.6 | 7,603 | 20,000 (one per entity) |
| Simple + partial index | 2.5 | 7,939 | 20,000 (one per entity) |
| Simple, improved | 2.7 | 7,480 | 20,000 (one per entity) |

### 3. Rows that always fail

100 rows that always fail (oldest events) and 5,000 good rows. 4 workers, batch 180, 20 ms per Salesforce call, 8 attempts. Ours and the improved one back off 30 s after the first failure (our production default).

| Repository | Good rows synced (s) | Sends of failing rows | Failing rows given up | Failing rows show their error |
|---|---:|---:|---:|---|
| Ours (InboxRepository) | 0.2 | 100 | 0 | yes (last_error; retried after the backoff) |
| Simple + partial index | 0.9 | 800 | 100 | no (nothing stored) |
| Simple, improved | 0.9 | 100 | 0 | no (nothing stored) |

### 4. Salesforce down for 10 seconds

2,000 due rows, 4 workers, batch 180, 50 ms per call, every send fails for the first 10 s, then succeeds. 8 attempts. Ours and the improved one back off 1 s, doubling (scaled down from 30 s).

| Repository | Synced | Given up (never claimed again) | Time to settle (s) |
|---|---:|---:|---:|
| Ours (InboxRepository) | 2,000 | 0 | 15.5 |
| Simple + partial index | 0 | 2,000 | 10.0 |
| Simple, improved | 2,000 | 0 | 15.8 |

### 5. A newer event arrives while the older one is being sent

20 entities, 1 worker, batch 20, 500 ms per call, lease 20 s (the improved one: 1 min, its lease is whole minutes). 200 ms into the first send, a newer event is stored for each entity.

| Repository | Newer event synced after (median, s) | Results that found no matching row |
|---|---:|---:|
| Ours (InboxRepository) | 0.8 | 0 |
| Simple + partial index | 20.3 | 20 |
| Simple, improved | 0.8 | 0 |

### 6. A pod whose clock runs 30 s ahead

2,000 due rows, 2 workers, batch 50, lease 20 s (the improved one: 1 min). One worker's calls take 2 s; the other's take 500 ms and its clock runs 30 s ahead. Ours and the improved one use the database clock: both workers are the same.

| Repository | Entities sent by two workers at once | Sends | Results that found no matching row |
|---|---:|---:|---:|
| Ours (InboxRepository) | 0 | 2,000 | 0 |
| Simple + partial index | 550 | 2,550 | 0 |
| Simple, improved | 0 | 2,000 | 0 |

### 7. A late result after the lease ran out

100 rows, lease 2 s (the improved one: 1 min). Worker A claims them; 0.2 s after its lease ran out, worker B claims them again and starts sending. 0.8 s later A reports a failure; 0.3 s after that worker C claims. Each worker is its own repository instance.

| Repository | Rows B took over | Rows C claimed while B was still sending | A's late results refused |
|---|---:|---:|---:|
| Ours (InboxRepository) | 100 | 0 | 100 (skipped) |
| Simple + partial index | 100 | 100 | 0 |
| Simple, improved | 100 | 0 | 100 |
