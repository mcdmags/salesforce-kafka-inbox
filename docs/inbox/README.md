# The inbox

How events are stored, claimed by workers and recorded once sent: the inbox tables, the lease that keeps an entity to
one worker while it talks to Salesforce, and why it works this way. Back to the [main README](../../README.md).

- [How events are stored](#how-events-are-stored)
- [A newer event while an entity is being synced](#a-newer-event-while-an-entity-is-being-synced)
- [How a lease works](#how-a-lease-works)
- [Why there is no `processing_timeout` column](#why-there-is-no-processing_timeout-column)
- [Why there is no lock token](#why-there-is-no-lock-token)
- [Why the outcome statements use `unnest`](#why-the-outcome-statements-use-unnest)
- [Why the Salesforce calls run outside a transaction](#why-the-salesforce-calls-run-outside-a-transaction)
- [Retries and dead letters](#retries-and-dead-letters)
- [Unknown outcomes](#unknown-outcomes)
- [Parallel processing: what is guaranteed](#parallel-processing-what-is-guaranteed)
- [Schema and migrations](#schema-and-migrations)

## How events are stored

**Ingest.** `KafkaInboxConsumer` splits each message ([Event format](../../README.md#event-format)) and reads each event it gets
with the typed event of its entity type (`EventReader.TryRead`, the same read the flow uses), storing it only if
it is valid: valid JSON for that type, the entity object with a non-empty id, a `meta.time` with an explicit offset,
and a `meta.type` of `inserted`, `updated` or `deleted`. A message that is no aggregate (no time with an offset, no
valid type, neither snapshot, no account `externalId`, `before` and `after` of different accounts), or an event that
is invalid, is logged with its topic, partition and offset and skipped. A row gets `entity_id` = the id from its
event, `event_time` = its `meta.time` and `payload` = the event the split made. The message's offset is stored only
after all of its events are; if storing one fails, the consumer rewinds and stores the whole message again. That is
safe because storing is idempotent: an event already stored (or synced) changes nothing, so a crash halfway through a
message just stores the rest on redelivery.

**One inbox table per object type**, all the same shape (one `ConfigureInbox` call per inbox row type in
`SyncDbContext.OnModelCreating`; table names are code constants, checked to be plain identifiers before they are put
into SQL). An inbox row also holds its entity's sync state: when it is due and the lease of the worker processing
it; no table is shared by the types. The tables the migrations create:

```sql
CREATE TABLE sf_account_inbox (                 -- and sf_account_relationship_inbox, sf_contact_inbox, ...
  entity_id       text        PRIMARY KEY,       -- the entity's id, read from the typed event by the consumer; one row per entity
  payload         jsonb       NOT NULL,          -- the entity's event, split from the Kafka message; never read by SQL
  attempt_count   int         NOT NULL DEFAULT 0,          -- times this event failed (a newer event starts at 0)
  last_error      text,
  last_updated    timestamptz NOT NULL DEFAULT now(),
  event_time      timestamptz NOT NULL,                    -- the event's meta.time, read by the consumer
  next_attempt_at timestamptz             -- null: idle (done, or dead with last_error); else claimable from then on (backoff and lease included)
);
CREATE INDEX sf_account_inbox_ready ON sf_account_inbox (next_attempt_at) WHERE next_attempt_at IS NOT NULL;
```

The inboxes vacuum after 2% of their rows changed (`autovacuum_vacuum_scale_factor = 0.02`, threshold 1,000), not
the default 20%: every event, every claim and every outcome updates them.

**One row per entity.** `entity_id` is the primary key, with no separate row id: an inbox row is an entity's newest event and what became of it, not
one event. Every event is the full record, so keeping only the newest loses nothing the sync needs (Kafka, within its
retention, is the history). A newer event overwrites the row, whatever became of the old one, as a new event: due, no
attempts, no error. An older one changes nothing. One as new (a Kafka redelivery) changes nothing either if the row
is done (idle, `next_attempt_at` and `last_error` null): that time is in Salesforce already, so there is nothing to claim or write. Otherwise (the event pending,
being written, retried or dead) it overwrites the row like a newer one. A rule on the payload instead would wrongly
drop the last event of A → B → A; by time, the last A is newer than B and is written.

Of the event, an inbox has only its entity and its time as columns, both read in C# by the consumer, and there is
**no SQL that names a JSON field** (except the one-time fill of `event_time` for rows stored before it existed, in the
`InboxEventTime` migration): the event's field names are yours to change in the payload classes. The sync's own
bookkeeping is in the row's own columns.

`event_time` holds `meta.time` cut to whole microseconds (`InboxRow.ToWholeMicroseconds`): .NET reads the time to
100 ns ticks, and Postgres keeps microseconds but would round a finer time (`.1234567` to `.123457`). Cut by the
consumer, which two events count as equally new (the store compares their times) is decided the same way for every
event, never by how a time is rounded on its way in.

**Store** (`InboxRepository.AddAsync`, one statement): insert the event into the entity's row (`ON CONFLICT (entity_id) DO
UPDATE ... WHERE event_time < excluded.event_time OR (event_time = excluded.event_time AND (next_attempt_at IS NOT NULL OR last_error IS NOT NULL))`),
keeping any lease on it. A new entity, or an idle one (`next_attempt_at` null), becomes due at once, and one that
is already due, backing off, deferred, held or leased keeps its time (a leased row's is the lease's end). The events
of an entity that arrive before it is claimed overwrite its row one after the other, and only the last is written.
The lock the store takes on the existing row is what orders it against a claim or an outcome of the same entity.

**Claim** (`InboxRepository.ClaimAsync`, the same statement for every type), one statement that commits on its own: lock up to `Sync:BatchSize` of the inbox's rows that are due (`next_attempt_at <= now()`),
longest-waiting first, off the ready index, `FOR UPDATE SKIP LOCKED`; lease them (`next_attempt_at =
now() + Sync:Lease`), and return them with their payloads and their `event_time`. Only pending rows are due: every
outcome that finishes a row sets its `next_attempt_at` null. A leased row is not due until its lease ends, so "due"
is the only condition: no separate lease check, and a lease that expired (its worker died) is simply due again.

Two workers can't hold one entity: they would lock the same row. Under READ COMMITTED a worker that reads a row
before another worker's lease on it commits re-checks the row when it locks it, finds it no longer due, and drops it
(`ClaimRaceTests`). Storing an event locks the row too, so a claim skips an entity whose event is being stored, and a
second event of the entity waits until the first has committed, and then overwrites it.

**Plans.** Every lookup is by key: a primary key (`entity_id`, `= ANY` of an array, never a join the planner could turn into a
scan), or the front of the ready index. The claim on `sf_sync_state` before this design
was checked with `EXPLAIN ANALYZE` (generic plans) against statistics taken while everything was idle and then a
100,000-entity backlog, and against statistics taken during a backlog that had since drained: the same index plans
each time, so no planner settings are pinned. The statements on the inbox row have the same shapes (the claim's due
rows off the ready index, the outcomes by primary key, the claimed times joined to them as a semi join) and were
checked with generic plans on an analyzed 200,000-row inbox, not yet against stale statistics. Claim and outcome
costs measured for the earlier claims are in [docs/benchmarks/README.md](../benchmarks/README.md).

Then, per claimed entity, in C#: deserialize its event, map → write → done (one statement for the batch marks the
rows `done`). An event that can't be read is dead-lettered on its own (`INVALID_EVENT`). All outcome statements are
set-based and conditional on the entity's row still being pending with the event the claim read, and each ends the lease of the rows it records
by setting `next_attempt_at`: done and dead-lettered rows are idle (null), retried ones due again when their backoff
ends, held ones when the hold ends. A row with no outcome (not attempted, or the whole claim deferred) only has its
lease ended (`EndLeasesAsync`): due again at once, or when the deferral ends, with no attempt spent.

**Overwritten while claimed** (explained step by step in
[A newer event while an entity is being synced](#a-newer-event-while-an-entity-is-being-synced)). The lease keeps other
workers away from an entity, not the consumer: a newer event can overwrite the row while the worker writes the event it
claimed. So every outcome statement updates only the claimed rows that still hold the `event_time` the claim read and
are still under its lease (`InboxRepository.StillClaimed`, part of each statement's WHERE: the claim's entity ids, times
and lease ends, `unnest(@entityIds, @times, @leaseEnds)`, each
row looked up by primary key and compared timestamptz to timestamptz, exactly: the times are the microseconds read
back from the column) and records outcomes on those only: done, retried, dead, held. An overwritten row stays
pending with the newer event, not marked done, and the unit's last statement (`EndLeasesAsync`, run only if the
outcomes left a claimed row under the lease) ends its lease and makes it due now (held: when the hold ends), not when
the lease would have ended, so the next claim writes it. The consumer and the outcome update the same row, so
Postgres's row lock orders them (see below).

```sql
-- Dead letters of one type, by cause
SELECT entity_id, payload -> 'meta' ->> 'time' AS time, last_error
FROM sf_account_inbox WHERE next_attempt_at IS NULL AND last_error IS NOT NULL ORDER BY last_updated DESC;

-- What an inbox holds for one entity: its newest event
SELECT entity_id, next_attempt_at, attempt_count, last_error, payload
FROM sf_account_inbox WHERE entity_id = 'ACC-000001';

-- Its state: due when (a lease is a due time in the future)
SELECT next_attempt_at FROM sf_account_inbox WHERE entity_id = 'ACC-000001';
```

(Queries like these, for people, may look inside the payload; the worker's own SQL never does.)

## A newer event while an entity is being synced

`sf_account_inbox` has one row per account: for ACC-1 it always holds ACC-1's latest event. A newer event for ACC-1
replaces that row's payload and `event_time` and sets it back to `pending`.

Syncing ACC-1 takes three steps, and they are not instant:

1. **Claim.** A worker reads ACC-1's row and leases ACC-1: no other worker can sync ACC-1 meanwhile.
2. **Write to Salesforce.** The worker sends what it read: a read call, then a write call, tens to hundreds of
   milliseconds.
3. **Record the result.** The worker marks the row `done` (or retried, or `dead`).

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
the worker simply marked the row `done`, the row would say `done` with "B" in it, so "B" would never be claimed;
even a redelivery of "B" would count as synced (the store leaves a done row of the same time as it is); and
Salesforce would keep "A" for good. The newest update would be lost, with no error anywhere.

**The guard.** In step 3 the worker records its result only on a row that still has the `event_time` it claimed
(09:59), checked by the statement that records it:

- **yes**: nothing arrived during the write, so it records `done` as usual;
- **no** (it is now 10:00): a newer event arrived, so it records **nothing** on the row. The row stays `pending` with
  "B", and when the lease ends ACC-1 is due again: the next claim writes "B".

Every result the worker can record (done, retried, dead, held) goes through the same check, since
each would otherwise overwrite the newer event's status.

**The check and the consumer can't interleave.** The check is part of the worker's own update of ACC-1's row
(`... WHERE next_attempt_at IS NOT NULL AND event_time = 09:59`), and the consumer updates the same row, so Postgres's row
lock puts one after the other. If the consumer's write committed first, the worker's update finds 10:00 and changes
nothing. If the consumer is still writing, the worker's update waits for it and then checks its condition again on
the row the consumer left (under READ COMMITTED an UPDATE that waited for a row re-checks its WHERE on the row's
newest version): 10:00 again. If the worker's update came first, the consumer waits until the worker has recorded its
result and then replaces the row with "B", `pending` and due. Either way "B" is synced next. Tests:
`OneRowPerEntityTests.An_outcome_for_a_claimed_event_is_not_recorded_on_the_newer_event_that_overwrote_it` and
`ClaimRaceTests.An_outcome_that_waits_for_a_store_of_a_newer_event_is_not_recorded_on_it` (every kind of result) and `LeaseTests.A_newer_event_stored_while_the_claimed_one_is_written_is_written_next_whatever_the_claimed_ones_outcome`
(end to end, Salesforce ends at "B").

**How often.** Only while a batch is out, from its claim to its outcome: the Salesforce round trip (0.3 to 1 s on a
real org). A row waiting in the inbox isn't claimed. For an entity whose events arrive on average every T seconds, the
chance that one arrives during a round trip of W seconds is about 1 - e^(-W/T): 5% for a save every 10 s and a 0.5 s
round trip, 39% for a save every second. With a user saving in quick succession it is common, and costs one more
write, which the newer event needs anyway: the outcome ends the skipped row's lease (`EndLeasesAsync`), so "B" is due at
once instead of when the lease expires.

## How a lease works

A lease is one column of the entity's inbox row, `next_attempt_at` (until when), as a queue's visibility timeout: a leased row is simply not due until its lease ends. It is what keeps
an entity to one worker while that worker talks to Salesforce, without holding a database lock or a transaction all
that time.

1. **Take.** In the claim statement, `Claim` locks due rows (`FOR UPDATE SKIP LOCKED`) and leases them: a new
   `next_attempt_at = now() + Sync:Lease`. The statement commits; the row locks go, the lease stays.
2. **While live.** Other workers skip the entity. This worker calls Salesforce outside any transaction and stops
   (cancels the call) at 80% of the lease, measured from the claim, so it gives up before anyone else can claim the
   entity.
3. **Release.** The outcome statements record done, retry, dead or hold; every statement only touches rows
   whose `next_attempt_at` is still the lease end the claim set, so a worker whose entity another worker took over or
   finished changes nothing, and only rows that still hold the event the claim read (see [Overwritten while claimed](#a-newer-event-while-an-entity-is-being-synced)). Each statement also
   ends the lease of its rows and sets `next_attempt_at`: null for a finished row, else when it is due again (the end
   of the backoff or hold). Its last statement (`EndLeasesAsync`) ends the lease of the rows left (the claimed ids the outcome
   statements did not return, still matched by lease end), those not attempted and
   those a newer event overwrote meanwhile (a store keeps a leased row's `next_attempt_at`, the lease's end), and
   makes them due now (held: when the hold ends). A deferral is that statement alone, due when the deferral ends.
   An event stored at the same moment is never left behind: the store and the outcome update the same row, so either
   the store came first (the outcome skips the overwritten row and `EndLease` ends its lease) or it waits for the
   outcome transaction and then overwrites the finished row and makes the idle entity due (`ClaimRaceTests`).
4. **Crash.** Nothing to clean up. The lease expires, which makes the entity due, and the next claim takes it. Its read
   of Salesforce shows what the crashed worker did or didn't save. (A write whose outcome is unknown is held instead;
   see [Unknown outcomes](#unknown-outcomes).)

**The claim is a receipt.** The claim returns, for each row it leased, the entity id, the `event_time` it read and
the lease end it set (`InboxEvent`). Every later statement of the batch sends that receipt back for the ids it is
about, with the values from the claim (`InboxClaim.Of`), never from the table, and Postgres applies it only to rows
that still match (`StillClaimed`). Nothing holds a row while Salesforce is called (no transaction, no row lock, no
connection), so the receipt is how a statement tells a row that is still its own from one that changed.

**The statements, one by one** (`InboxRepository`):

| Statement | What it does | Why this way |
|---|---|---|
| **Store** (`AddAsync`) | Overwrites the row if the event is newer; one of the same time (a Kafka redelivery) only if the row isn't done yet. A pending row keeps its `next_attempt_at`. | A leased row's `next_attempt_at` is the lease end: setting it to now would let a second worker claim the entity while the first is still writing it. The lease holder's outcome makes the newer event due (below). |
| **Claim** (`ClaimAsync`) | Leases the longest-waiting due rows: `next_attempt_at = now() + Sync:Lease`, one statement. | `FOR UPDATE SKIP LOCKED` lets workers claim side by side without waiting for each other; under READ COMMITTED a row another claim leased after this one's snapshot is re-checked and dropped, so no row gets two leases (`ClaimRaceTests`). The ids are collected into an array, not a CTE joined back, so the planner can't turn it into a scan. A lost commit leaves its rows leased until the lease expires. |
| **Done** (`MarkDoneAsync`) | `next_attempt_at` null, error cleared. | Only on rows matching the receipt: a row a newer event overwrote is not marked done unsent. |
| **Failed** (`MarkFailedAsync`) | One more attempt: due again after an exponential backoff, or dead-lettered (idle, with the error) at the `MaxRetries`-th failure, or at once if the failure is permanent. | One statement for the whole batch: the errors go as one JSON object of id → error, read by a scalar subquery so it is parsed once (as a CTE it was re-read for every row: 230 ms for 5,000). |
| **Release** (`ReleaseAsync`) | Due again after a delay, no attempt spent: a write whose outcome is unknown (held until Salesforce has surely applied or dropped it; a newer event waits too, since the store keeps the due time), or an inbox pause (a mapping error, full storage, Salesforce's API limits), which (`PauseInbox`) also delays every other due row of that inbox. | An inbox pause fails every event of the type alike: waiting loses nothing, retrying would dead-letter them all. |
| **End leases** (`EndLeasesAsync`) | After each outcome, the claimed rows it did not touch are made due (now, or after the release delay). | These are rows a newer event overwrote, whose lease would otherwise run to its end. Matched by lease end only, not `event_time`, because the event changed but the lease is still this claim's; a row another worker has claimed since has another lease end and is left alone. The rows left are told apart by what the outcome returned, not by `last_updated`: a store in the same microsecond would look recorded, and its row would stay leased until expiry. |

## Why there is no `processing_timeout` column

A queue table often has a `processing_timeout` (or `locked_until`) column next to a status, plus a job that resets rows
stuck in `processing`. Here the lease is that column and that job in one: `next_attempt_at` is moved to `now() + Sync:Lease`
when a worker claims the row, so "being processed" and "times out at" are the same timestamp.

- **Expiry needs no cleanup.** The claim takes rows where `next_attempt_at <= now()`. A worker that died or hung leaves
  its row with a lease end in the past, so the row is due again and the next claim takes it. No job resets anything.
- **A late worker mostly changes nothing.** Every outcome statement requires the row to be pending, so a worker that
  finishes after another finished the entity matches no row (see below).
- **One column, one index.** A retry's backoff, a hold and a lease all set `next_attempt_at`, so the one partial index
  (`next_attempt_at WHERE next_attempt_at IS NOT NULL`) serves them all; a finished row has `next_attempt_at` null and
  is not in it.

The cost: `Sync:Lease` must be longer than a normal batch takes (startup checks it against the HTTP request timeout),
or the worker's outcome is dropped and another worker may already have the rows.

## Why there is no lock token

An outcome is recorded on a row only if it is still under the claim's **lease** (`next_attempt_at` is still the lease
end the claim set and read back) and still holds the **event** the claim read (`event_time`: a newer event overwrites
the row). An alternative is a `lock_token` column: each claim writes a new random token on its rows, every outcome
matches `lock_token = @token`, and finishing a row clears it. We chose not to: it is one more column to write, clear and
keep consistent on every statement, and the lease end already does the same job. While a lease runs only a claim
changes it (a store keeps it, and finishing or failing the row replaces it), and two claims never set the same
microsecond. So:

- **A worker slower than its lease** (a long pause, a network outage) that comes back after another worker took the
  entity over records nothing and ends no lease: the row has the new worker's lease end.
- **An outcome or lease end run again** (a statement retried after its reply was lost, since the statements commit one
  by one) matches nothing the second time: the first run replaced the lease end. A failure is counted once.
- **A claim retried after a lost commit** finds the rows the lost attempt leased still leased: they wait for the lease
  to expire (`Sync:Lease`), then the next claim takes them.

Tests: `LeaseTests.Worker_whose_entity_was_finished_by_another_can_record_nothing`, `ClaimRetryTests`,
`ClaimRaceTests.A_late_end_of_lease_leaves_the_lease_of_the_worker_that_claimed_the_row_since_alone` and
`ClaimRaceTests.A_failure_recorded_again_after_a_lost_reply_counts_one_attempt`.

## Why the outcome statements use `unnest`

An outcome must touch a row only if it is still under the claim's lease **and** the row still holds the event the
claim read. Both are per row: the entity id with its event time and lease end. The claim returns them, and the outcome
sends them back as parallel arrays, `@entityIds`, `@times` and `@leaseEnds`. `unnest` turns the arrays into a small
table (`('A1', t1, l1)`, `('A2', t2, l2)`, ...), which the statement compares row by row:

```sql
i.entity_id = ANY(@entityIds)
  AND EXISTS (SELECT 1 FROM unnest(@entityIds, @times, @leaseEnds) AS c(entity_id, event_time, lease_end)
              WHERE c.entity_id = i.entity_id AND c.event_time = i.event_time AND c.lease_end = i.next_attempt_at)
```

- **Why not only the entity id:** a newer event for the same entity has the same id, so the id alone can't tell "the
  event I wrote" from "the event that overwrote it". Only the pair can.
- **Why `unnest` and not a statement per entity:** one statement handles the whole batch, whether it is 1 or 200
  entities. `= ANY(array)` tests one array; `unnest` with several arrays is how Postgres compares arrays by position.
- **Cost:** each row is found by its primary key (`entity_id`), so the planner never scans the table.

## Why the Salesforce calls run outside a transaction

A batch runs two short units of work: the claim (`InboxRepository.ClaimAsync`, one statement) and the outcomes
(`MarkDoneAsync` and `MarkFailedAsync`, one statement each), each through `RunAsync` (the execution strategy, no explicit
transaction: every statement commits on its own). The Salesforce read and write happen between them, in no
transaction. One transaction around everything would be worse:

| One transaction around the batch would | Because |
|---|---|
| only look atomic | Salesforce can't take part in a Postgres transaction. A rollback can't undo a write Salesforce saved. |
| send the HTTP calls again | The execution strategy re-runs the whole delegate on a transient Postgres error (dropped connection, failover). |
| hold row locks for seconds, up to the request timeout (90 s) | Other workers and the consumer wait on them. |
| stall the Kafka consumer | Storing a newer event of an entity being sent waits for its row lock until Salesforce answers, and the consumer reads a partition in order, so every account behind that message waits too. Today a store never waits. |
| pin a pooled connection per worker | Workers × pods connections busy doing nothing. |
| block autovacuum | An open transaction holds back the horizon, and the inbox tables churn: bloat builds up. |

Not even the statements get a transaction of their own: each commits alone. A claim is one round trip instead of three
(BEGIN, statement, COMMIT), and rows stay leased for less time (streaming with 8 workers on 3 pods went from 6.4 s to
4.6 s). The claim's receipt makes that safe, because every statement may run twice or late ([Why there is no lock
token](#why-there-is-no-lock-token)); if a claim's commit is lost, its rows wait for the lease to expire.

The lease protects the entities between the two units. The trade-off: a crash after Salesforce saved but before
the outcomes committed leaves the events pending. That is safe, because every attempt reads Salesforce first and writes
only what differs, so the retry finds the records saved and ends `done` without writing them again. A write whose
outcome is unknown is held rather than retried at once.

## Retries and dead letters

A transient failure (`UNABLE_TO_LOCK_ROW`, `DEPENDENCY_NOT_SYNCED`, `INVALID_RECORD_TYPE`, a create answered with `DUPLICATE_VALUE` because
someone else created the record first, an unknown error code, a failed read) is retried
with exponential backoff of the entity: `Sync:RetryBaseDelay` (30 s) doubling per attempt (the row's
`attempt_count`, which only a failure increments) up to `Sync:MaxRetryDelay` (1 h); an event is dead-lettered once
it failed `Sync:MaxRetries` (8) times (`attempt_count`; a newer event of the entity starts again at 0). With the defaults the attempts come about 30 s, 1 min, 2 min,
4 min, 8 min, 16 min and 32 min apart: roughly an hour from the first failure to the dead letter. Permanent errors
are dead-lettered at once. The error that caused it stays in `last_error`, naming the code (query by cause with `last_error LIKE '%CODE%'`). Inbox pauses, rows left without an outcome
(a newer event overwrote them meanwhile) and unknown outcomes don't spend an attempt.

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
UPDATE sf_account_relationship_inbox SET attempt_count = 0, next_attempt_at = now()
WHERE next_attempt_at IS NULL AND last_error IS NOT NULL AND last_error LIKE '%DEPENDENCY_NOT_SYNCED%';
```

(`sf_account_inbox` and `sf_contact_inbox` the same way. Setting `next_attempt_at` is what makes the rows due again;
a row is claimed only while it has one, so a row changed by hand is waiting exactly when it has a `next_attempt_at`; without one it is done, or dead when it has a `last_error`.)

A newer event of a dead-lettered entity needs no re-queue: it overwrites the dead row, which is pending again (and the dead
letter's error is gone with the old event).

## Unknown outcomes

A write to Salesforce whose result never arrives may still be applied, possibly after the worker has given up. If
the entity were released and a newer event synced in the meantime, the late write would overwrite newer data.

- **What counts as unknown** (`SalesforceCalls.IsUnknownOutcome`): a cancellation or timeout (the lease cutoff, the HTTP
  timeout, shutdown while the request is in flight), a connection lost once the request may have left, a 502/504, a
  503 that is not Salesforce's own, or a response that can't be read.
- **What does not**: anything Salesforce itself answered (4xx including 429, `REQUEST_LIMIT_EXCEEDED`, per-record
  errors, a 5xx with a Salesforce error body), a connection that was never established (refused, DNS, TLS), and
  failures before the request left (getting the OAuth token: `SalesforceAuthException`). Failures of the
  record type query or the read are never unknown: nothing was written. These follow the normal rules at once.
- **What happens**: every entity of that batch is held: its row gets `next_attempt_at = now() +
  Sync:UnknownOutcomeHold` and loses its lease; it stays
  waiting with a `last_error` that starts `UNKNOWN_OUTCOME:`, and the attempt doesn't count. A
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

## Parallel processing: what is guaranteed

| Guarantee | How | Test |
|---|---|---|
| Two workers or pods never hold the same entity at once | the entity's one inbox row: locked `FOR UPDATE SKIP LOCKED` and leased, its lease re-checked when it is locked | `ConcurrencyTests.Parallel_pods_never_process_the_same_account_concurrently_and_end_at_the_newest_version` (3 pods × 8 loops, claim audit: 0 overlaps); `ClaimRaceTests` (claims while another claim or a store of the entity is open, events stored concurrently) |
| An event stored while its entity's lease ends is not left behind | storing and ending a lease update the same row, so its lock orders them; a store keeps a due row's time and makes an idle row due, and the lease of an overwritten row ends with it due now (`EndLeasesAsync`), not at the lease's end | `ClaimRaceTests.An_event_stored_while_the_outcome_transaction_has_the_row_overwrites_it_and_leaves_it_due` |
| An event arriving while its entity is being processed is not claimed until the lease is released or expires, then wins if newer | a leased row is not due until its lease ends (its `next_attempt_at`) | `LeaseTests.Event_arriving_while_its_entity_is_being_processed_waits_for_the_lease_then_wins_if_newer` |
| Events out of `meta.time` order, over any partition and consumer, end at the newest; an older record is never written after a newer one | a row is overwritten only by a newer event, or one as new while its event is not done | `ConcurrencyTests.Events_out_of_time_order_over_any_partition_and_pod_end_at_the_newest_time`, `SyncEndToEndTests.The_Kafka_key_is_not_used_and_arrival_order_does_not_matter` |
| An event stored while its entity's older event is being written is not lost: the older one's outcome isn't recorded on it, and it is written next | every outcome statement updates only claimed rows whose `event_time` is still the one the claim read (`InboxRepository.StillClaimed`, in its WHERE); storing updates the same row, so the row lock orders the two, and an outcome that waited for a store re-checks its WHERE on the row the store left (READ COMMITTED); `EndLease` ends the overwritten row's lease and leaves it due | `OneRowPerEntityTests.An_outcome_for_a_claimed_event_is_not_recorded_on_the_newer_event_that_overwrote_it` and `ClaimRaceTests.An_outcome_that_waits_for_a_store_of_a_newer_event_is_not_recorded_on_it` (done, failed, dead, released, deferred, held), `LeaseTests.A_newer_event_stored_while_the_claimed_one_is_written_is_written_next_whatever_the_claimed_ones_outcome`, `ClaimRaceTests.An_event_stored_while_the_outcome_transaction_has_the_row_overwrites_it_and_leaves_it_due` |
| A worker can't record an outcome on a finished entity | every statement is conditional on the row being pending with the event the claim read | `LeaseTests.Worker_whose_entity_was_finished_by_another_can_record_nothing`, `LeaseTests.Done_completes_an_event_only_while_it_is_pending_and_only_once` |
| Dropped Postgres connections cause no overlap, loss or regression | `PostgresRetryStrategy` (EF execution strategy), idempotent statements | `ConcurrencyTests.Pods_survive_their_Postgres_connections_being_dropped` |
| A write with an unknown outcome can't be overtaken by a newer write | the hold | `UnknownOutcomeTests.Entity_is_held_after_a_write_with_an_unknown_outcome_and_ends_at_the_newest_event` |

Windows in which a guarantee does not hold, stated plainly: (1) a worker whose **Postgres** work to record a hold
fails (database unreachable) or whose process is killed mid-write leaves only the lease; if Salesforce then takes
longer than the rest of the lease to apply the request, a newer write can be overtaken. (2) A lease that expires
while its worker is still running (clock stall, a pause longer than the 20% margin) lets a second worker claim the
entity; the first can no longer record anything, but a request it already sent is in Salesforce's hands. (3)
Ordering is only as good as `meta.time`: two different events of an entity with the same time are not ordered by
content (the one stored later overwrites the other; if it does so while the other is being written, the check by
time can't tell them apart, the other's outcome is recorded on it and it isn't written until a newer event comes;
and if the other is done already, it changes nothing), and a source clock that goes backwards makes a newer change
look older (it changes nothing). (4)
The inbox holds each entity's newest event only, not a log: an event overwritten before it was claimed, or older
than the row when it arrived, leaves no trace in Postgres; Kafka, within its retention, has it.

## Schema and migrations

`SyncDbContext.OnModelCreating` defines every table: the inboxes (one `ConfigureInbox` call per inbox row type),
the claim audit table, with the column names in snake_case, the partial indexes (`HasFilter`) and the autovacuum
settings (`HasStorageParameter`). EF migrations in `src/SalesforceSync.Data/Migrations` create them. Queries stay raw SQL
(in `InboxRepository`) where LINQ can't express them. There is no rate limiting.

`InitialSchema` creates the inboxes (one row per entity, with its due time and lease); later migrations add and then
drop the rate-limit tables (`DropApiUsageAndBudget`, `DropRateLimitTables`). The earlier history (a chain of migrations that evolved the first
design into this one) was deleted and `InitialSchema` regenerated from the model, so **a database created with the
earlier migrations doesn't match it**: its `__EFMigrationsHistory` names migrations that no longer exist. Start from a
fresh database (or drop the sync's tables and `__EFMigrationsHistory`).

**A custom schema.** The tables live in whatever schema is first on the connection's `search_path` (`public` by
default). `HasDefaultSchema("x")` only changes the SQL that EF generates; the raw statements (`InboxRepository`)
name their tables unqualified, so with a schema that is not on the path they fail with `relation "sf_account_inbox" does not
exist` while EF's own queries and migrations work. The fix is on the connection string, once for everything:

```
Host=...;Database=...;Username=...;Search Path=your_schema
```

(and `HasDefaultSchema("your_schema")` too, so EF's migration history table is in the same schema). To check what a
connection sees: `select current_schemas(true), to_regclass('sf_account_inbox');`, where a null `to_regclass` means the schema is
not on the path. Qualifying the raw SQL instead (the table name from `db.Model.GetDefaultSchema()`) is the alternative when the connection string can't change.

The pods never migrate. Deploying is one step, before the pods roll out: the schema, as an EF migrations bundle (build it
once per release):

```bash
dotnet ef migrations bundle --project src/SalesforceSync.Data --self-contained -r linux-x64 --output efbundle
./efbundle --connection "$CONNECTION"
```

The AppHost mimics the pipeline: its `migrate-database` resource runs `SalesforceSync --migrate`, which applies the
migrations (`SyncSchema.MigrateAsync`), and `salesforce-sync` waits for it to complete.
`MigrateAsync` takes a session-level advisory lock (`0x5346_5348`), so two overlapping runs migrate one at a time.
(EF 9 also locks its history table while it migrates.) Changing the model:

```bash
dotnet tool restore
dotnet ef migrations add <Name> --project src/SalesforceSync.Data --output-dir Migrations
```

A unit test (`Every_change_to_the_model_has_its_migration`, `HasPendingModelChanges`) fails while a model change
has no migration.
