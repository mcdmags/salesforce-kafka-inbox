-- TEST / BENCHMARK ONLY: records the lifetime of every entity claim so concurrent processing of the
-- same entity (e.g. the same account) can be detected after the fact. Not part of the migrations; do not install in production.
-- Run it after the migrations: it attaches to every inbox table, where every entity is leased on its row.
--
-- A claim starts when a worker moves a due row's next_attempt_at to the end of its lease (ClaimAsync) and ends when
-- the row's next_attempt_at changes while it is leased (an outcome or the end of the lease). Ordering uses a sequence, not
-- the clock: wall-clock time can step backwards (NTP / VM time sync), while a claim that can only happen after another
-- worker's release commits always draws a higher sequence number than that release.

create sequence if not exists sf_claim_audit_seq;

drop view if exists sf_claim_overlaps;
drop table if exists sf_claim_audit;  -- recreated: its shape follows the inbox key
create table sf_claim_audit (
  inbox          text        not null,   -- the inbox table
  entity_id      text        not null,
  claim_seq      bigint      not null,
  claim_txid     bigint      not null,   -- the claiming transaction: its rows are one batch
  release_seq    bigint,
  claimed_at     timestamptz not null,   -- for humans only
  released_at    timestamptz,
  primary key (inbox, entity_id, claim_seq)
);

create or replace function sf_claim_audit_fn() returns trigger language plpgsql as $$
begin
  -- A leased row (not due) whose next_attempt_at changes is released. If another worker takes the row over while a claim
  -- is still open, the first claim stays open, so the takeover shows up in sf_claim_overlaps.
  if old.next_attempt_at > now() and new.next_attempt_at is distinct from old.next_attempt_at then
    update sf_claim_audit set release_seq = nextval('sf_claim_audit_seq'), released_at = clock_timestamp()
    where inbox = tg_table_name and entity_id = new.entity_id and release_seq is null;
  end if;
  if old.next_attempt_at <= now() and new.next_attempt_at > now() then
    insert into sf_claim_audit (inbox, entity_id, claim_seq, claim_txid, claimed_at)
    values (tg_table_name, new.entity_id, nextval('sf_claim_audit_seq'), txid_current(), clock_timestamp());
  end if;
  return new;
end $$;

drop trigger if exists sf_claim_audit_trg on sf_account_inbox;
create trigger sf_claim_audit_trg before update of next_attempt_at on sf_account_inbox
  for each row execute function sf_claim_audit_fn();
drop trigger if exists sf_claim_audit_trg on sf_account_relationship_inbox;
create trigger sf_claim_audit_trg before update of next_attempt_at on sf_account_relationship_inbox
  for each row execute function sf_claim_audit_fn();
drop trigger if exists sf_claim_audit_trg on sf_contact_inbox;
create trigger sf_claim_audit_trg before update of next_attempt_at on sf_contact_inbox
  for each row execute function sf_claim_audit_fn();

-- Overlapping claims on the same entity (must be empty):
create view sf_claim_overlaps as
select a.inbox, a.entity_id,
       a.claim_seq as first_claim_seq, a.release_seq as first_release_seq,
       b.claim_seq as second_claim_seq
from sf_claim_audit a
join sf_claim_audit b
  on b.inbox = a.inbox and b.entity_id = a.entity_id
 and b.claim_seq > a.claim_seq
 and b.claim_seq < coalesce(a.release_seq, 9223372036854775807);
