-- TEST / BENCHMARK ONLY: records the lifetime of every entity claim so concurrent processing of the
-- same entity (e.g. the same account) can be detected after the fact. Not part of schema.sql; do not install in production.
--
-- A claim starts when a worker sets lock_token (InboxSql.ClaimKeys) and ends when that worker clears
-- the token (Complete / Fail / Dead / Release). Ordering uses a sequence, not the clock: wall-clock time can step
-- backwards (NTP / VM time sync), while a claim that can only happen after another worker's release commits
-- always draws a higher sequence number than that release.

create sequence if not exists sf_claim_audit_seq;

drop view if exists sf_claim_overlaps;
drop table if exists sf_claim_audit;  -- recreated: its shape follows the inbox key
create table sf_claim_audit (
  entity_type    text        not null,   -- sf_event_type / sf_event_key of the claimed rows' payload
  entity_key     text        not null,
  lock_token     uuid        not null,
  claim_seq      bigint      not null,
  release_seq    bigint,
  claimed_at     timestamptz not null,   -- for humans only
  released_at    timestamptz,
  versions       bigint[]    not null default '{}',
  primary key (entity_type, entity_key, lock_token)
);

create or replace function sf_claim_audit_fn() returns trigger language plpgsql as $$
declare
  -- The payload never changes, so old and new give the same entity
  t text := sf_event_type(new.payload);
  k text := sf_event_key(new.payload);
begin
  -- Released only when the holder clears its token. If another worker overwrites a token that is still
  -- set, the first claim stays open, so the takeover shows up in sf_claim_overlaps.
  if old.lock_token is not null and new.lock_token is null then
    update sf_claim_audit set release_seq = nextval('sf_claim_audit_seq'), released_at = clock_timestamp()
    where entity_type = t and entity_key = k and lock_token = old.lock_token and release_seq is null;
  end if;
  if new.lock_token is not null and new.lock_token is distinct from old.lock_token then
    insert into sf_claim_audit (entity_type, entity_key, lock_token, claim_seq, claimed_at, versions)
    values (t, k, new.lock_token, nextval('sf_claim_audit_seq'), clock_timestamp(), array[sf_event_version(new.payload)])
    on conflict (entity_type, entity_key, lock_token) do update set versions = sf_claim_audit.versions || excluded.versions;
  end if;
  return new;
end $$;

drop trigger if exists sf_claim_audit_trg on sf_inbox;
create trigger sf_claim_audit_trg
  before update of lock_token on sf_inbox
  for each row execute function sf_claim_audit_fn();

-- Overlapping claims on the same entity (must be empty):
create view sf_claim_overlaps as
select a.entity_type, a.entity_key,
       a.lock_token as first_token,  a.claim_seq as first_claim_seq,  a.release_seq as first_release_seq, a.versions as first_versions,
       b.lock_token as second_token, b.claim_seq as second_claim_seq, b.versions as second_versions
from sf_claim_audit a
join sf_claim_audit b
  on b.entity_type = a.entity_type and b.entity_key = a.entity_key
 and b.lock_token <> a.lock_token
 and b.claim_seq > a.claim_seq
 and b.claim_seq < coalesce(a.release_seq, 9223372036854775807);
