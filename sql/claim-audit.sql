-- TEST / BENCHMARK ONLY: records the lifetime of every account claim so concurrent processing of the
-- same account can be detected after the fact. Not part of schema.sql; do not install in production.
--
-- A claim starts when a worker sets lock_token (InboxSql.ClaimAccounts) and ends when that worker clears
-- the token (Complete / Fail / Defer). Ordering uses a sequence, not the clock: wall-clock time can step
-- backwards (NTP / VM time sync), while a claim that can only happen after another worker's release commits
-- always draws a higher sequence number than that release.

create sequence if not exists sf_claim_audit_seq;

create table if not exists sf_claim_audit (
  account_ext_id text        not null,
  lock_token     uuid        not null,
  claim_seq      bigint      not null,
  release_seq    bigint,
  claimed_at     timestamptz not null,   -- for humans only
  released_at    timestamptz,
  versions       bigint[]    not null default '{}',
  primary key (account_ext_id, lock_token)
);

create or replace function sf_claim_audit_fn() returns trigger language plpgsql as $$
begin
  -- Released only when the holder clears its token. If another worker overwrites a token that is still
  -- set, the first claim stays open, so the takeover shows up in sf_claim_overlaps.
  if old.lock_token is not null and new.lock_token is null then
    update sf_claim_audit set release_seq = nextval('sf_claim_audit_seq'), released_at = clock_timestamp()
    where account_ext_id = old.account_ext_id and lock_token = old.lock_token and release_seq is null;
  end if;
  if new.lock_token is not null and new.lock_token is distinct from old.lock_token then
    insert into sf_claim_audit (account_ext_id, lock_token, claim_seq, claimed_at, versions)
    values (new.account_ext_id, new.lock_token, nextval('sf_claim_audit_seq'), clock_timestamp(), array[new.version])
    on conflict (account_ext_id, lock_token) do update set versions = sf_claim_audit.versions || new.version;
  end if;
  return new;
end $$;

drop trigger if exists sf_claim_audit_trg on sf_inbox;
create trigger sf_claim_audit_trg
  before update of lock_token on sf_inbox
  for each row execute function sf_claim_audit_fn();

-- Overlapping claims on the same account (must be empty):
create or replace view sf_claim_overlaps as
select a.account_ext_id,
       a.lock_token as first_token,  a.claim_seq as first_claim_seq,  a.release_seq as first_release_seq, a.versions as first_versions,
       b.lock_token as second_token, b.claim_seq as second_claim_seq, b.versions as second_versions
from sf_claim_audit a
join sf_claim_audit b
  on b.account_ext_id = a.account_ext_id
 and b.lock_token <> a.lock_token
 and b.claim_seq > a.claim_seq
 and b.claim_seq < coalesce(a.release_seq, 9223372036854775807);
