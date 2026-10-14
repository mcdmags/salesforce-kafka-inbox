-- The sync's Postgres schema, applied at worker startup by InboxSchema (idempotent, under an advisory lock).
-- No statement here or in InboxSql reads inside an event: the payload is opaque jsonb, and everything the SQL needs
-- (which entity a row belongs to) is a plain column filled by the worker from its typed event classes.

-- An inbox from before there was one table per object type (any earlier shape: event columns, or only the raw
-- payload read by sf_event_* functions): keep it for inspection as sf_inbox_legacy, rows unmigrated. Its indexes and
-- id sequence keep their names on a rename, so they are renamed too.
do $$
declare
  legacy text := 'sf_inbox_legacy';
  n int := 1;
  idx text;
begin
  if to_regclass(format('%I.sf_inbox', current_schema())) is not null then
    while to_regclass(format('%I.%I', current_schema(), legacy)) is not null loop
      n := n + 1;
      legacy := 'sf_inbox_legacy_' || n;
    end loop;
    for idx in select c.relname from pg_index i join pg_class c on c.oid = i.indexrelid
               where i.indrelid = format('%I.sf_inbox', current_schema())::regclass loop
      execute format('alter index %I rename to %I', idx, legacy || substr(idx, length('sf_inbox') + 1));
    end loop;
    execute format('alter sequence if exists sf_inbox_id_seq rename to %I', legacy || '_id_seq');
    execute format('alter table sf_inbox rename to %I', legacy);
  end if;
end $$;

-- The functions that used to read the entity type, key and time out of the payload. Cascade: the legacy inbox's
-- expression indexes were built on them.
drop function if exists sf_event_version(jsonb) cascade;
drop function if exists sf_event_key(jsonb) cascade;
drop function if exists sf_event_type(jsonb) cascade;

-- Newest event time synced to Salesforce per entity. Lets a late or replayed older event be recognised as stale
-- without storing anything in Salesforce. Only advanced by the worker holding the entity's lease.
create table if not exists sf_sync_state (
  entity_type     text        not null,          -- the strategy's entity type, e.g. 'account'
  entity_id       text        not null,
  synced_time     bigint      not null,          -- the newest synced event's time, microseconds since the epoch (from C#)
  synced_at       timestamptz not null default now(),
  constraint sf_sync_state_pkey primary key (entity_type, entity_id)
);

-- Upgrade in place: the same values under their earlier names, so watermarks survive.
do $$
begin
  if exists (select 1 from information_schema.columns
             where table_schema = current_schema() and table_name = 'sf_sync_state' and column_name = 'entity_key') then
    alter table sf_sync_state rename column entity_key to entity_id;
    alter table sf_sync_state rename column synced_version to synced_time;
  end if;
end $$;

-- >>> inbox table template
-- Applied once per synced object type, with {inbox} replaced by that type's table name (a code constant, e.g.
-- sf_account_inbox, sf_account_relationship_inbox). Every inbox has the same shape: one row per Kafka event plus its
-- processing state. Rows of one entity_id are claimed together and never by two workers at once.
create table if not exists {inbox} (
  id              bigserial   primary key,
  entity_id       text        not null,          -- the Kafka message key (the entity's id); set by the consumer
  payload         jsonb       not null,          -- the Kafka message value, unchanged; never read by SQL
  status          text        not null default 'pending',  -- pending | done | superseded | dead
  retry_count     int         not null default 0,
  last_updated    timestamptz not null default now(),
  last_error      text,
  last_error_code text,                          -- e.g. FIELD_CUSTOM_VALIDATION_EXCEPTION, to query dead letters by cause
  next_attempt_at timestamptz not null default now(),  -- not claimable before; while leased, the end of the lease
  locked_until    timestamptz,                   -- lease while a worker processes the row
  lock_token      uuid,                          -- which worker holds the lease
  received_at     timestamptz not null default now()
);

-- The claim's lookups, all on plain columns (InboxSql.LockHeads):
-- rows that may be claimed now, oldest first. Leased rows have next_attempt_at at the end of their lease, so they are
-- beyond the range the claim scans, and re-enter it if the lease expires;
create index if not exists {inbox}_ready on {inbox} (next_attempt_at, id) where status = 'pending';
-- an entity's pending rows in arrival order: its head, the rows to lease, and the walk over all pending entities;
create index if not exists {inbox}_pending on {inbox} (entity_id, id) where status = 'pending';
-- an entity's leased rows: is it in flight, and does this worker still hold it.
create index if not exists {inbox}_leased on {inbox} (entity_id, locked_until) where locked_until is not null;
-- <<< inbox table template

-- Shared token bucket + daily budget guard
create table if not exists sf_rate_limit (
  name                 text primary key,
  tokens               float8      not null,
  capacity             float8      not null,   -- max burst
  base_refill_per_sec  float8      not null,   -- normal rate
  refill_per_sec       float8      not null,   -- current rate (reduced near the daily limit)
  updated_at           timestamptz not null default clock_timestamp(),
  paused_until         timestamptz,
  api_used             bigint,      -- Sforce-Limit-Info: api-usage (org daily, rolling 24h)
  api_max              bigint,
  burst_used           bigint,      -- Sforce-Limit-Info: api-bursts
  burst_max            bigint,
  app_used             bigint,      -- Sforce-Limit-Info: per-app-api-usage (connected app allocation)
  app_max              bigint,
  app_name             text,
  usage_updated_at     timestamptz,
  daily_budget         bigint       -- this integration's calls per rolling 24h: RateLimit:DailyBudgetShare x the lower of
                                    -- the configured allocation and api_max
);

-- Upgrade path for databases created from the earlier schema.
-- The view depends on these columns and blocks "alter column type", so drop it here; it is recreated below.
drop view if exists sf_limit_status;
alter table sf_rate_limit alter column api_used type bigint;
alter table sf_rate_limit alter column api_max  type bigint;
alter table sf_rate_limit add column if not exists burst_used bigint;
alter table sf_rate_limit add column if not exists burst_max  bigint;
alter table sf_rate_limit add column if not exists app_used   bigint;
alter table sf_rate_limit add column if not exists app_max    bigint;
alter table sf_rate_limit add column if not exists app_name   text;
alter table sf_rate_limit add column if not exists daily_budget bigint;  -- this integration's calls per rolling 24h

-- This integration's own Salesforce calls in 10-minute buckets: the rolling 24-hour window that
-- sf_rate_limit.daily_budget is enforced against (the org-wide total comes from Sforce-Limit-Info).
create table if not exists sf_api_usage (
  bucket  timestamptz primary key,
  calls   bigint      not null
);

-- What Salesforce last reported (handy for dashboards / alerts)
create or replace view sf_limit_status as
select api_used, api_max, round(100.0 * api_used / nullif(api_max, 0), 1)     as api_pct,
       burst_used, burst_max, round(100.0 * burst_used / nullif(burst_max, 0), 1) as burst_pct,
       app_name, app_used, app_max, round(100.0 * app_used / nullif(app_max, 0), 1) as app_pct,
       refill_per_sec, base_refill_per_sec, paused_until, usage_updated_at,
       (select coalesce(sum(calls), 0) from sf_api_usage
        where bucket >= clock_timestamp() - interval '24 hours 10 minutes') as own_calls_24h,
       daily_budget
from sf_rate_limit
where name = 'salesforce';

-- Bucket size, refill rate and daily budget are set from configuration at worker startup (RateLimitInitializer); the
-- budget is lowered when Salesforce reports a lower allocation in Sforce-Limit-Info
insert into sf_rate_limit (name, tokens, capacity, base_refill_per_sec, refill_per_sec)
values ('salesforce', 5, 5, 2, 2)
on conflict (name) do nothing;
