-- Inbox of raw Kafka change events (one row per event). Events are ordered and processed per
-- (entity_type, entity_key): e.g. ('account', 'ACC-000001') or ('account-relationship', 'REL-000001').
create table if not exists sf_inbox (
  id              bigserial   primary key,
  entity_type     text        not null default 'account',
  entity_key      text        not null,
  version         bigint      not null,          -- monotonic per entity, from the source
  payload         jsonb       not null,
  status          text        not null default 'pending',  -- pending | done | superseded | dead
  retry_count     int         not null default 0,
  last_updated    timestamptz not null default now(),
  last_error      text,
  last_error_code text,                          -- e.g. FIELD_CUSTOM_VALIDATION_EXCEPTION, to query dead letters by cause
  next_attempt_at timestamptz not null default now(),
  locked_until    timestamptz,                   -- lease while a worker processes the row
  lock_token      uuid,                          -- which worker holds the lease
  received_at     timestamptz not null default now(),
  constraint sf_inbox_entity_version unique (entity_type, entity_key, version)  -- idempotent ingest
);

-- Newest event version synced to Salesforce per entity. Lets a late or replayed older event be recognised as
-- stale without storing anything in Salesforce. Only advanced by the worker holding the entity's lease.
create table if not exists sf_sync_state (
  entity_type     text        not null default 'account',
  entity_key      text        not null,
  synced_version  bigint      not null,
  synced_at       timestamptz not null default now(),
  constraint sf_sync_state_pkey primary key (entity_type, entity_key)
);

-- Upgrade databases created when the inbox only held accounts (account_ext_id)
do $$
begin
  if exists (select 1 from information_schema.columns
             where table_schema = current_schema() and table_name = 'sf_inbox' and column_name = 'account_ext_id') then
    alter table sf_inbox rename column account_ext_id to entity_key;
    alter table sf_inbox add column if not exists entity_type text not null default 'account';
    alter table sf_inbox drop constraint if exists sf_inbox_account_ext_id_version_key;
    alter table sf_inbox add constraint sf_inbox_entity_version unique (entity_type, entity_key, version);
    drop index if exists sf_inbox_pending_head;
    drop index if exists sf_inbox_ready;
    drop index if exists sf_inbox_leased;
  end if;
  if exists (select 1 from information_schema.columns
             where table_schema = current_schema() and table_name = 'sf_sync_state' and column_name = 'account_ext_id') then
    alter table sf_sync_state rename column account_ext_id to entity_key;
    alter table sf_sync_state add column if not exists entity_type text not null default 'account';
    alter table sf_sync_state drop constraint if exists sf_sync_state_pkey;
    alter table sf_sync_state add constraint sf_sync_state_pkey primary key (entity_type, entity_key);
  end if;
end $$;
alter table sf_inbox add column if not exists last_error_code text;

create index if not exists sf_inbox_pending_key on sf_inbox (entity_type, entity_key, version) where status = 'pending';
create index if not exists sf_inbox_ready       on sf_inbox (entity_type, next_attempt_at)    where status = 'pending';
create index if not exists sf_inbox_leased_key  on sf_inbox (entity_type, entity_key)         where locked_until is not null;

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
  daily_budget         bigint       -- this integration's calls per rolling 24h (RateLimit:DailyBudgetShare)
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

-- Bucket size, refill rate and daily budget are set from configuration at worker startup (RateLimitInitializer)
insert into sf_rate_limit (name, tokens, capacity, base_refill_per_sec, refill_per_sec)
values ('salesforce', 5, 5, 2, 2)
on conflict (name) do nothing;

-- Ingest (Kafka consumer):
-- insert into sf_inbox (entity_type, entity_key, version, payload)
-- values (@type, @key, @version, @payload::jsonb)
-- on conflict (entity_type, entity_key, version) do nothing;
