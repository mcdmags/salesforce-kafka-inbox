-- Inbox of raw Kafka change events (one row per event)
create table if not exists sf_inbox (
  id              bigserial   primary key,
  account_ext_id  text        not null,
  version         bigint      not null,          -- monotonic per account, from the source
  payload         jsonb       not null,
  status          text        not null default 'pending',  -- pending | done | superseded | dead
  retry_count     int         not null default 0,
  last_updated    timestamptz not null default now(),
  last_error      text,
  next_attempt_at timestamptz not null default now(),
  locked_until    timestamptz,                   -- lease while a worker processes the row
  lock_token      uuid,                          -- which worker holds the lease
  received_at     timestamptz not null default now(),
  unique (account_ext_id, version)               -- idempotent ingest
);

create index if not exists sf_inbox_pending_head on sf_inbox (account_ext_id, version) where status = 'pending';
create index if not exists sf_inbox_ready        on sf_inbox (next_attempt_at)        where status = 'pending';
create index if not exists sf_inbox_leased       on sf_inbox (account_ext_id)         where locked_until is not null;

-- Newest event version synced to Salesforce per account. Lets a late or replayed older event be recognised as
-- stale without storing anything in Salesforce. Only advanced by the worker holding the account's lease.
create table if not exists sf_sync_state (
  account_ext_id  text        primary key,
  synced_version  bigint      not null,
  synced_at       timestamptz not null default now()
);

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
  usage_updated_at     timestamptz
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

-- What Salesforce last reported (handy for dashboards / alerts)
create or replace view sf_limit_status as
select api_used, api_max, round(100.0 * api_used / nullif(api_max, 0), 1)     as api_pct,
       burst_used, burst_max, round(100.0 * burst_used / nullif(burst_max, 0), 1) as burst_pct,
       app_name, app_used, app_max, round(100.0 * app_used / nullif(app_max, 0), 1) as app_pct,
       refill_per_sec, base_refill_per_sec, paused_until, usage_updated_at
from sf_rate_limit
where name = 'salesforce';

insert into sf_rate_limit (name, tokens, capacity, base_refill_per_sec, refill_per_sec)
values ('salesforce', 5, 5, 2, 2)
on conflict (name) do nothing;

-- Ingest (Kafka consumer):
-- insert into sf_inbox (account_ext_id, version, payload)
-- values (@acct, @version, @payload::jsonb)
-- on conflict (account_ext_id, version) do nothing;
