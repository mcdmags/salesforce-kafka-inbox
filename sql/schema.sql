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

-- Ingest (Kafka consumer):
-- insert into sf_inbox (entity_type, entity_key, version, payload)
-- values (@type, @key, @version, @payload::jsonb)
-- on conflict (entity_type, entity_key, version) do nothing;
