-- Event accessors. sf_inbox stores only the raw Kafka message value; an event's entity type, key and order are
-- read from it by these functions, so the JSON paths live here and nowhere else:
--   { "meta": { "time": "2026-10-14T01:02:03.456Z" }, "account": { "externalId": "ACC-000001", ... } }
-- type = the single top-level key besides "meta" ('account'), key = <type>.externalId ('ACC-000001'),
-- version = meta.time in microseconds since the epoch (the order of an entity's events).
-- Every claim query uses exactly these calls, so the planner can match them to the expression indexes below.
-- SQL-standard bodies (Postgres 14+) are bound when the function is created, so they work under the restricted
-- search_path Postgres 17 uses while building an index.

-- The top-level key other than "meta". KafkaInboxConsumer only admits events with exactly one; a single jsonpath
-- expression (rather than a subquery over jsonb_object_keys) can be inlined and is about 4x cheaper per row,
-- which matters because the claim evaluates it for every pending row it considers.
create or replace function sf_event_type(event jsonb) returns text
language sql immutable strict parallel safe
return jsonb_path_query_first(event, '$.keyvalue() ? (@.key != "meta").key') #>> '{}';

create or replace function sf_event_key(event jsonb) returns text
language sql immutable strict parallel safe
return event -> sf_event_type(event) ->> 'externalId';

-- Declared immutable although a text -> timestamptz cast is only stable: the cast depends on the session's TimeZone
-- (only for a time without an offset) and DateStyle (only for ambiguous formats like 01/02/03). KafkaInboxConsumer
-- admits only ISO-8601 times with an explicit offset or Z, for which neither setting matters, so the result never
-- changes and the function can back the indexes below. Do not insert events with any other time format.
create or replace function sf_event_version(event jsonb) returns bigint
language sql immutable strict parallel safe
return (extract(epoch from (event -> 'meta' ->> 'time')::timestamptz) * 1000000)::bigint;

-- A database created when the inbox had entity_type / entity_key / version columns (or the older account_ext_id):
-- keep its rows for inspection as sf_inbox_legacy and create the new table. Its indexes and id sequence keep their
-- names on a rename, so they are renamed too; otherwise "if not exists" would skip creating the new ones.
do $$
declare
  idx text;
begin
  if exists (select 1 from information_schema.columns
             where table_schema = current_schema() and table_name = 'sf_inbox'
               and column_name in ('entity_key', 'account_ext_id')) then
    for idx in select c.relname from pg_index i join pg_class c on c.oid = i.indexrelid
               where i.indrelid = format('%I.sf_inbox', current_schema())::regclass loop
      execute format('alter index %I rename to %I', idx, 'sf_inbox_legacy' || substr(idx, length('sf_inbox') + 1));
    end loop;
    alter sequence if exists sf_inbox_id_seq rename to sf_inbox_legacy_id_seq;
    alter table sf_inbox rename to sf_inbox_legacy;
  end if;
end $$;

-- Inbox of raw Kafka change events (one row per event) plus their processing state. Events are ordered and
-- processed per (sf_event_type, sf_event_key): e.g. ('account', 'ACC-000001') or ('accountRelationship', 'REL-1'),
-- by (sf_event_version, id).
create table if not exists sf_inbox (
  id              bigserial   primary key,
  payload         jsonb       not null,          -- the Kafka message value, unchanged
  status          text        not null default 'pending',  -- pending | done | superseded | dead
  retry_count     int         not null default 0,
  last_updated    timestamptz not null default now(),
  last_error      text,
  last_error_code text,                          -- e.g. FIELD_CUSTOM_VALIDATION_EXCEPTION, to query dead letters by cause
  next_attempt_at timestamptz not null default now(),
  locked_until    timestamptz,                   -- lease while a worker processes the row
  lock_token      uuid,                          -- which worker holds the lease
  received_at     timestamptz not null default now()
);

-- Newest event version synced to Salesforce per entity. Lets a late or replayed older event be recognised as
-- stale without storing anything in Salesforce. Only advanced by the worker holding the entity's lease.
create table if not exists sf_sync_state (
  entity_type     text        not null,
  entity_key      text        not null,
  synced_version  bigint      not null,          -- sf_event_version of the newest synced event
  synced_at       timestamptz not null default now(),
  constraint sf_sync_state_pkey primary key (entity_type, entity_key)
);

create index if not exists sf_inbox_pending_key
  on sf_inbox (sf_event_type(payload), sf_event_key(payload), sf_event_version(payload)) where status = 'pending';
create index if not exists sf_inbox_ready
  on sf_inbox (sf_event_type(payload), next_attempt_at) where status = 'pending';
create index if not exists sf_inbox_leased_key
  on sf_inbox (sf_event_type(payload), sf_event_key(payload)) where locked_until is not null;

-- Idempotent ingest: a Kafka redelivery of the same event is ignored. The hash tells apart two different events
-- for the same entity at the same time, which are both kept (and ordered by id). jsonb::text is canonical, so the
-- same JSON with other whitespace or key order counts as the same event.
create unique index if not exists sf_inbox_event_unique
  on sf_inbox (sf_event_type(payload), sf_event_key(payload), sf_event_version(payload), md5(payload::text));

-- Ingest (Kafka consumer):
-- insert into sf_inbox (payload) values (@payload::jsonb) on conflict do nothing;
