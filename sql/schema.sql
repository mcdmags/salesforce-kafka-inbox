-- The sync's Postgres schema, applied at worker startup by InboxSchema (idempotent, under an advisory lock).
-- No statement here or in InboxSql reads inside an event: the payload is opaque jsonb, and everything the SQL needs
-- (which entity a row belongs to, the time of the event it synced) is a plain column filled by the worker from its
-- typed event classes.

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

-- Every synced entity's sync state, one row per (entity type, entity id) for all object types: whether it has events
-- to process and when (next_attempt_at), which worker holds it (the lease), and its watermark. Claiming an entity is
-- locking and leasing its row here, so two workers can never hold one entity: they would lock the same row. The
-- inboxes hold the events only.
create table if not exists sf_sync_state (
  entity_type     text        not null,   -- the strategy's type name: 'account', 'accountRelationship', 'contact'
  entity_id       text        not null,   -- the Kafka message key, as in the inbox
  next_attempt_at timestamptz,            -- null: nothing pending; else claimable from then on (backoff included)
  locked_until    timestamptz,            -- lease while a worker processes the entity
  lock_token      uuid,                   -- which worker holds the lease
  synced_time     bigint,                 -- watermark: the time of the newest event written to Salesforce,
                                          -- microseconds since the epoch (from C#)
  primary key (entity_type, entity_id)
);
-- Upgrade: the first sf_sync_state held only watermarks (entity_type, entity_key, synced_version, synced_at); its rows
-- are still right, so the table gets the new columns. (A later version moved those watermarks into the inboxes as done
-- rows; they come back from there below.)
do $$
begin
  if exists (select 1 from information_schema.columns
             where table_schema = current_schema() and table_name = 'sf_sync_state' and column_name = 'entity_key') then
    alter table sf_sync_state rename column entity_key to entity_id;
    alter table sf_sync_state rename column synced_version to synced_time;
  end if;
end $$;
alter table sf_sync_state add column if not exists next_attempt_at timestamptz;
alter table sf_sync_state add column if not exists locked_until timestamptz;
alter table sf_sync_state add column if not exists lock_token uuid;
alter table sf_sync_state alter column synced_time drop not null;
alter table sf_sync_state drop column if exists synced_at;
-- The claim (InboxSql.ClaimEntities): an entity type's due entities, the longest-waiting first. Idle entities (null)
-- are not in the index.
create index if not exists sf_sync_state_ready on sf_sync_state (entity_type, next_attempt_at) where next_attempt_at is not null;
-- Hot: every stored event, claim and outcome updates a row here, and the claim reads the front of the ready index,
-- where the dead index entries those updates leave behind collect. Vacuum after 2% (plus 1,000 rows) instead of the
-- default 20%, as for the inboxes.
alter table sf_sync_state set (autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_threshold = 1000);

-- >>> inbox table template
-- Applied once per synced object type, with {inbox} replaced by that type's table name (a code constant, e.g.
-- sf_account_inbox, sf_account_relationship_inbox, sf_contact_inbox). Every inbox has the same shape: one row per
-- Kafka event and what became of it. Which entities are due, the lease and the watermark are in sf_sync_state.
create table if not exists {inbox} (
  id              bigserial   primary key,
  entity_id       text        not null,          -- the Kafka message key (the entity's id); set by the consumer
  payload         jsonb       not null,          -- the Kafka message value, unchanged; never read by SQL
  status          text        not null default 'pending',  -- pending | done | superseded | dead
  attempt_count   int         not null default 0,          -- times this event was claimed
  last_error      text,
  last_error_code text,                          -- e.g. FIELD_CUSTOM_VALIDATION_EXCEPTION, to query dead letters by cause
  received_at     timestamptz not null default now(),
  last_updated    timestamptz not null default now()
);
-- An entity's pending events in arrival order: the events a claim takes (InboxSql.ClaimEvents), and whether any are left
-- when its lease ends.
create index if not exists {inbox}_pending on {inbox} (entity_id, id) where status = 'pending';
-- Every event is updated at least twice (claimed, then finished), and the claim reads the front of an entity's
-- pending entries, where the dead index entries those updates leave behind collect. The default (vacuum once 20% of
-- the table is dead) lets that grow with the table, which keeps every finished row. Vacuum after 2% (plus 1,000 rows).
alter table {inbox} set (autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_threshold = 1000);
-- <<< inbox table template

-- Upgrade: the inboxes used to hold the claim state themselves (next_attempt_at, the lease, retry_count) and the
-- watermark (synced_time of done rows). Per inbox of that shape: its entities' state into sf_sync_state (the newest
-- synced time of the done rows, merged with a watermark already there; due when the first pending event was; no
-- lease, so a lease of a worker from before the upgrade is not honoured), retry_count into attempt_count, then the
-- old columns go (with the indexes on them: _ready, _leased, _synced).
do $$
declare
  m record;
begin
  for m in select v.entity_type, v.inbox
           from (values ('account', 'sf_account_inbox'), ('accountRelationship', 'sf_account_relationship_inbox'),
                        ('contact', 'sf_contact_inbox')) v(entity_type, inbox)
           where exists (select 1 from information_schema.columns
                         where table_schema = current_schema() and table_name = v.inbox and column_name = 'retry_count') loop
    execute format($sql$
      insert into sf_sync_state as s (entity_type, entity_id, synced_time, next_attempt_at)
      select $1, i.entity_id, max(i.synced_time) filter (where i.status = 'done'),
             min(i.next_attempt_at) filter (where i.status = 'pending')
      from %1$I i
      group by i.entity_id
      having bool_or(i.status = 'pending') or max(i.synced_time) filter (where i.status = 'done') is not null
      on conflict (entity_type, entity_id) do update
        set synced_time = greatest(s.synced_time, excluded.synced_time),
            next_attempt_at = least(s.next_attempt_at, excluded.next_attempt_at)
      $sql$, m.inbox) using m.entity_type;
    execute format('alter table %I add column if not exists attempt_count int not null default 0', m.inbox);
    execute format('update %I set attempt_count = retry_count', m.inbox);
    execute format('alter table %I drop column retry_count, drop column next_attempt_at, drop column locked_until,
                    drop column lock_token, drop column synced_time', m.inbox);
  end loop;
end $$;
