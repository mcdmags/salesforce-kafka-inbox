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

-- >>> inbox table template
-- Applied once per synced object type, with {inbox} replaced by that type's table name (a code constant, e.g.
-- sf_account_inbox, sf_account_relationship_inbox, sf_contact_inbox). Every inbox has the same shape: one row per Kafka event plus its
-- processing state. Rows of one entity_id are claimed together and never by two workers at once. The table is also
-- the sync's watermark: an entity's newest synced event time is the highest synced_time of its done rows, so a late
-- or replayed older event is recognised as stale without storing anything in Salesforce or in a table of its own.
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
  received_at     timestamptz not null default now(),
  synced_time     bigint                         -- done rows only: the time of the event written to Salesforce,
                                                 -- microseconds since the epoch (from C#)
);
alter table {inbox} add column if not exists synced_time bigint;

-- The claim's lookups, all on plain columns (InboxSql.LockHeads). Every statement is written for one of these indexes
-- and runs under InboxSql.PinPlans, so the plans don't change with the statistics:
-- rows that may be claimed now, oldest first. Leased rows have next_attempt_at at the end of their lease, so they are
-- beyond the range the claim scans, and re-enter it if the lease expires;
create index if not exists {inbox}_ready on {inbox} (next_attempt_at, id) where status = 'pending';
-- an entity's pending rows in arrival order: its head, the rows to lease, and the walk over all pending entities;
create index if not exists {inbox}_pending on {inbox} (entity_id, id) where status = 'pending';
-- an entity's leased rows: is it in flight, and does this worker still hold it;
create index if not exists {inbox}_leased on {inbox} (entity_id, locked_until) where locked_until is not null;
-- an entity's watermark: its newest synced_time, one probe from the top (InboxSql.SyncedTimes).
create index if not exists {inbox}_synced on {inbox} (entity_id, synced_time) where status = 'done';
-- A queue table: every event is updated at least twice (leased, then finished), and the claim reads the oldest
-- entries of the ready index, which is where the dead index entries those updates leave behind collect. The default
-- (vacuum once 20% of the table is dead) lets that grow with the table, which keeps every finished row: measured
-- on 200,000 rows, the first claim after 150,000 had finished took 187 ms and the following ones 5 to 7 ms, against
-- 2 ms once vacuumed. Vacuum after 2% (plus 1,000 rows) instead. The plans themselves don't depend on statistics
-- (InboxSql.PinPlans), so analyze keeps its defaults.
alter table {inbox} set (autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_threshold = 1000);
-- <<< inbox table template

-- Upgrade: the watermarks used to be one table for every type, sf_sync_state. Each moves into its object's inbox as
-- a done row with an empty payload (never claimed: it is not pending), unless the inbox already has a done row at
-- least as new. Rows of a type without an inbox here are left, and so is the table until none are.
do $$
declare
  m record;
begin
  if to_regclass(format('%I.sf_sync_state', current_schema())) is null then
    return;
  end if;
  -- its column names before the move to entity_id
  if exists (select 1 from information_schema.columns
             where table_schema = current_schema() and table_name = 'sf_sync_state' and column_name = 'entity_key') then
    alter table sf_sync_state rename column entity_key to entity_id;
    alter table sf_sync_state rename column synced_version to synced_time;
  end if;
  for m in select v.entity_type, v.inbox
           from (values ('account', 'sf_account_inbox'), ('accountRelationship', 'sf_account_relationship_inbox'),
                        ('contact', 'sf_contact_inbox')) v(entity_type, inbox)
           where to_regclass(format('%I.%I', current_schema(), v.inbox)) is not null loop
    execute format($sql$
      insert into %1$I (entity_id, payload, status, synced_time)
      select s.entity_id, jsonb_build_object(), 'done', s.synced_time from sf_sync_state s
      where s.entity_type = $1
        and not exists (select 1 from %1$I d
                        where d.entity_id = s.entity_id and d.status = 'done' and d.synced_time >= s.synced_time)
      $sql$, m.inbox) using m.entity_type;
    delete from sf_sync_state where entity_type = m.entity_type;
  end loop;
  if not exists (select 1 from sf_sync_state) then
    drop table sf_sync_state;
  end if;
end $$;
