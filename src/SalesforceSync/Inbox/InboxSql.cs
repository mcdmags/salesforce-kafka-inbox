namespace SalesforceSync.Inbox;

/// <summary>
/// Inbox statements, for any entity type. Rows hold only the raw event (payload); its entity type, key and version
/// are read with sf_event_type / sf_event_key / sf_event_version (sql/schema.sql), always written exactly as in the
/// expression indexes so the planner uses them. Rows are ordered per (type, key) by (version, id) and leased per entity.
/// Every statement is safe to run again after an unknown commit (see PostgresExecutionStrategy): updates are
/// conditional on the lease token, the watermark only moves forward, ingest ignores duplicates.
/// Outcome statements take arrays, so recording a batch costs a fixed number of round trips, not two per entity.
/// </summary>
internal static class InboxSql
{
    /// <summary>
    /// Locks the head (oldest pending) row of entities of one type that have nothing in flight.
    /// Locking the head instead of arbitrary rows is what makes processing per entity serial. Events of an entity
    /// with the same time are ordered by id, so every entity has exactly one head.
    /// </summary>
    public const string LockHeads = """
        select sf_event_key(i.payload)
        from sf_inbox i
        where sf_event_type(i.payload) = @entityType
          and i.status = 'pending'
          and i.next_attempt_at <= now()
          and (i.locked_until is null or i.locked_until < now())
          and not exists (select 1 from sf_inbox o
                          where sf_event_type(o.payload) = @entityType and sf_event_key(o.payload) = sf_event_key(i.payload)
                            and o.status = 'pending'
                            and (sf_event_version(o.payload), o.id) < (sf_event_version(i.payload), i.id))
          and not exists (select 1 from sf_inbox p
                          where sf_event_type(p.payload) = @entityType and sf_event_key(p.payload) = sf_event_key(i.payload)
                            and p.locked_until > now())
        order by i.next_attempt_at, i.received_at
        limit @batchSize
        for update of i skip locked
        """;

    /// <summary>Leases every pending row of the claimed entities; bumps retry_count / last_updated.</summary>
    public const string ClaimKeys = """
        update sf_inbox
        set lock_token = @token, locked_until = now() + @lease,
            retry_count = retry_count + 1, last_updated = now()
        where sf_event_type(payload) = @entityType and status = 'pending' and sf_event_key(payload) = any(@keys)
        returning id, sf_event_key(payload) as Key, sf_event_version(payload) as Version, payload::text as Payload,
                  retry_count as RetryCount
        """;

    /// <summary>
    /// Undoes ClaimKeys of earlier attempts of a retried claim whose commit outcome was unknown: no worker knows those
    /// tokens, so their rows would otherwise sit leased, unprocessed, until the lease expires. Restores retry_count.
    /// </summary>
    public const string ReleaseLostClaims = """
        update sf_inbox
        set locked_until = null, lock_token = null, last_updated = now(),
            retry_count = greatest(retry_count - 1, 0)
        where sf_event_type(payload) = @entityType and status = 'pending' and lock_token = any(@tokens)
        """;

    public const string SyncedVersions = """
        select entity_key as Key, synced_version as Version
        from sf_sync_state where entity_type = @entityType and entity_key = any(@keys)
        """;

    /// <summary>
    /// Advances the watermarks of entities (@keys[i] to @versions[i]), but only while this worker still holds the
    /// lease on the entity's rows, so a worker whose lease expired cannot move them. A row with a token always has
    /// locked_until set (both are set and cleared together); saying so lets the lookup use the leased index.
    /// </summary>
    public const string MarkSynced = """
        insert into sf_sync_state (entity_type, entity_key, synced_version)
        select @entityType, s.entity_key, s.version
        from unnest(@keys, @versions) as s(entity_key, version)
        where exists (select 1 from sf_inbox
                      where sf_event_type(payload) = @entityType and sf_event_key(payload) = s.entity_key
                        and locked_until is not null and lock_token = @token)
        on conflict (entity_type, entity_key) do update
        set synced_version = greatest(sf_sync_state.synced_version, excluded.synced_version), synced_at = now()
        """;

    public const string Complete = """
        update sf_inbox
        set status = @status, locked_until = null, lock_token = null,
            last_updated = now(), last_error = null, last_error_code = null
        where id = any(@ids) and lock_token = @token
        """;

    /// <summary>
    /// Transient failure: retry with exponential backoff; dead-letter once retries are used up.
    /// Row @ids[i] gets error @errs[i] / @codes[i], so a whole batch's failures are one statement.
    /// </summary>
    public const string Fail = """
        update sf_inbox i
        set locked_until = null, lock_token = null, last_updated = now(), last_error = f.err, last_error_code = f.code,
            next_attempt_at = now() + make_interval(secs => least(@maxDelaySecs, @baseDelaySecs * power(2, i.retry_count - 1))),
            status = case when i.retry_count >= @maxRetries then 'dead' else 'pending' end
        from unnest(@ids, @errs, @codes) as f(id, err, code)
        where i.id = f.id and i.lock_token = @token
        """;

    /// <summary>Permanent failure: retrying the same data can't succeed, dead-letter now. Arrays as in <see cref="Fail"/>.</summary>
    public const string Dead = """
        update sf_inbox i
        set locked_until = null, lock_token = null, last_updated = now(), last_error = f.err, last_error_code = f.code,
            status = 'dead'
        from unnest(@ids, @errs, @codes) as f(id, err, code)
        where i.id = f.id and i.lock_token = @token
        """;

    /// <summary>
    /// Rate limited (all claimed rows): hand back without spending a retry. Filtered by id like the others, since
    /// lock_token has no index.
    /// </summary>
    public const string Defer = """
        update sf_inbox
        set locked_until = null, lock_token = null, last_updated = now(),
            retry_count = greatest(retry_count - 1, 0),
            next_attempt_at = now() + @delay
        where id = any(@ids) and lock_token = @token
        """;

    /// <summary>Not attempted (graph halted / rolled back by others): hand back for the next claim, no retry spent.</summary>
    public const string Release = """
        update sf_inbox
        set locked_until = null, lock_token = null, last_updated = now(),
            retry_count = greatest(retry_count - 1, 0)
        where id = any(@ids) and lock_token = @token
        """;

    /// <summary>The raw event. A redelivery hits the unique (type, key, version, md5) index and is ignored.</summary>
    public const string Insert = """
        insert into sf_inbox (payload) values (@payload::jsonb)
        on conflict do nothing
        """;
}
