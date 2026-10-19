namespace SalesforceSync.IntegrationTests;

/// <summary>
/// <c>InboxRepository</c>'s statements for one inbox table, copied so a test can play a worker by hand on its own
/// connections (claim without committing, record on a finished entity, ...). Keep them in step with the repository;
/// the tests that run the repository itself catch a copy that has drifted.
/// </summary>
public sealed class InboxStatements
{
    // An outcome lands only while the row still holds the claimed event: if a newer event overwrote it meanwhile,
    // the outcome is for an event that is gone and must not touch the newer one
    private const string StillClaimed = """
        i.entity_id = ANY(@entityIds)
          AND EXISTS (SELECT 1 FROM unnest(@entityIds, @times, @leaseEnds) AS c(entity_id, event_time, lease_end)
                      WHERE c.entity_id = i.entity_id AND c.event_time = i.event_time AND c.lease_end = i.next_attempt_at)
        """;

    private InboxStatements(string table)
    {
        Table = table;
        // Same time overwrites only a row not yet done (a done row is in Salesforce already). coalesce keeps a running
        // lease, so a second worker can't claim the entity while the first is still writing it
        Store = $"""
            INSERT INTO {table} AS i (entity_id, event_time, payload, next_attempt_at)
            VALUES (@entityId, @eventTime, @payload::jsonb, now())
            ON CONFLICT (entity_id) DO UPDATE
            SET payload = excluded.payload, event_time = excluded.event_time, attempt_count = 0,
                last_error = NULL, last_updated = now(),
                next_attempt_at = coalesce(i.next_attempt_at, excluded.next_attempt_at)
            WHERE i.event_time < excluded.event_time
               OR (i.event_time = excluded.event_time AND (i.next_attempt_at IS NOT NULL OR i.last_error IS NOT NULL))
            """;
        // SKIP LOCKED lets workers claim side by side without waiting on each other's rows
        Claim = $"""
            UPDATE {table} i
            SET next_attempt_at = now() + @lease, last_updated = now()
            WHERE i.entity_id = ANY(ARRAY(
              SELECT d.entity_id FROM {table} d
              WHERE d.next_attempt_at <= now()
              ORDER BY d.next_attempt_at
              LIMIT @batchSize
              FOR UPDATE SKIP LOCKED))
            RETURNING i.entity_id AS EntityId, i.payload::text AS Payload, i.event_time AS EventTime, i.next_attempt_at AS LeaseEnd
            """;
        Done = $"""
            UPDATE {table} i
            SET last_updated = now(), last_error = NULL, next_attempt_at = NULL
            WHERE {StillClaimed}
            """;
        Fail = $"""
            UPDATE {table} i
            SET last_updated = now(), attempt_count = i.attempt_count + 1,
                last_error = (SELECT @errors::jsonb) ->> i.entity_id,
                next_attempt_at = CASE WHEN i.attempt_count + 1 < @maxRetries
                                       THEN now() + make_interval(secs => least(@maxDelaySecs, @baseDelaySecs * power(2, i.attempt_count))) END
            WHERE {StillClaimed}
            """;
        EndLease = $"""
            UPDATE {table} i
            SET next_attempt_at = now() + @delay, last_updated = now()
            WHERE i.entity_id = ANY(@entityIds)
              AND EXISTS (SELECT 1 FROM unnest(@entityIds, @leaseEnds) AS c(entity_id, lease_end)
                          WHERE c.entity_id = i.entity_id AND c.lease_end = i.next_attempt_at)
            """;
    }

    public string Table { get; }
    public string Store { get; }
    public string Claim { get; }
    public string Done { get; }
    public string Fail { get; }
    public string EndLease { get; }

    public static InboxStatements For(string table)
    {
        return new(table);
    }
}
