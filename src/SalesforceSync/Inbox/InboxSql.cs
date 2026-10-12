namespace SalesforceSync.Inbox;

internal static class InboxSql
{
    /// <summary>
    /// Locks the head (oldest pending) row of accounts that have nothing in flight.
    /// Locking the head instead of arbitrary rows is what makes processing per-account serial.
    /// </summary>
    public const string LockHeads = """
        select i.account_ext_id
        from sf_inbox i
        where i.status = 'pending'
          and i.next_attempt_at <= now()
          and (i.locked_until is null or i.locked_until < now())
          and not exists (select 1 from sf_inbox o
                          where o.account_ext_id = i.account_ext_id
                            and o.status = 'pending' and o.version < i.version)
          and not exists (select 1 from sf_inbox p
                          where p.account_ext_id = i.account_ext_id
                            and p.locked_until > now())
        order by i.next_attempt_at, i.received_at
        limit @batchSize
        for update of i skip locked
        """;

    /// <summary>Leases every pending row of the claimed accounts; bumps retry_count / last_updated.</summary>
    public const string ClaimAccounts = """
        update sf_inbox
        set lock_token = @token, locked_until = now() + @lease,
            retry_count = retry_count + 1, last_updated = now()
        where status = 'pending' and account_ext_id = any(@accts)
        returning id, account_ext_id as AccountExtId, version, payload::text as Payload, retry_count as RetryCount
        """;

    public const string Complete = """
        update sf_inbox
        set status = @status, locked_until = null, lock_token = null,
            last_updated = now(), last_error = null
        where id = any(@ids) and lock_token = @token
        """;

    public const string Fail = """
        update sf_inbox
        set locked_until = null, lock_token = null, last_updated = now(), last_error = @err,
            next_attempt_at = now() + make_interval(secs => least(3600, 30 * power(2, retry_count - 1))),
            status = case when retry_count >= @maxRetries then 'dead' else 'pending' end
        where id = any(@ids) and lock_token = @token
        """;

    /// <summary>Rate limited: hand back without spending a retry.</summary>
    public const string Defer = """
        update sf_inbox
        set locked_until = null, lock_token = null, last_updated = now(),
            retry_count = greatest(retry_count - 1, 0),
            next_attempt_at = now() + @delay
        where lock_token = @token
        """;
}
