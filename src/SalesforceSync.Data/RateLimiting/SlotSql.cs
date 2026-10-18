namespace SalesforceSync.Data.RateLimiting;

/// <summary>
/// The statements of the concurrency slots, on the one <c>sf_rate_limit</c> row. Its <c>slots</c> column is a JSON array
/// with an element per batch that is calling Salesforce: its token and when its lease ends. The row is the slots' lock:
/// an UPDATE locks it, so two pods never take the last slot together.
/// <para>
/// The condition and the new value read the row's own <c>slots</c>: under READ COMMITTED an UPDATE that waited for the row
/// lock is re-checked against the row's new version, and only a condition that reads the row itself is re-evaluated
/// (a CTE or a separate SELECT would keep the answer of the old version, and let both pods in).
/// </para>
/// </summary>
internal static class SlotSql
{
    /// <summary>
    /// Takes a slot if fewer than @max leases are live: drops the expired leases and the token's own (so a retry of an
    /// acquire whose answer was lost doesn't take a second slot) and adds the token's, ending at now + @lease. One row
    /// updated: the slot is held; none: all slots are busy.
    /// </summary>
    public const string Acquire = """
        UPDATE sf_rate_limit
        SET slots = (SELECT coalesce(jsonb_agg(s), '[]'::jsonb) FROM jsonb_array_elements(slots) AS s
                     WHERE (s->>'until')::timestamptz > now() AND s->>'token' <> @token)
                    || jsonb_build_array(jsonb_build_object('token', @token::text, 'until', now() + @lease))
        WHERE name = 'salesforce'
          AND (SELECT count(*) FROM jsonb_array_elements(slots) AS s
               WHERE (s->>'until')::timestamptz > now() AND s->>'token' <> @token) < @max
        """;

    /// <summary>Gives the slot back: removes the token's lease (and the expired ones).</summary>
    public const string Release = """
        UPDATE sf_rate_limit
        SET slots = (SELECT coalesce(jsonb_agg(s), '[]'::jsonb) FROM jsonb_array_elements(slots) AS s
                     WHERE (s->>'until')::timestamptz > now() AND s->>'token' <> @token)
        WHERE name = 'salesforce'
        """;
}
