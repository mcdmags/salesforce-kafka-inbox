-- Writes the configured rate limits to the shared sf_rate_limit row, only when they differ from what it holds. Run it
-- after the migrations, once per deploy, with the values of the RateLimit settings:
--
--   psql "$CONNECTION" -v capacity=5 -v rate=2 -f sql/apply-rate-limits.sql
--
--   capacity    RateLimit:BurstCapacity           the bucket's size
--   rate        RateLimit:MaxRequestsPerSecond    the normal refill rate
--
-- The same file runs from `SalesforceSync --migrate` (the AppHost's stand-in for the pipeline), which substitutes the
-- values the same way. It keeps the stored tokens and the current throttle factor (refill_per_sec / base_refill_per_sec).
-- Safe to run again: it updates nothing when nothing changed.
update sf_rate_limit r set
    capacity = :capacity,
    tokens = least(r.tokens, :capacity),
    refill_per_sec = :rate * case when r.base_refill_per_sec > 0 then r.refill_per_sec / r.base_refill_per_sec else 1 end,
    base_refill_per_sec = :rate
where r.name = 'salesforce'
  and (r.capacity, r.base_refill_per_sec)
      is distinct from (:capacity::double precision, :rate::double precision);
