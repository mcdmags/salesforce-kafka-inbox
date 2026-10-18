-- Writes the configured rate limits to the shared sf_rate_limit row, only when they differ from what it holds. Run it
-- after the migrations, once per deploy, with the values of the RateLimit and SalesforceLimits settings:
--
--   psql "$CONNECTION" -v capacity=5 -v rate=2 -v allocation=115000 -v share=0.5 -f sql/apply-rate-limits.sql
--
--   capacity    RateLimit:BurstCapacity           the bucket's size
--   rate        RateLimit:MaxRequestsPerSecond    the normal refill rate
--   allocation  SalesforceLimits:DailyApiAllocation
--   share       RateLimit:DailyBudgetShare
--
-- The same file runs from `SalesforceSync --migrate` (the AppHost's stand-in for the pipeline), which substitutes the
-- values the same way. It keeps the stored tokens and the current throttle factor (refill_per_sec / base_refill_per_sec),
-- and caps the budget at the allocation Salesforce last reported (api_max). Safe to run again: it updates nothing when
-- nothing changed.
with wanted as (
    select name, floor(least(:allocation, coalesce(nullif(api_max, 0), :allocation)) * :share)::bigint as budget
    from sf_rate_limit
    where name = 'salesforce'
)
update sf_rate_limit r set
    capacity = :capacity,
    tokens = least(r.tokens, :capacity),
    refill_per_sec = :rate * case when r.base_refill_per_sec > 0 then r.refill_per_sec / r.base_refill_per_sec else 1 end,
    base_refill_per_sec = :rate,
    daily_budget = w.budget
from wanted w
where r.name = w.name
  and (r.capacity, r.base_refill_per_sec, r.daily_budget)
      is distinct from (:capacity::double precision, :rate::double precision, w.budget);
