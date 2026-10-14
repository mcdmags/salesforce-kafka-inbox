namespace SalesforceSync.RateLimiting;

/// <summary>The statements of <see cref="SalesforceRateLimiter"/> on the shared <c>sf_rate_limit</c> row and <c>sf_api_usage</c>.</summary>
internal static class RateLimitSql
{
    public const string Lock = "select 1 from sf_rate_limit where name = 'salesforce' for update";

    // Runs after Lock in the same transaction: the row lock serializes every acquire in every process, so the
    // budget check and the usage increment are exact. The window includes the whole 10-minute bucket that straddles "24 hours ago",
    // so usage is over- rather than under-counted.
    public const string Acquire = """
        with s as (
          select name, refill_per_sec, paused_until, daily_budget,
                 least(capacity, tokens + extract(epoch from clock_timestamp() - updated_at)::float8 * refill_per_sec) as available
          from sf_rate_limit
          where name = 'salesforce'
          for update
        ), w as (
          select coalesce(sum(calls), 0)::float8 as used, min(bucket) as oldest
          from sf_api_usage
          where bucket >= clock_timestamp() - interval '24 hours 10 minutes'
        ), u as (
          update sf_rate_limit r
          set tokens = s.available - @n, updated_at = clock_timestamp()
          from s, w
          where r.name = s.name
            and (s.paused_until is null or s.paused_until <= clock_timestamp())
            and s.available >= @n
            and (s.daily_budget is null or w.used + @n <= s.daily_budget)
          returning 1
        ), counted as (
          insert into sf_api_usage (bucket, calls)
          select to_timestamp(floor(extract(epoch from clock_timestamp()) / 600) * 600), @n
          where exists (select 1 from u)
          on conflict (bucket) do update set calls = sf_api_usage.calls + excluded.calls
        )
        select exists (select 1 from u) as granted,
               (case when s.paused_until > clock_timestamp()
                     then extract(epoch from s.paused_until - clock_timestamp())
                     when s.daily_budget is not null and w.used + @n > s.daily_budget
                     then greatest(60, extract(epoch from w.oldest + interval '24 hours 10 minutes' - clock_timestamp()))
                     else greatest(0, (@n - s.available) / s.refill_per_sec) end)::float8 as wait_seconds,
               w.used + (case when exists (select 1 from u) then @n else 0 end) as used_24h,
               s.daily_budget::float8 as daily_budget
        from s, w
        """;

    // Keeps the current throttle factor (refill_per_sec / base_refill_per_sec) under the new base rate.
    public const string ApplyConfiguration = """
        update sf_rate_limit
        set tokens              = least(@capacity, tokens + extract(epoch from clock_timestamp() - updated_at)::float8 * refill_per_sec),
            updated_at          = clock_timestamp(),
            capacity            = @capacity,
            refill_per_sec      = @rate * coalesce(refill_per_sec / nullif(base_refill_per_sec, 0), 1),
            base_refill_per_sec = @rate,
            daily_budget        = @budget
        where name = 'salesforce';
        delete from sf_api_usage where bucket < clock_timestamp() - interval '25 hours';
        """;

    public const string IsPaused = """
        select coalesce(paused_until > clock_timestamp(), false)
        from sf_rate_limit where name = 'salesforce'
        """;

    public const string Pause = """
        update sf_rate_limit
        set paused_until = greatest(coalesce(paused_until, clock_timestamp()), clock_timestamp() + @duration)
        where name = 'salesforce'
        """;

    // Refill tokens accrued at the old rate before changing the rate, so the switch is exact.
    public const string ReportLimits = """
        update sf_rate_limit
        set tokens          = least(capacity, tokens + extract(epoch from clock_timestamp() - updated_at)::float8 * refill_per_sec),
            updated_at      = clock_timestamp(),
            refill_per_sec  = greatest(base_refill_per_sec * @speed, 0.01),
            paused_until    = case when @pauseSeconds > 0
                                   then greatest(coalesce(paused_until, clock_timestamp()),
                                                 clock_timestamp() + make_interval(secs => @pauseSeconds))
                                   else paused_until end,
            api_used        = coalesce(@apiUsed, api_used),
            api_max         = coalesce(@apiMax, api_max),
            burst_used      = coalesce(@burstUsed, burst_used),
            burst_max       = coalesce(@burstMax, burst_max),
            app_used        = coalesce(@appUsed, app_used),
            app_max         = coalesce(@appMax, app_max),
            app_name        = coalesce(@appName, app_name),
            usage_updated_at = clock_timestamp()
        where name = 'salesforce';
        delete from sf_api_usage where bucket < clock_timestamp() - interval '25 hours';
        """;
}
