namespace SalesforceSync.Data;

public sealed class PostgresRetryOptions
{
    public const string Section = "Postgres:Retry";

    /// <summary>Retries after the first attempt (0 = no retries).</summary>
    public int MaxRetryCount { get; set; } = 5;

    /// <summary>Delay before the first retry; doubles per retry (with jitter) up to <see cref="MaxRetryDelay"/>.</summary>
    public TimeSpan BaseDelay { get; set; } = TimeSpan.FromMilliseconds(100);

    public TimeSpan MaxRetryDelay { get; set; } = TimeSpan.FromSeconds(5);
}
