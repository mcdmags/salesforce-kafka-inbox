namespace SalesforceSync.Salesforce;

public sealed class SalesforceOptions
{
    public const string Section = "Salesforce";

    /// <summary>Salesforce stops REST calls (a composite request as a whole) after this long.</summary>
    public static readonly TimeSpan MaxApiTimeout = TimeSpan.FromMinutes(10);

    public string InstanceUrl { get; set; } = "";
    public string TokenUrl { get; set; } = "";
    public string ClientId { get; set; } = "";
    public string ClientSecret { get; set; } = "";

    /// <summary>
    /// How this process names itself in the User-Agent, which Salesforce API logs (and the mock org's per-client
    /// counters) show. Unset = machine name (the pod name in Kubernetes) and process id.
    /// </summary>
    public string? UserAgentInstance { get; set; }

    /// <summary>
    /// Client timeout per Salesforce call. Salesforce stops REST calls (a composite request as a whole) after
    /// <see cref="MaxApiTimeout"/>, so this must not exceed that, and it must be shorter than the sync's lease.
    /// </summary>
    public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(90);

    /// <summary>Salesforce does not return expires_in for client credentials; refresh proactively.</summary>
    public TimeSpan TokenLifetime { get; set; } = TimeSpan.FromMinutes(30);

    /// <summary>
    /// How long a cached sObject Describe is trusted before it is revalidated with If-Modified-Since
    /// (a 304 Not Modified is cheap, but still one API call).
    /// </summary>
    public TimeSpan SchemaRefreshInterval { get; set; } = TimeSpan.FromMinutes(15);

    /// <summary>Request bodies at least this large are sent gzip-compressed (0 = never).</summary>
    public int CompressRequestsLargerThan { get; set; } = 1024;

    /// <summary>
    /// Salesforce error codes that won't succeed on retry; the entity's events are dead-lettered immediately.
    /// Unset = <see cref="SalesforceErrors.DefaultPermanentCodes"/>; setting it replaces the list.
    /// Codes not listed (and not <see cref="SalesforceErrors.ProcessingHalted"/>) are treated as transient and retried with backoff.
    /// </summary>
    public string[]? PermanentErrorCodes { get; set; }
}