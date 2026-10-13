namespace SalesforceSync.Salesforce;

public sealed class SalesforceOptions
{
    public const string Section = "Salesforce";

    public string InstanceUrl { get; set; } = "";
    public string TokenUrl { get; set; } = "";
    public string ClientId { get; set; } = "";
    public string ClientSecret { get; set; } = "";

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
    /// Salesforce error codes that won't succeed on retry; the account's events are dead-lettered immediately.
    /// Unset = <see cref="SalesforceErrors.DefaultPermanentCodes"/>; setting it replaces the list.
    /// Codes not listed (and not "not attempted") are treated as transient and retried with backoff.
    /// </summary>
    public string[]? PermanentErrorCodes { get; set; }
}
