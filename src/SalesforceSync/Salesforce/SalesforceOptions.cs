namespace SalesforceSync.Salesforce;

public sealed class SalesforceOptions
{
    public const string Section = "Salesforce";

    public string InstanceUrl { get; set; } = "";
    public string TokenUrl { get; set; } = "";
    public string ClientId { get; set; } = "";
    public string ClientSecret { get; set; } = "";

    /// <summary>
    /// How this process names itself in the User-Agent, which Salesforce API logs (and the mock org's per-client
    /// counters) show. Unset = machine name (the pod name in Kubernetes) and process id.
    /// </summary>
    public string? UserAgentInstance { get; set; }

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
    /// Codes not listed (and not "not attempted") are treated as transient and retried with backoff.
    /// </summary>
    public string[]? PermanentErrorCodes { get; set; }

    /// <summary>
    /// Sent as <c>allOrNone</c> on every write (the Composite request and each sObject Collections subrequest).
    /// False (default): every record is saved or rejected on its own, so one bad record costs only its own entity.
    /// True: any failing record rolls back the whole request; the entities that were fine are retried at once, the
    /// failing one is dead-lettered or backs off, so a batch with a bad record takes an extra round trip.
    /// </summary>
    public bool AllOrNone { get; set; }

    /// <summary>How a child is linked to a parent that is created in the same request.</summary>
    public ParentLink ParentLink { get; set; } = ParentLink.ReferenceId;
}

public enum ParentLink
{
    /// <summary>
    /// The lookup is a Composite reference to the parent's create result, <c>"@{Account_create[3].id}"</c>. One failed
    /// parent leaves its reference without a value, which fails the whole subrequest of children that use references.
    /// </summary>
    ReferenceId,

    /// <summary>
    /// The lookup is given through the parent's external id, <c>"Account__r": { "External_Id__c": "ACC-1" }</c>, which
    /// Salesforce resolves per record: a failed parent fails only its own child.
    /// </summary>
    ExternalId
}