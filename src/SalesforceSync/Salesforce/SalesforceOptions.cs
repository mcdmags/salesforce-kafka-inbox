namespace SalesforceSync.Salesforce;

public sealed class SalesforceOptions
{
    public const string Section = "Salesforce";

    public string InstanceUrl { get; set; } = "";

    /// <summary>
    /// The OAuth token endpoint. Unset = <c>{InstanceUrl}/services/oauth2/token</c> (the org's My Domain, and the mock
    /// org's); set it only when the org issues tokens elsewhere, e.g. another My Domain host or
    /// <c>https://login.salesforce.com/services/oauth2/token</c> (<c>test.salesforce.com</c> for a sandbox).
    /// </summary>
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
    /// Salesforce error codes that won't succeed on retry; the entity's event is dead-lettered immediately.
    /// Unset = <see cref="SalesforceErrors.DefaultPermanentCodes"/>; setting it replaces the list.
    /// Codes not listed (and not <see cref="SalesforceErrors.ProcessingHalted"/>) are treated as transient and retried with backoff.
    /// </summary>
    public string[]? PermanentErrorCodes { get; set; }

    /// <summary>
    /// How the org says a record's parent is missing when the parent is named by a Text foreign key
    /// (<see cref="SObjectForeignKeyAttribute"/>): the org's own automation rejects such a child (a validation rule,
    /// a trigger's addError or a flow's Custom Error: FIELD_CUSTOM_VALIDATION_EXCEPTION with the org's message; an
    /// unhandled trigger exception: CANNOT_INSERT_UPDATE_ACTIVATE_ENTITY). A record error whose message contains one of
    /// these (case-insensitive) is <see cref="SalesforceErrors.DependencyNotSynced"/>, whatever its code: retried like
    /// a parent the read didn't find, not dead-lettered. Empty (the default) = no message means that.
    /// </summary>
    public string[] ParentMissingMessages { get; set; } = [];
}