namespace SalesforceSync.Infrastructure.Salesforce;

public sealed class SalesforceOptions
{
    public const string Section = "Salesforce";

    public string InstanceUrl { get; set; } = "";

    // BaseApiPath and ApiVersion have no default on purpose: appsettings.json is their only source, so the called path
    // is visible and changed in one place, and a deployment missing them fails at startup instead of silently calling
    // a version nobody chose.

    /// <summary>The REST API's base path on the instance, before the version, e.g. <c>/services/data</c>. Required.</summary>
    public string BaseApiPath { get; set; } = "";

    /// <summary>The REST API version every call is made against, e.g. <c>v62.0</c>. Required.</summary>
    public string ApiVersion { get; set; } = "";

    /// <summary>The path every REST call starts with: <c>{BaseApiPath}/{ApiVersion}</c>, e.g. <c>/services/data/v62.0</c>.</summary>
    /// <exception cref="InvalidOperationException">Either setting is missing.</exception>
    public string DataPath => string.IsNullOrWhiteSpace(BaseApiPath) || string.IsNullOrWhiteSpace(ApiVersion)
        ? throw new InvalidOperationException($"{Section}:BaseApiPath and {Section}:ApiVersion must be set (appsettings.json)")
        : $"/{BaseApiPath.Trim('/')}/{ApiVersion.Trim('/')}";

    /// <summary>
    /// The OAuth token endpoint. Unset = <c>{InstanceUrl}/services/oauth2/token</c> (the org's My Domain, and the mock
    /// org's). Set it only when the org issues tokens elsewhere, e.g. another My Domain host or
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
    /// How long the record type Ids read from the org are trusted before they are read again (one SOQL query per
    /// object that has record types).
    /// </summary>
    public TimeSpan RecordTypeRefreshInterval { get; set; } = TimeSpan.FromMinutes(15);

    /// <summary>Request bodies at least this large are sent gzip-compressed (0 = never).</summary>
    public int CompressRequestsLargerThan { get; set; } = 1024;

    /// <summary>
    /// Salesforce error codes that won't succeed on retry; the entity's event is dead-lettered immediately.
    /// Unset = <see cref="SalesforceErrors.DefaultPermanentCodes"/>; setting it replaces the list.
    /// Codes not listed are treated as transient and retried with backoff.
    /// </summary>
    public string[]? PermanentErrorCodes { get; set; }

    /// <summary>
    /// How the org says a record's parent is missing when a Text foreign key names the parent
    /// (a parent in a Rec): the org's own automation rejects such a child (a validation rule,
    /// a trigger's addError or a flow's Custom Error: FIELD_CUSTOM_VALIDATION_EXCEPTION with the org's message; an
    /// unhandled trigger exception: CANNOT_INSERT_UPDATE_ACTIVATE_ENTITY). A record error whose message contains one of
    /// these (case-insensitive) is <see cref="SalesforceErrors.DependencyNotSynced"/>, whatever its code: retried like
    /// a parent the read didn't find, not dead-lettered. Empty (default) = no message means that.
    /// </summary>
    public string[] ParentMissingMessages { get; set; } = [];
}