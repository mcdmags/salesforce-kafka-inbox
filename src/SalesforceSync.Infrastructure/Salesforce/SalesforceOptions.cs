namespace SalesforceSync.Infrastructure.Salesforce;

public sealed class SalesforceOptions
{
    public const string Section = "Salesforce";

    public string InstanceUrl { get; set; } = "";

    // BaseApiPath, ApiVersion: no default on purpose. appsettings.json only source → called path visible, changed in
    // one place; deployment missing them fails at startup, not silently calling a version nobody chose.

    /// <summary>REST API base path on instance, before version, e.g. <c>/services/data</c>. Required.</summary>
    public string BaseApiPath { get; set; } = "";

    /// <summary>REST API version for every call, e.g. <c>v62.0</c>. Required.</summary>
    public string ApiVersion { get; set; } = "";

    /// <summary>Path prefix of every REST call: <c>{BaseApiPath}/{ApiVersion}</c>, e.g. <c>/services/data/v62.0</c>.</summary>
    /// <exception cref="InvalidOperationException">Either setting missing.</exception>
    public string DataPath => string.IsNullOrWhiteSpace(BaseApiPath) || string.IsNullOrWhiteSpace(ApiVersion)
        ? throw new InvalidOperationException($"{Section}:BaseApiPath and {Section}:ApiVersion must be set (appsettings.json)")
        : $"/{BaseApiPath.Trim('/')}/{ApiVersion.Trim('/')}";

    /// <summary>
    /// OAuth token endpoint. Unset = <c>{InstanceUrl}/services/oauth2/token</c> (org's My Domain, and mock org's).
    /// Set only when org issues tokens elsewhere, e.g. another My Domain host or
    /// <c>https://login.salesforce.com/services/oauth2/token</c> (<c>test.salesforce.com</c> for sandbox).
    /// </summary>
    public string TokenUrl { get; set; } = "";
    public string ClientId { get; set; } = "";
    public string ClientSecret { get; set; } = "";

    /// <summary>
    /// Process name in User-Agent, shown in Salesforce API logs (and mock org's per-client counters). Unset = machine
    /// name (pod name in Kubernetes) + process id.
    /// </summary>
    public string? UserAgentInstance { get; set; }

    /// <summary>Salesforce does not return expires_in for client credentials; refresh proactively.</summary>
    public TimeSpan TokenLifetime { get; set; } = TimeSpan.FromMinutes(30);

    /// <summary>
    /// How long record type Ids read from org are trusted before re-read (one SOQL query per object with record
    /// types).
    /// </summary>
    public TimeSpan RecordTypeRefreshInterval { get; set; } = TimeSpan.FromMinutes(15);

    /// <summary>Request bodies ≥ this size are gzipped (0 = never).</summary>
    public int CompressRequestsLargerThan { get; set; } = 1024;

    /// <summary>
    /// Salesforce error codes that never succeed on retry → entity's event dead-lettered at once.
    /// Unset = <see cref="SalesforceErrors.DefaultPermanentCodes"/>; setting replaces the list.
    /// Unlisted codes = transient, retried with backoff.
    /// </summary>
    public string[]? PermanentErrorCodes { get; set; }

    /// <summary>
    /// How org says a record's parent is missing when a Text foreign key names it: org's own
    /// automation rejects the child (validation rule, trigger addError or flow Custom Error:
    /// FIELD_CUSTOM_VALIDATION_EXCEPTION with org's message; unhandled trigger exception:
    /// CANNOT_INSERT_UPDATE_ACTIVATE_ENTITY). Record error whose message contains one of these (case-insensitive) is
    /// <see cref="SalesforceErrors.DependencyNotSynced"/>, whatever its code: retried like a parent the read didn't
    /// find, not dead-lettered. Empty (default) = no message means that.
    /// </summary>
    public string[] ParentMissingMessages { get; set; } = [];
}