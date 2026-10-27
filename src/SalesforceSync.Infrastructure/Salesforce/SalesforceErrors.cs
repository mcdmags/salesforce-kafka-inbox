using System.Net;
using Microsoft.Extensions.Options;

namespace SalesforceSync.Infrastructure.Salesforce;

/// <summary>Decides what a failed record's errors mean: retry now, retry later, or give up.</summary>
public sealed class SalesforceErrors(IOptions<SalesforceOptions> options)
{
    /// <summary>
    /// Our own code (not Salesforce's): a foreign key's parent, e.g. the Account of a relationship,
    /// isn't in Salesforce yet. Transient: it is expected to arrive through its own sync.
    /// </summary>
    public const string DependencyNotSynced = "DEPENDENCY_NOT_SYNCED";

    /// <summary>
    /// Our own code: the record's record type is not among the object's active record types, or Salesforce rejected
    /// its Id. Transient (not in <see cref="DefaultPermanentCodes"/>): an admin
    /// fixes the org, and the record types are read again.
    /// </summary>
    public const string InvalidRecordType = "INVALID_RECORD_TYPE";

    /// <summary>Our own code: the inbox event can't be mapped to records (malformed payload). Permanent.</summary>
    public const string InvalidEvent = "INVALID_EVENT";

    /// <summary>
    /// Our own code: the event is a person and Salesforce has the Account as a business account, or the other way
    /// round. An update can't change that (a conversion in Salesforce). Permanent: replay once the Account is fixed.
    /// </summary>
    public const string AccountKindMismatch = "ACCOUNT_KIND_MISMATCH";

    /// <summary>
    /// Salesforce's code when the org's data storage is full: every create fails until an admin frees space (deleted
    /// records count until the Recycle Bin is emptied). Stops the inbox for a while instead of failing entities.
    /// </summary>
    public const string StorageLimitExceeded = "STORAGE_LIMIT_EXCEEDED";

    /// <summary>Salesforce's code when a call is over the org's API limits (daily allocation, concurrent long requests).</summary>
    public const string RequestLimitExceeded = "REQUEST_LIMIT_EXCEEDED";

    /// <summary>
    /// Salesforce asks us to back off, for every call alike: over the org's API limits (403
    /// <see cref="RequestLimitExceeded"/>), 429, or 503 (down for maintenance or overloaded). Stops the inbox for a
    /// while instead of failing entities.
    /// </summary>
    public static bool IsRequestLimit(Exception ex)
    {
        return ex is SalesforceResponseException { StatusCode: { } status } r
               && (status is HttpStatusCode.TooManyRequests or HttpStatusCode.ServiceUnavailable
                   || (status == HttpStatusCode.Forbidden && r.Body.Contains(RequestLimitExceeded, StringComparison.Ordinal)));
    }

    /// <summary>Errors in the data or the org's configuration: retrying the same data fails the same way.</summary>
    public static readonly IReadOnlyList<string> DefaultPermanentCodes =
    [
        "FIELD_CUSTOM_VALIDATION_EXCEPTION",
        "FIELD_INTEGRITY_EXCEPTION",
        "REQUIRED_FIELD_MISSING",
        "INVALID_FIELD",
        "INVALID_FIELD_FOR_INSERT_UPDATE",
        "INVALID_TYPE_ON_FIELD_IN_RECORD",
        "INVALID_OR_NULL_FOR_RESTRICTED_PICKLIST",
        "INVALID_CROSS_REFERENCE_KEY",
        "INVALID_EMAIL_ADDRESS",
        "MALFORMED_ID",
        "NUMBER_OUTSIDE_VALID_RANGE",
        "STRING_TOO_LONG",
        "ENTITY_IS_DELETED",
        "INSUFFICIENT_ACCESS_ON_CROSS_REFERENCE_ENTITY",
        "INSUFFICIENT_ACCESS_OR_READONLY",
        "CANNOT_INSERT_UPDATE_ACTIVATE_ENTITY",
        // A delete refused, other than for dependents still pointing at the record (HasDependents: waited for)
        "DELETE_FAILED",
        InvalidEvent
    ];

    private readonly HashSet<string> _permanent = new(
        options.Value.PermanentErrorCodes ?? DefaultPermanentCodes, StringComparer.Ordinal);

    private readonly string[] _parentMissing = [.. options.Value.ParentMissingMessages.Where(m => !string.IsNullOrWhiteSpace(m))];

    public bool IsPermanent(string code)
    {
        return _permanent.Contains(code);
    }

    /// <summary>
    /// Whether a delete's error says the record is gone already: in the Recycle Bin (<c>ENTITY_IS_DELETED</c>, "entity
    /// is deleted", also seen under <c>DELETE_FAILED</c>), or no record has the Id (<c>INVALID_CROSS_REFERENCE_KEY</c>,
    /// <c>NOT_FOUND</c>). For a delete that is success: the record is not there. (On an upsert the same codes mean
    /// something else, so this applies to deletes only.)
    /// </summary>
    public static bool IsAlreadyDeleted(SalesforceError error)
    {
        return error.Code is "ENTITY_IS_DELETED" or "INVALID_CROSS_REFERENCE_KEY" or "NOT_FOUND"
               || error.Message?.Contains("entity is deleted", StringComparison.OrdinalIgnoreCase) == true;
    }

    /// <summary>
    /// Whether a delete was refused because other records still point at the record: Salesforce's own refusal for a
    /// lookup that restricts the delete, or the org's automation guarding a Text foreign key, both
    /// <c>DELETE_FAILED</c> with "... could not be completed because it is associated with the following ...". The
    /// dependents are expected to go through their own sync (an Account's contacts and relationships), so the delete
    /// waits for them like a record waits for its parent (<see cref="DependencyNotSynced"/>).
    /// </summary>
    public static bool HasDependents(SalesforceError error)
    {
        return error.Code == "DELETE_FAILED" && error.Message?.Contains("associated with", StringComparison.OrdinalIgnoreCase) == true;
    }

    /// <summary>Whether the error is the org saying a record's parent is missing (<see cref="SalesforceOptions.ParentMissingMessages"/>).</summary>
    public bool IsParentMissing(SalesforceError error)
    {
        return error.Message is { } message && _parentMissing.Any(m => message.Contains(m, StringComparison.OrdinalIgnoreCase));
    }
}

/// <summary>An error Salesforce reported for one record (or for a whole subrequest), or one found before sending.</summary>
/// <param name="Fields">The fields Salesforce blames, when it says.</param>
/// <param name="ReferenceId">The subrequest the error belongs to, null for a call of its own.</param>
/// <param name="Code">Salesforce's error code, e.g. FIELD_CUSTOM_VALIDATION_EXCEPTION.</param>
/// <param name="Message">Salesforce's message, if any.</param>
public sealed record SalesforceError(string? ReferenceId, string Code, string? Message, IReadOnlyList<string>? Fields = null)
{
    public override string ToString()
    {
        return ReferenceId is null ? $"{Code} {Message}" : $"{ReferenceId}: {Code} {Message}";
    }
}

/// <summary>Salesforce answered with an error status. <see cref="IsSalesforceError"/>: the body is Salesforce's own error list.</summary>
public sealed class SalesforceResponseException(HttpStatusCode status, string responseBody)
    : HttpRequestException($"Salesforce {(int)status}: {responseBody}", null, status)
{
    /// <summary>
    /// True when Salesforce itself produced the response (<c>[{ "errorCode": ..., "message": ... }]</c>), as opposed to a
    /// gateway or load balancer in front of it, which can't know whether the request was processed.
    /// </summary>
    public bool IsSalesforceError => Body.Contains("\"errorCode\"", StringComparison.Ordinal);

    /// <summary>The response's body, Salesforce's error list when <see cref="IsSalesforceError"/>.</summary>
    public string Body { get; } = responseBody;

    /// <summary>Salesforce's code of the first error of the response, e.g. INVALID_FIELD, or null.</summary>
    public string? ErrorCode
    {
        get
        {
            try
            {
                using var json = System.Text.Json.JsonDocument.Parse(Body);
                return json.RootElement is { ValueKind: System.Text.Json.JsonValueKind.Array } list && list.GetArrayLength() > 0
                    ? SalesforceResponseParser.StringProperty(list[0], "errorCode")
                    : null;
            }
            catch (System.Text.Json.JsonException)
            {
                return null;
            }
        }
    }
}