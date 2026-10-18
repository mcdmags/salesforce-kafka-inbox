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
    /// Our own code: the record's record type (its sObject class's <c>RecordType</c>) is not in the object's Describe,
    /// not active, or not available to the running user, or Salesforce rejected its Id. Transient (not in
    /// <see cref="DefaultPermanentCodes"/>): an admin fixes the org, and the next describe sees it.
    /// </summary>
    public const string InvalidRecordType = "INVALID_RECORD_TYPE";

    /// <summary>Our own code: the inbox event can't be mapped to records (malformed payload). Permanent.</summary>
    public const string InvalidEvent = "INVALID_EVENT";

    /// <summary>
    /// A subrequest skipped because an earlier one it refers to failed: the record itself was fine but was not saved,
    /// because of another record in the request.
    /// </summary>
    public const string ProcessingHalted = "PROCESSING_HALTED";

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

/// <summary>A query subrequest failed, e.g. INVALID_FIELD because a field was deleted after it was described.</summary>
public sealed class SalesforceQueryException(string referenceId, IReadOnlyList<SalesforceError> errors)
    : Exception($"Query {referenceId} failed: {string.Join("; ", errors)}")
{
    public IReadOnlyList<SalesforceError> Errors { get; } = errors;
}

/// <summary>Salesforce answered with an error status. <see cref="IsSalesforceError"/>: the body is Salesforce's own error list.</summary>
public sealed class SalesforceResponseException(HttpStatusCode status, string body)
    : HttpRequestException($"Salesforce {(int)status}: {body}", null, status)
{
    /// <summary>
    /// True when Salesforce itself produced the response (<c>[{ "errorCode": ..., "message": ... }]</c>), as opposed to a
    /// gateway or load balancer in front of it, which can't know whether the request was processed.
    /// </summary>
    public bool IsSalesforceError { get; } = body.Contains("\"errorCode\"", StringComparison.Ordinal);
}