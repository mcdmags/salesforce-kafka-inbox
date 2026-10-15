using System.Net;
using Microsoft.Extensions.Options;

namespace SalesforceSync.Salesforce;

/// <summary>Decides what a failed record's errors mean: retry now, retry later, or give up.</summary>
public sealed class SalesforceErrors(IOptions<SalesforceOptions> options)
{
    /// <summary>
    /// Our own code (not Salesforce's): a lookup target or a foreign key's parent, e.g. the Account of a relationship,
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
        InvalidEvent
    ];

    private readonly HashSet<string> _permanent = new(
        options.Value.PermanentErrorCodes ?? DefaultPermanentCodes, StringComparer.Ordinal);

    private readonly string[] _parentMissing = [.. options.Value.ParentMissingMessages.Where(m => !string.IsNullOrWhiteSpace(m))];

    public bool IsPermanent(string code)
    {
        return _permanent.Contains(code);
    }

    /// <summary>Whether the error is the org saying a record's parent is missing (<see cref="SalesforceOptions.ParentMissingMessages"/>).</summary>
    public bool IsParentMissing(SalesforceError error)
    {
        return error.Message is { } message && _parentMissing.Any(m => message.Contains(m, StringComparison.OrdinalIgnoreCase));
    }
}

/// <summary>An error Salesforce reported for one record (or for a whole subrequest), or one found before sending.</summary>
/// <param name="Fields">The fields Salesforce blames, when it says.</param>
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
    public string ReferenceId { get; } = referenceId;
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