using Microsoft.Extensions.Options;

namespace SalesforceSync.Salesforce;

/// <summary>Decides what a failed record's errors mean: retry now, retry later, or give up.</summary>
public sealed class SalesforceErrors(IOptions<SalesforceOptions> options)
{
    /// <summary>
    /// Our own code (not Salesforce's): a lookup target, e.g. the Account of a relationship, isn't in Salesforce yet.
    /// Transient: it is expected to arrive through its own sync.
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

    public bool IsPermanent(string code)
    {
        return _permanent.Contains(code);
    }
}