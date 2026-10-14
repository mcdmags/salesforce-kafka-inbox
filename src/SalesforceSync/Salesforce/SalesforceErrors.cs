using Microsoft.Extensions.Options;
using SalesforceSync.Sync;

namespace SalesforceSync.Salesforce;

/// <summary>An error Salesforce reported for one record (or for a whole subrequest), or one found before sending.</summary>
/// <param name="Fields">The fields Salesforce blames, when it says.</param>
public sealed record SalesforceError(string? ReferenceId, string Code, string? Message, IReadOnlyList<string>? Fields = null)
{
    public override string ToString() => ReferenceId is null ? $"{Code} {Message}" : $"{ReferenceId}: {Code} {Message}";
}

/// <summary>Decides what a failed record's errors mean: retry now, retry later, or give up.</summary>
public sealed class SalesforceErrors(IOptions<SalesforceOptions> options)
{
    /// <summary>
    /// Our own code (not Salesforce's): a lookup target, e.g. the Account of a relationship, isn't in Salesforce yet.
    /// Transient: it is expected to arrive through its own sync.
    /// </summary>
    public const string DependencyNotSynced = "DEPENDENCY_NOT_SYNCED";

    /// <summary>Our own code: the inbox event can't be mapped to records (malformed payload). Permanent.</summary>
    public const string InvalidEvent = "INVALID_EVENT";

    /// <summary>
    /// Our own code: the event's meta.type disagrees with Salesforce (inserted but the record exists, updated but it
    /// doesn't) and <see cref="Inbox.SyncOptions.EventTypeMismatch"/> says to dead-letter rather than reconcile.
    /// </summary>
    public const string EventTypeMismatch = "EVENT_TYPE_MISMATCH";

    /// <summary>The record itself was fine but was not saved, because of another record in the request.</summary>
    public static readonly IReadOnlySet<string> NotAttemptedCodes = new HashSet<string>(StringComparer.Ordinal)
    {
        "PROCESSING_HALTED",                  // a subrequest skipped or undone because an earlier one failed
        "ALL_OR_NONE_OPERATION_ROLLED_BACK"   // allOrNone: rolled back because another record failed
    };

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

    public bool IsPermanent(string code) => _permanent.Contains(code);

    /// <summary>Classifies the errors of one failed record.</summary>
    public SyncOutcome Classify(IReadOnlyCollection<SalesforceError> errors)
    {
        var real = errors.Where(e => !NotAttemptedCodes.Contains(e.Code)).ToList();
        if (real.Count == 0) return SyncOutcome.NotAttempted;
        return real.Any(e => IsPermanent(e.Code)) ? SyncOutcome.PermanentFailure : SyncOutcome.TransientFailure;
    }
}
