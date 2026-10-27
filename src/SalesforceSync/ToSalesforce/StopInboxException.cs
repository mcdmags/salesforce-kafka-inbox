using SalesforceSync.Infrastructure.Salesforce;

namespace SalesforceSync.ToSalesforce;

/// <summary>Why an inbox stops (<see cref="StopInboxException"/>).</summary>
public enum StopReason
{
    /// <summary>A field the code sends that the org lacks or won't let us set. Fixed by a deploy or an org field.</summary>
    MappingError,

    /// <summary>The org's data storage is full (STORAGE_LIMIT_EXCEEDED). Fixed by freeing space.</summary>
    StorageFull,

    /// <summary>Salesforce asks us to back off (<see cref="SalesforceErrors.IsRequestLimit"/>). Clears with time.</summary>
    RequestLimit
}

/// <summary>
/// An error that is wrong for every entity of the type, not just one. The type's inbox stops for a while: no attempt is
/// spent and nothing is dead-lettered until the cause is fixed.
/// </summary>
public sealed class StopInboxException(StopReason reason, IReadOnlyList<(string Key, SalesforceError Error)> errors)
    : Exception($"{errors.Count} error(s), e.g. {errors[0].Error.Code}: {errors[0].Error.Message}")
{
    public StopReason Reason => reason;

    public IReadOnlyList<(string Key, SalesforceError Error)> Errors => errors;
}
