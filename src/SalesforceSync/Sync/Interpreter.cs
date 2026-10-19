using SalesforceSync.Infrastructure.Salesforce;
using static SalesforceSync.Sync.Differ;

namespace SalesforceSync.Sync;

/// <summary>
/// Step 6. Reads Salesforce's answer for one record: saved, or failed and whether retrying can help.
/// </summary>
internal sealed class Interpreter(SalesforceErrors errors)
{
    /// <summary>
    /// Reads Salesforce's answer for one record: saved, or failed and, if failed, whether retrying can help. Upserts and
    /// deletes are read the same way: a refused delete is permanent or transient by its code, like a rejected upsert.
    /// </summary>
    /// <param name="change">The record that was sent.</param>
    /// <param name="result">Salesforce's answer for it; null when it gave none.</param>
    /// <returns>Saved, transient or permanent failure, and the errors (permanent first).</returns>
    internal (SyncOutcome Outcome, List<SalesforceError> Errors) Interpret(Change change, RecordResult? result)
    {
        if (result is null)
        {
            return (SyncOutcome.TransientFailure, [new SalesforceError(change.ReferenceId, "UNKNOWN_ERROR", "Salesforce gave no result for the record")]);
        }

        if (result is { Success: true, Created: true } && !change.IsCreate && !change.IsDelete)
        {
            // An update sent as an upsert found no record by its external id (deleted after the read) and created one with
            // only the changed fields. The retry reads it and writes the rest
            return (SyncOutcome.TransientFailure, [new SalesforceError(change.ReferenceId, "ENTITY_IS_DELETED",
                "The record was gone when the update was upserted; it was created with the changed fields only")]);
        }

        // A record to delete that is already gone (deleted after the read, by someone else or by an earlier attempt whose
        // answer was lost) is what the delete wanted
        var gone = change.IsDelete && result.Errors.Count > 0 && result.Errors.All(SalesforceErrors.IsAlreadyDeleted);
        if (result.Success || gone)
        {
            return (SyncOutcome.Saved, []);
        }

        var failures = result.Errors.Select(e => Translate(change, e)).ToList();
        if (failures.Count == 0)
        {
            failures.Add(new SalesforceError(change.ReferenceId, "UNKNOWN_ERROR", "The record failed without error details"));
        }

        // An update of a record deleted between the read and the write is retried, not dead-lettered: the next read finds
        // it gone and creates it. The update, an upsert, tries to create the record from the changed fields alone,
        // which fails on a required field the update didn't need to send.
        var permanent = failures.Any(e => errors.IsPermanent(e.Code)
            && !(!change.IsCreate && !change.IsDelete && e.Code is "INVALID_CROSS_REFERENCE_KEY" or "ENTITY_IS_DELETED" or "REQUIRED_FIELD_MISSING"));
        return (permanent ? SyncOutcome.PermanentFailure : SyncOutcome.TransientFailure,
            [.. failures.OrderByDescending(e => errors.IsPermanent(e.Code))]);
    }

    /// <summary>
    /// The org rejecting a record because its foreign key's parent isn't there (a message listed in
    /// <see cref="SalesforceOptions.ParentMissingMessages"/>, whatever the code) is the same as the read not finding
    /// it: the parent should arrive through its own sync, so the record waits for it instead of being dead-lettered.
    /// The same in reverse: a delete refused because records still point at it
    /// (<see cref="SalesforceErrors.HasDependents"/>, an Account whose contacts are not deleted yet) waits for their
    /// deletes, which their own inboxes sync. Salesforce rejecting the RecordTypeId (the record type was deactivated or
    /// unassigned after its Id was read) is an <see cref="SalesforceErrors.InvalidRecordType"/>, like the org not having
    /// it: retried, not dead-lettered. Salesforce often leaves <c>fields</c> empty on that error ("Record Type ID: this ID
    /// value isn't valid for the user"), so the message identifies it too.
    /// </summary>
    private SalesforceError Translate(Change change, SalesforceError e)
    {
        var recordType = e.Code is "INVALID_CROSS_REFERENCE_KEY"
                         && (e.Fields?.Contains(RecordTypeCache.RecordTypeIdField, StringComparer.OrdinalIgnoreCase) == true
                             || e.Message?.Contains("Record Type", StringComparison.OrdinalIgnoreCase) == true);

        return recordType ? new SalesforceError(change.ReferenceId, SalesforceErrors.InvalidRecordType, $"{e.Code}: {e.Message}", e.Fields)
            : errors.IsParentMissing(e) || (change.IsDelete && SalesforceErrors.HasDependents(e))
                ? new SalesforceError(change.ReferenceId, SalesforceErrors.DependencyNotSynced, $"{e.Code}: {e.Message}", e.Fields)
                : e with { ReferenceId = change.ReferenceId };
    }
}
