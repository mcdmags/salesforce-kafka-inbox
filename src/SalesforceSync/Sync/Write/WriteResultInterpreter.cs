using SalesforceSync.Infrastructure.Salesforce;

namespace SalesforceSync.Sync.Write;

/// <summary>What came of one record, least to most significant: an entity's outcome follows its worst record.</summary>
internal enum WriteStatus
{
    Saved,
    /// <summary>May succeed later: retry with backoff.</summary>
    Transient,
    /// <summary>Retrying the same data fails the same way: dead-letter.</summary>
    Permanent
}

/// <summary>
/// Reads Salesforce's answer for one record: saved, or failed and, if failed, whether retrying can help. Upserts and
/// deletes are read the same way: a refused delete is permanent or transient by its code like a rejected upsert.
/// </summary>
internal sealed class WriteResultInterpreter(SalesforceErrors errors)
{
    /// <param name="change">The record that was sent.</param>
    /// <param name="result">Salesforce's answer for it; null when it gave none.</param>
    public (WriteStatus Status, List<SalesforceError> Errors) Interpret(Change change, RecordResult? result)
    {
        if (result is null)
        {
            return (WriteStatus.Transient, [new SalesforceError(change.ReferenceId, "UNKNOWN_ERROR", "Salesforce gave no result for the record")]);
        }

        if (result is { Success: true, Created: true } && !change.IsCreate && !change.IsDelete)
        {
            // An update sent as an upsert found no record by its external id (deleted after the read) and created one with
            // only the changed fields: the retry reads it and writes the rest
            return (WriteStatus.Transient, [new SalesforceError(change.ReferenceId, "ENTITY_IS_DELETED",
                "The record was gone when the update was upserted; it was created with the changed fields only")]);
        }

        // A record to delete that is gone already (deleted after the read, by someone else or by an earlier attempt whose
        // answer was lost) is what the delete wanted
        var gone = change.IsDelete && result.Errors.Count > 0 && result.Errors.All(SalesforceErrors.IsAlreadyDeleted);
        if (result.Success || gone)
        {
            return (WriteStatus.Saved, []);
        }

        var failures = result.Errors.Select(e => Translate(change, e)).ToList();
        if (failures.Count == 0)
        {
            failures.Add(new SalesforceError(change.ReferenceId, "UNKNOWN_ERROR", "The record failed without error details"));
        }

        var permanent = failures.Any(e => errors.IsPermanent(e.Code) && !IsStaleUpdate(change, e));
        return (permanent ? WriteStatus.Permanent : WriteStatus.Transient, [.. failures.OrderByDescending(e => errors.IsPermanent(e.Code))]);
    }

    /// <summary>
    /// The org rejecting a record because its foreign key's parent isn't there (a message listed in
    /// <see cref="SalesforceOptions.ParentMissingMessages"/>, whatever the code) is the same situation as the read not
    /// finding it: the parent is expected to arrive through its own sync, so the record waits for it rather than being
    /// dead-lettered. The same in reverse: a delete refused because records still point at it
    /// (<see cref="SalesforceErrors.HasDependents"/>, an Account whose contacts are not deleted yet) waits for their
    /// deletes, which their own inboxes sync.
    /// <para>
    /// Salesforce rejecting the RecordTypeId (the record type was deactivated or unassigned after the describe) is an
    /// <see cref="SalesforceErrors.InvalidRecordType"/>, like the describe not offering it: retried, not dead-lettered.
    /// Salesforce often leaves <c>fields</c> empty on this error ("Record Type ID: this ID value isn't valid for the
    /// user"), so the message identifies it too.
    /// </para>
    /// </summary>
    private SalesforceError Translate(Change change, SalesforceError e)
    {
        if (e.Code is "INVALID_CROSS_REFERENCE_KEY" && RejectsRecordType(e))
        {
            return new SalesforceError(change.ReferenceId, SalesforceErrors.InvalidRecordType, $"{e.Code}: {e.Message}", e.Fields);
        }

        var parentMissing = errors.IsParentMissing(e) || (change.IsDelete && SalesforceErrors.HasDependents(e));
        return parentMissing
            ? new SalesforceError(change.ReferenceId, SalesforceErrors.DependencyNotSynced, $"{e.Code}: {e.Message}", e.Fields)
            : e with { ReferenceId = change.ReferenceId };
    }

    private static bool RejectsRecordType(SalesforceError e)
    {
        return e.Fields?.Contains(SObjectSchema.RecordTypeIdField, StringComparer.OrdinalIgnoreCase) == true
               || e.Message?.Contains("Record Type", StringComparison.OrdinalIgnoreCase) == true;
    }

    /// <summary>
    /// The record was deleted between the read and the update: the next read finds it gone and creates it. The update, an
    /// upsert, then tries to create the record from the changed fields alone, which fails on a required field the update
    /// didn't need to send.
    /// </summary>
    private static bool IsStaleUpdate(Change change, SalesforceError e)
    {
        return !change.IsCreate && !change.IsDelete && e.Code is "INVALID_CROSS_REFERENCE_KEY" or "ENTITY_IS_DELETED" or "REQUIRED_FIELD_MISSING";
    }
}
