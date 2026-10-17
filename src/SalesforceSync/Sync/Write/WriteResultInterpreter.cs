using SalesforceSync.Salesforce;

namespace SalesforceSync.Sync.Write;

/// <summary>
/// Turns Salesforce's per-record results into one outcome per unit: saved (or deleted) only if every record is;
/// permanent if any record's own error is; else transient if any is; else not attempted. Upserts and deletes are
/// read the same way: a refused delete is permanent or transient by its code like a rejected upsert.
/// </summary>
internal sealed class WriteResultInterpreter(SalesforceErrors errors)
{
    /// <summary>One outcome per unit of the request, from its upserts' answers by reference id.</summary>
    public List<SyncResult> Interpret(CompositeWrite request, IReadOnlyList<CollectionResult> answers)
    {
        // Parents were sent first, so a child is classified after its parent
        foreach (var (referenceId, _, writes) in request.Collections)
        {
            Classify(referenceId, writes, answers.FirstOrDefault(a => a.ReferenceId == referenceId));
        }

        return Results(request.Units);
    }

    /// <summary>One outcome per unit, from its records' statuses (every collection with its writes classified).</summary>
    private List<SyncResult> Results(IReadOnlyList<PlannedUnit> units)
    {
        // Records failed, but none says why (every one of them reports being skipped because of another, or has no
        // answer): handing the units straight back would repeat the same request forever, so they spend a retry and
        // back off instead.
        var all = units.SelectMany(u => u.Writes).ToList();
        var causeUnknown = all.Any(w => w.Status != WriteStatus.Saved)
                           && !all.Any(w => w.Status is WriteStatus.Transient or WriteStatus.Permanent);

        return [.. units.Select(unit => UnitResult(unit, causeUnknown))];
    }

    /// <summary>Sets the status and errors of every record of one collection from its upsert's answer.</summary>
    private void Classify(string referenceId, List<PlannedWrite> writes, CollectionResult? answer)
    {
        var subrequestErrors = SubrequestErrors(referenceId, writes.Count, answer);
        // A reference to a parent that wasn't saved has no value, which fails every record of the subrequest
        var brokenReference = writes.Any(w => w.Links.Any(l => l.Parent.Status != WriteStatus.Saved));

        for (var i = 0; i < writes.Count; i++)
        {
            var write = writes[i];
            var result = subrequestErrors is null ? answer!.Records![i] : null;
            if (result is { Success: true, Created: true } && !write.Create)
            {
                // An update sent as an upsert found no record by its external id (deleted after the read) and created
                // one with only the changed fields: the retry reads it and writes the rest
                write.Errors = [new SalesforceError(write.ReferenceId, "ENTITY_IS_DELETED",
                    "The record was gone when the update was upserted; it was created with the changed fields only")];
                write.Status = WriteStatus.Transient;
                continue;
            }

            // A record to delete that is gone already (deleted after the read, by someone else or by an earlier attempt
            // whose answer was lost) is what the delete wanted
            if (result is { Success: true } || (write.Delete && result is { Errors: [_, ..] why }
                                                && why.All(SalesforceErrors.IsAlreadyDeleted)))
            {
                write.Status = WriteStatus.Saved;
                continue;
            }

            write.Errors = [.. (result?.Errors ?? subrequestErrors!).Select(e => Translate(write, e))];
            if (result is not null && write.Errors.Count == 0)
            {
                write.Errors.Add(new SalesforceError(write.ReferenceId, "UNKNOWN_ERROR", "The record failed without error details"));
            }

            write.Status = FailureStatus(write, hasOwnResult: result is not null, brokenReference);
        }
    }

    /// <summary>
    /// The errors that apply to every record of a subrequest that has no usable per-record results, or null when
    /// each record has a result of its own.
    /// </summary>
    private static IReadOnlyList<SalesforceError>? SubrequestErrors(string referenceId, int sent, CollectionResult? answer)
    {
        return answer switch
        {
            null => [],   // not in the response: never processed
            { Records: null } => answer.Errors,
            { Records: var records } when records.Count != sent =>
                [new SalesforceError(referenceId, "UNKNOWN_ERROR", $"{records.Count} results for {sent} records")],
            _ => null
        };
    }

    /// <param name="hasOwnResult">False when the record's subrequest failed as a whole.</param>
    /// <param name="write">The record that wasn't saved.</param>
    /// <param name="brokenReference">Its subrequest failed because a reference had no value.</param>
    private WriteStatus FailureStatus(PlannedWrite write, bool hasOwnResult, bool brokenReference)
    {
        // E.g. the org rejecting a child whose new parent, an earlier upsert of the same Composite request, wasn't
        // saved (children are sent regardless, and the org decides): the parent's result decides the unit
        if (write.Parents.Any(p => p.Status != WriteStatus.Saved))
        {
            return WriteStatus.ParentNotSaved;
        }

        var ownErrors = write.Errors.Where(e => e.Code != SalesforceErrors.ProcessingHalted).ToList();
        if (ownErrors.Count == 0 || (!hasOwnResult && brokenReference))
        {
            return WriteStatus.NotAttempted;
        }

        // A subrequest that failed as a whole says nothing about any one record's data, so it never
        // dead-letters: the records are retried, and by then a child finds its parent by the read
        return hasOwnResult && ownErrors.Any(e => errors.IsPermanent(e.Code) && !IsStaleUpdate(write, e))
            ? WriteStatus.Permanent
            : WriteStatus.Transient;
    }

    private SyncResult UnitResult(PlannedUnit unit, bool causeUnknown)
    {
        // Most significant first: the record that decided the outcome, and within it the permanent error
        var unitErrors = unit.Writes.Where(w => w.Status != WriteStatus.ParentNotSaved).OrderByDescending(w => w.Status)
            .SelectMany(w => w.Errors.OrderByDescending(e => errors.IsPermanent(e.Code)))
            .ToList();
        var outcome = unit.Writes.Max(w => w.Status) switch
        {
            WriteStatus.Saved => SyncOutcome.Saved,
            WriteStatus.Permanent => SyncOutcome.PermanentFailure,
            WriteStatus.Transient => SyncOutcome.TransientFailure,
            _ => SyncOutcome.NotAttempted
        };
        if (outcome == SyncOutcome.NotAttempted && causeUnknown)
        {
            outcome = SyncOutcome.TransientFailure;
            unitErrors.Insert(0, new SalesforceError(null, "UNKNOWN_ERROR",
                "Records of the request failed without any of them reporting the cause"));
        }
        return new SyncResult(unit.Key, outcome, unitErrors);
    }

    /// <summary>
    /// Salesforce rejecting a lookup because its target isn't there (the target was deleted after the read), or the
    /// org rejecting a record because its foreign key's parent isn't there (a message listed in
    /// <see cref="SalesforceOptions.ParentMissingMessages"/>, whatever the code), is the same situation as the read not
    /// finding it: the parent is expected to arrive through its own sync, so the record waits for it rather than being
    /// dead-lettered. The same in reverse: a delete refused because records still point at it
    /// (<see cref="SalesforceErrors.HasDependents"/>, an Account whose contacts are not deleted yet) waits for their
    /// deletes, which their own inboxes sync.
    /// <para>
    /// Salesforce rejecting the RecordTypeId (the record type was deactivated or unassigned after the describe) is an
    /// <see cref="SalesforceErrors.InvalidRecordType"/>, like the describe not offering it: retried, not dead-lettered.
    /// Salesforce often leaves <c>fields</c> empty on this error ("Record Type ID: this ID value isn't valid for the
    /// user"), so the message identifies it too; it is checked before lookups, or a record with lookups would wait
    /// for a parent with the stale record type Id still cached.
    /// </para>
    /// </summary>
    private SalesforceError Translate(PlannedWrite write, SalesforceError e)
    {
        if (e.Code is "INVALID_CROSS_REFERENCE_KEY" && RejectsRecordType(e))
        {
            return new SalesforceError(write.ReferenceId, SalesforceErrors.InvalidRecordType, $"{e.Code}: {e.Message}", e.Fields);
        }

        var targetMissing = (write.LookupFields.Count > 0
                             && e.Code is "INVALID_CROSS_REFERENCE_KEY" or "ENTITY_IS_DELETED"
                             && (e.Fields is not { Count: > 0 } || e.Fields.Any(write.LookupFields.Contains)))
                            || errors.IsParentMissing(e)
                            || (write.Delete && SalesforceErrors.HasDependents(e));
        return targetMissing
            ? new SalesforceError(write.ReferenceId, SalesforceErrors.DependencyNotSynced, $"{e.Code}: {e.Message}", e.Fields)
            : e with { ReferenceId = write.ReferenceId };
    }

    private static bool RejectsRecordType(SalesforceError e)
    {
        return e.Fields?.Contains(SObjectSchema.RecordTypeIdField, StringComparer.OrdinalIgnoreCase) == true
               || e.Message?.Contains("Record Type", StringComparison.OrdinalIgnoreCase) == true;
    }

    /// <summary>
    /// The record was deleted between the read and the update: the next read finds it gone and creates it. The update,
    /// an upsert, then tries to create the record from the changed fields alone, which fails on a required field the
    /// update didn't need to send.
    /// </summary>
    private static bool IsStaleUpdate(PlannedWrite write, SalesforceError e)
    {
        return !write.Create && !write.Delete && e.Code is "INVALID_CROSS_REFERENCE_KEY" or "ENTITY_IS_DELETED" or "REQUIRED_FIELD_MISSING";
    }
}