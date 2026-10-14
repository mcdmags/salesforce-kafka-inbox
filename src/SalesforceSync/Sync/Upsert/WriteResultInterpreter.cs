using SalesforceSync.Salesforce;

namespace SalesforceSync.Sync.Upsert;

/// <summary>
/// Turns Salesforce's per-record results into one outcome per unit: saved only if every record is; permanent if
/// any record's own error is; else transient if any is; else not attempted.
/// </summary>
internal sealed class WriteResultInterpreter(SalesforceErrors errors)
{
    /// <param name="saved">Receives the values of every record that was saved.</param>
    public List<UpsertResult> Interpret(CompositeWrite request, IReadOnlyList<CollectionResult> answers, List<WriteEcho> saved)
    {
        // Parents were sent first, so a child is classified after its parent
        foreach (var (referenceId, writes) in request.Collections)
            ClassifyCollection(referenceId, writes, answers.FirstOrDefault(a => a.ReferenceId == referenceId), saved);

        // Records failed, but none says why (every one of them reports being rolled back because of another): handing
        // the units straight back would repeat the same request forever, so they spend a retry and back off instead.
        var all = request.Units.SelectMany(u => u.Writes).ToList();
        var causeUnknown = all.Any(w => w.Status != WriteStatus.Saved)
                           && !all.Any(w => w.Status is WriteStatus.Transient or WriteStatus.Permanent);

        return request.Units.Select(unit => UnitResult(unit, causeUnknown)).ToList();
    }

    /// <summary>Sets the status and errors of every record of one collection from its subrequest's answer.</summary>
    private void ClassifyCollection(string referenceId, List<PlannedWrite> writes, CollectionResult? answer, List<WriteEcho> saved)
    {
        var subrequestErrors = SubrequestErrors(referenceId, writes.Count, answer);
        // A reference to a parent that wasn't saved has no value, which fails every record of the subrequest
        var brokenReference = writes.Any(w => w.Links.Any(l => l.ByReference && l.Parent.Status != WriteStatus.Saved));

        for (var i = 0; i < writes.Count; i++)
        {
            var write = writes[i];
            var result = subrequestErrors is null ? answer!.Records![i] : null;
            if (result is { Success: true })
            {
                write.Status = WriteStatus.Saved;
                saved.AddRange(write.Echoes);
                continue;
            }

            write.Errors = (result?.Errors ?? subrequestErrors!).Select(e => Translate(write, e)).ToList();
            if (result is not null && write.Errors.Count == 0)
                write.Errors.Add(new SalesforceError(write.ReferenceId, "UNKNOWN_ERROR", "The record failed without error details"));

            write.Status = FailureStatus(write, hasOwnResult: result is not null, brokenReference);
        }
    }

    /// <summary>
    /// The errors that apply to every record of a subrequest that has no usable per-record results, or null when
    /// each record has a result of its own.
    /// </summary>
    private static IReadOnlyList<SalesforceError>? SubrequestErrors(string referenceId, int sent, CollectionResult? answer)
        => answer switch
        {
            null => [],   // not in the response: never processed
            { Records: null } => answer.Errors,
            { Records: var records } when records.Count != sent =>
                [new SalesforceError(referenceId, "UNKNOWN_ERROR", $"{records.Count} results for {sent} records")],
            _ => null
        };

    /// <param name="hasOwnResult">False when the record's subrequest failed as a whole.</param>
    private WriteStatus FailureStatus(PlannedWrite write, bool hasOwnResult, bool brokenReference)
    {
        if (write.Links.Any(l => l.Parent.Status != WriteStatus.Saved)) return WriteStatus.ParentNotSaved;

        var ownErrors = write.Errors.Where(e => !SalesforceErrors.NotAttemptedCodes.Contains(e.Code)).ToList();
        if (ownErrors.Count == 0 || (!hasOwnResult && brokenReference)) return WriteStatus.NotAttempted;

        // A subrequest that failed as a whole says nothing about any one record's data, so it never
        // dead-letters: the records are retried, and by then a child finds its parent by the read
        return hasOwnResult && ownErrors.Any(e => errors.IsPermanent(e.Code) && !IsStaleUpdate(write, e))
            ? WriteStatus.Permanent
            : WriteStatus.Transient;
    }

    private UpsertResult UnitResult(PlannedUnit unit, bool causeUnknown)
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
        return new UpsertResult(unit.Key, outcome, unitErrors);
    }

    /// <summary>
    /// Salesforce rejecting a lookup because its target isn't there (the target was deleted after the read, or the
    /// external id it is addressed by doesn't exist) is the same situation as the read not finding it: the target is
    /// expected to arrive through its own sync, so the record waits for it rather than being dead-lettered.
    /// </summary>
    private static SalesforceError Translate(PlannedWrite write, SalesforceError e)
    {
        var targetMissing =
            e.Message?.Contains("Foreign key external ID", StringComparison.OrdinalIgnoreCase) == true
            || (e.Code is "INVALID_CROSS_REFERENCE_KEY" or "ENTITY_IS_DELETED"
                && write.LookupFields.Count > 0
                && (e.Fields is not { Count: > 0 } || e.Fields.Any(write.LookupFields.Contains)));
        return targetMissing
            ? new SalesforceError(write.ReferenceId, SalesforceErrors.DependencyNotSynced, $"{e.Code}: {e.Message}", e.Fields)
            : e with { ReferenceId = write.ReferenceId };
    }

    /// <summary>The record was deleted between the read and the update: the next read finds it gone and creates it.</summary>
    private static bool IsStaleUpdate(PlannedWrite write, SalesforceError e)
        => !write.Create && e.Code is "INVALID_CROSS_REFERENCE_KEY" or "ENTITY_IS_DELETED";
}
