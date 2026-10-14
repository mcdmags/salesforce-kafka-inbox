using SalesforceSync.Salesforce;

namespace SalesforceSync.Sync;

/// <summary>The outcome for one entity (one unit).</summary>
public sealed record UpsertResult(string Key, SyncOutcome Outcome, IReadOnlyList<SalesforceError> Errors)
{
    /// <summary>The error that decided the outcome (errors are ordered most significant first).</summary>
    public SalesforceError? PrimaryError =>
        Errors.FirstOrDefault(e => e.Code != SalesforceErrors.ProcessingHalted) ?? Errors.FirstOrDefault();
}