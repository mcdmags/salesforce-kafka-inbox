using SalesforceSync.Salesforce;

namespace SalesforceSync.Sync;

/// <summary>The outcome for one entity (one unit).</summary>
/// <param name="RetryAfter">For <see cref="SyncOutcome.RateLimited"/>: how long to wait before trying again.</param>
public sealed record UpsertResult(string Key, SyncOutcome Outcome, IReadOnlyList<SalesforceError> Errors, TimeSpan? RetryAfter = null)
{
    /// <summary>The error that decided the outcome (errors are ordered most significant first).</summary>
    public SalesforceError? PrimaryError =>
        Errors.FirstOrDefault(e => !SalesforceErrors.NotAttemptedCodes.Contains(e.Code)) ?? Errors.FirstOrDefault();
}