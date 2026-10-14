namespace SalesforceSync.Salesforce;

/// <summary>A query subrequest failed, e.g. INVALID_FIELD because a field was deleted after it was described.</summary>
public sealed class SalesforceQueryException(string referenceId, IReadOnlyList<SalesforceError> errors)
    : Exception($"Query {referenceId} failed: {string.Join("; ", errors)}")
{
    public string ReferenceId { get; } = referenceId;
    public IReadOnlyList<SalesforceError> Errors { get; } = errors;
}