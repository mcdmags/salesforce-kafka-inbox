namespace SalesforceSync.Infrastructure.Salesforce;

/// <summary>
/// One record's result in an upsert or a delete, in request order, as Salesforce answered it; callers decide what it
/// means. <c>Created</c> is set for upserts only.
/// </summary>
public sealed record RecordResult(bool Success, string? Id, IReadOnlyList<SalesforceError> Errors, bool? Created = null);
