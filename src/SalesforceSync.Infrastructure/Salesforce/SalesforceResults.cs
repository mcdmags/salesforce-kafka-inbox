namespace SalesforceSync.Infrastructure.Salesforce;

// What Salesforce answered, per subrequest and per record, with nothing decided about it: the engine interprets these.

/// <summary>One record's result within a collection, in request order.</summary>
/// <param name="Created">Upserts only: whether the record was created (true) or updated (false).</param>
/// <param name="Success">Whether the record was saved.</param>
/// <param name="Id">The record's Id when it was saved.</param>
/// <param name="Errors">Why it wasn't saved; empty when it was.</param>
public sealed record RecordResult(bool Success, string? Id, IReadOnlyList<SalesforceError> Errors, bool? Created = null);

/// <param name="Records">Null when the subrequest failed as a whole (see <paramref name="Errors"/>).</param>
/// <param name="ReferenceId">The collection's reference id in the request.</param>
/// <param name="Errors">Why the subrequest failed as a whole; empty when it answered per record.</param>
public sealed record CollectionResult(string ReferenceId, IReadOnlyList<RecordResult>? Records, IReadOnlyList<SalesforceError> Errors);
