namespace SalesforceSync.Infrastructure.Salesforce;

// What Salesforce answered for one record, with nothing decided about it: the engine interprets it.

/// <summary>One record's result in an upsert or a delete, in request order.</summary>
/// <param name="Created">Upserts only: whether the record was created (true) or updated (false).</param>
/// <param name="Success">Whether the record was saved.</param>
/// <param name="Id">The record's Id when it was saved.</param>
/// <param name="Errors">Why it wasn't saved; empty when it was.</param>
public sealed record RecordResult(bool Success, string? Id, IReadOnlyList<SalesforceError> Errors, bool? Created = null);
