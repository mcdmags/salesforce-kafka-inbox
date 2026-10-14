namespace SalesforceSync.Salesforce;

/// <summary>A SOQL query to run as one subrequest of a Composite request.</summary>
public sealed record CompositeQuery(string ReferenceId, string Soql);

/// <summary>
/// An sObject Collections subrequest: creates (POST) or updates (PATCH) up to 200 records, each
/// <c>{ "attributes": { "type": ... }, field: value, ... }</c>, with its <c>id</c> when updating.
/// </summary>
public sealed record CollectionRequest(string ReferenceId, bool Create, IReadOnlyList<IReadOnlyDictionary<string, object?>> Records);

/// <summary>One record's result within a collection, in request order.</summary>
public sealed record RecordResult(bool Success, string? Id, IReadOnlyList<SalesforceError> Errors);

/// <param name="Records">Null when the subrequest failed as a whole (see <paramref name="Errors"/>).</param>
public sealed record CollectionResult(string ReferenceId, IReadOnlyList<RecordResult>? Records, IReadOnlyList<SalesforceError> Errors);