namespace SalesforceSync.Infrastructure.Salesforce;

// What the client is asked to send. Each operation is a record that knows its own method, path and body, so a new bulk
// operation is a new record here, not a new client method (the client sends any CollectionRequest the same way).

/// <summary>A SOQL query to run as one subrequest of a Composite request.</summary>
public sealed record CompositeQuery(string ReferenceId, string Soql);

/// <summary>
/// One sObject Collections operation of a write request (<see cref="SalesforceRestClient.WriteAsync"/>): a call of its
/// own, or a Composite subrequest named <paramref name="ReferenceId"/>.
/// </summary>
public abstract record CollectionRequest(string ReferenceId)
{
    public abstract HttpMethod Method { get; }

    /// <summary>The path after the data path (<see cref="SalesforceOptions.DataPath"/>), query string included.</summary>
    public abstract string Path { get; }

    /// <summary>Its records: 1 to 200.</summary>
    public abstract int Count { get; }

    /// <summary>The JSON body, or null when it has none.</summary>
    public abstract object? Body { get; }
}

/// <summary>
/// An sObject Collections upsert: up to 200 records of <paramref name="SObject"/>, each
/// <c>{ "attributes": { "type": ... }, externalIdField: ..., field: value, ... }</c>, created or updated by
/// <paramref name="ExternalIdField"/>.
/// </summary>
public sealed record CollectionUpsert(string ReferenceId, string SObject, string ExternalIdField,
    IReadOnlyList<IReadOnlyDictionary<string, object?>> Records) : CollectionRequest(ReferenceId)
{
    public override HttpMethod Method => HttpMethod.Patch;
    public override string Path => $"/composite/sobjects/{SObject}/{ExternalIdField}";
    public override int Count => Records.Count;
    // allOrNone=false: every record succeeds or fails on its own, so one bad record doesn't cost the other 199 (and
    // the call that carried them) a retry
    public override object Body => new { allOrNone = false, records = Records };
}

/// <summary>
/// An sObject Collections delete: up to 200 records, of any objects, by their Salesforce Ids (it takes no external
/// ids), in the URL: <c>DELETE /composite/sobjects?ids=001...,003...&amp;allOrNone=false</c>. Deleted records go to the
/// Recycle Bin. 200 Ids of 18 characters make a URL of under 4 KB, well within <see cref="SalesforceRestClient.MaxUrlBytes"/>.
/// </summary>
public sealed record CollectionDelete(string ReferenceId, IReadOnlyList<string> Ids) : CollectionRequest(ReferenceId)
{
    public override HttpMethod Method => HttpMethod.Delete;

    // Up to 200 Ids in one call, of any objects: a delete is the cheapest write there is. They go in the URL (a DELETE has
    // no body), and 200 18-character Ids stay far below the URL limit. Ids are letters and digits: nothing to escape
    public override string Path => $"/composite/sobjects?ids={string.Join(",", Ids)}&allOrNone=false";
    public override int Count => Ids.Count;
    public override object? Body => null;
}
