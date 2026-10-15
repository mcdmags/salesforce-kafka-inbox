using System.Text.Json;
using SalesforceSync.Salesforce;

namespace SalesforceSync.Sync.Upsert;

/// <summary>A query of the batch's read and the object its rows belong to.</summary>
internal sealed record ReadQuery(CompositeQuery Query, string SObject);

/// <summary>
/// Builds the batch's read: the strategy's queries as SOQL, each selecting every field the diff compares and the Ids
/// the lookups need (or the external ids that show a parent exists), split into chunks of at most 100 values whose
/// URL fits a call (<see cref="SalesforceRestClient.MaxUrlBytes"/>).
/// </summary>
internal static class ReadQueryBuilder
{
    /// <summary>External ids per query, at most; fewer when long ids would make its URL too long.</summary>
    internal const int MaxValuesPerQuery = 100;

    /// <summary>
    /// Entities per batch. Not a Salesforce limit of its own: it keeps a batch's read to one Composite request while
    /// the ids are short, i.e. 5 queries of <see cref="MaxValuesPerQuery"/> external ids each, where an entity can need
    /// up to 3 ids per 100 (a relationship: itself and two Accounts). Ids long enough to split the queries further
    /// cost more read calls, not a failure. The write side would allow more (5 collections of 200 records).
    /// </summary>
    internal const int MaxEntitiesPerBatch = MaxValuesPerQuery;

    /// <summary>
    /// One query per chunk of values, named q0, q1, ...; more than one Composite request takes (5) are sent as several.
    /// </summary>
    /// <exception cref="InvalidOperationException">A query's URL is too long even with a single value.</exception>
    public static List<ReadQuery> Build(IReadOnlyList<NormalizedUnit> units, IReadOnlyList<SoqlQuery> queries,
        IReadOnlyDictionary<string, SObjectSchema> schemas)
    {
        var records = units.SelectMany(u => u.Records).ToList();
        var composite = new List<ReadQuery>();

        foreach (var query in queries.Where(q => q.Values.Count > 0))
        {
            var target = schemas[query.SObject];
            var fields = FieldsToSelect(target, query, records);

            foreach (var soql in Chunked(target.Name, fields, query.WhereField, query.Values.Distinct(StringComparer.Ordinal)))
            {
                composite.Add(new ReadQuery(new CompositeQuery($"q{composite.Count}", soql), target.Name));
            }
        }

        return composite;
    }

    /// <summary>
    /// The values in order as queries of at most <see cref="MaxValuesPerQuery"/> values: a query takes the next value
    /// while its URL, built as the client sends it, still fits.
    /// </summary>
    private static IEnumerable<string> Chunked(string sobject, List<string> fields, string whereField, IEnumerable<string> values)
    {
        var chunk = new List<string>();
        string? soql = null; // chunk's query
        foreach (var value in values)
        {
            if (chunk.Count < MaxValuesPerQuery
                && SalesforceRestClient.Soql(sobject, fields, whereField, [.. chunk, value]) is var longer
                && SalesforceRestClient.FitsUrlLimit(SalesforceRestClient.QueryUrl(longer)))
            {
                chunk.Add(value);
                soql = longer;
                continue;
            }

            if (soql is not null)
            {
                yield return soql;
            }

            chunk = [value];
            soql = SalesforceRestClient.Soql(sobject, fields, whereField, chunk);
            if (!SalesforceRestClient.FitsUrlLimit(SalesforceRestClient.QueryUrl(soql)))
            {
                throw new InvalidOperationException(
                    $"The read of {sobject} by {whereField} does not fit a URL ({SalesforceRestClient.MaxUrlBytes:N0} bytes) " +
                    $"even for one value ({value.Length:N0} characters) and its {fields.Count} fields");
            }
        }

        if (soql is not null)
        {
            yield return soql;
        }
    }

    /// <summary>
    /// Every field of <paramref name="target"/> the diff compares, the external ids its records and the lookups and
    /// foreign keys to it are matched by, the field the query filters on, and the Id.
    /// </summary>
    private static List<string> FieldsToSelect(SObjectSchema target, SoqlQuery query, IReadOnlyList<NormalizedRecord> records)
    {
        var ownFields = records.Where(r => r.Schema == target)
            .SelectMany(r => r.Fields.Keys.Select(f => f.Name)
                .Concat(r.Lookups.Select(l => l.Field.Name))
                .Append(r.Record.ExternalIdField));
        var lookedUpBy = records.SelectMany(r => r.Record.Parents)
            .Where(l => target.Name.Equals(l.SObject, StringComparison.OrdinalIgnoreCase))
            .Select(l => l.ExternalIdField);

        return [.. ownFields.Concat(lookedUpBy)
            .Append(query.WhereField)
            .Append("Id")
            .Select(f => target.Find(f)?.Name ?? f)
            .Distinct(StringComparer.OrdinalIgnoreCase)];
    }
}

/// <summary>A record as Salesforce has it now: the fields the read selected, by API name (case-insensitive).</summary>
internal sealed class CurrentRecord(Dictionary<string, JsonElement> fields)
{
    /// <summary>The Id as Salesforce returned it; an update addresses the record by it.</summary>
    public string Id => fields["Id"].GetString()!;

    /// <summary>The Id in its 18-character form, which is how lookup values are compared and sent.</summary>
    public string? Id18 => FieldValues.To18CharId(Id);

    public IEnumerable<KeyValuePair<string, JsonElement>> Fields => fields;

    public bool TryGetValue(string field, out JsonElement value)
    {
        return fields.TryGetValue(field, out value);
    }
}

/// <summary>Rows read from Salesforce, indexed by every text field that was selected (external ids, Id).</summary>
internal sealed class CurrentRecords
{
    public static readonly CurrentRecords None = new([]);

    private readonly Dictionary<(string SObject, string Field, string Value), CurrentRecord> _index = [];

    /// <summary>The rows of a Composite read (by query reference id), each query's rows belonging to one object.</summary>
    public static CurrentRecords From(IReadOnlyDictionary<string, List<JsonElement>> rowsByQuery, IEnumerable<ReadQuery> queries)
    {
        var sobjectOf = queries.ToDictionary(q => q.Query.ReferenceId, q => q.SObject, StringComparer.Ordinal);
        return new CurrentRecords(rowsByQuery.SelectMany(q => q.Value.Select(row => (sobjectOf[q.Key], new CurrentRecord(row.EnumerateObject()
            .Where(p => p.Name != "attributes")
            .ToDictionary(p => p.Name, p => p.Value, StringComparer.OrdinalIgnoreCase))))));
    }

    public CurrentRecords(IEnumerable<(string SObject, CurrentRecord Record)> rows)
    {
        foreach (var (sobject, record) in rows)
        {
            var sobjectKey = sobject.ToLowerInvariant();
            foreach (var (field, value) in record.Fields)
            {
                if (value.ValueKind == JsonValueKind.String)
                {
                    _index.TryAdd((sobjectKey, field.ToLowerInvariant(), value.GetString()!), record);
                }
            }
        }
    }

    public CurrentRecord? Find(string sobject, string field, string value)
    {
        return _index.GetValueOrDefault((sobject.ToLowerInvariant(), field.ToLowerInvariant(), value));
    }

    /// <summary>The record as Salesforce has it, or null if it doesn't exist yet.</summary>
    public CurrentRecord? Find(SObjectRecord record)
    {
        return Find(record.SObject, record.ExternalIdField, record.ExternalId);
    }

    /// <summary>The record a lookup or foreign key points at, or null if it isn't in Salesforce.</summary>
    public CurrentRecord? Find(SObjectLookup lookup)
    {
        return Find(lookup.SObject, lookup.ExternalIdField, lookup.ExternalId);
    }
}