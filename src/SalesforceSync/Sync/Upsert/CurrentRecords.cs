using System.Text.Json;
using SalesforceSync.Salesforce;

namespace SalesforceSync.Sync.Upsert;

/// <summary>A query of the batch's read and the object its rows belong to.</summary>
internal sealed record ReadQuery(CompositeQuery Query, string SObject);

/// <summary>
/// Builds the batch's read: the strategy's queries as SOQL, each selecting every field the diff compares and the Ids
/// the lookups need, split into chunks of 100 values.
/// </summary>
internal static class ReadQueryBuilder
{
    /// <summary>External ids per query; with 5 queries per Composite request this is what bounds Sync:BatchSize.</summary>
    internal const int MaxValuesPerQuery = 100;

    /// <summary>
    /// Entities per batch. Not a Salesforce limit of its own: the batch's read must fit one Composite request, i.e. 5
    /// queries of <see cref="MaxValuesPerQuery"/> external ids each, and an entity can need up to 3 ids per 100 (a
    /// relationship: itself and two Accounts). The write side would allow more (5 collections of 200 records).
    /// </summary>
    internal const int MaxEntitiesPerBatch = MaxValuesPerQuery;

    /// <summary>One query per chunk of values, named q0, q1, ...</summary>
    /// <exception cref="InvalidOperationException">More queries than a Composite request takes.</exception>
    public static List<ReadQuery> Build(IReadOnlyList<NormalizedUnit> units, IReadOnlyList<SoqlQuery> queries,
        IReadOnlyDictionary<string, SObjectSchema> schemas)
    {
        var records = units.SelectMany(u => u.Records).ToList();
        var composite = new List<ReadQuery>();

        foreach (var query in queries.Where(q => q.Values.Count > 0))
        {
            var target = schemas[query.SObject];
            var fields = FieldsToSelect(target, query, records);

            foreach (var chunk in query.Values.Distinct(StringComparer.Ordinal).Chunk(MaxValuesPerQuery))
            {
                var soql = SalesforceRestClient.Soql(target.Name, fields, query.WhereField, chunk);
                composite.Add(new ReadQuery(new CompositeQuery($"q{composite.Count}", soql), target.Name));
            }
        }

        return composite.Count > SalesforceRestClient.MaxCompositeQueries
            ? throw new InvalidOperationException(
                $"The strategy's queries need {composite.Count} SOQL queries; a Composite request allows {SalesforceRestClient.MaxCompositeQueries}. Use a smaller Sync:BatchSize.")
            : composite;
    }

    /// <summary>
    /// Every field of <paramref name="target"/> the diff compares, the external ids its records and the lookups to it
    /// are matched by, the field the query filters on, and the Id.
    /// </summary>
    private static List<string> FieldsToSelect(SObjectSchema target, SoqlQuery query, IReadOnlyList<NormalizedRecord> records)
    {
        var ownFields = records.Where(r => r.Schema == target)
            .SelectMany(r => r.Fields.Keys.Select(f => f.Name)
                .Concat(r.Lookups.Select(l => l.Field.Name))
                .Append(r.Record.ExternalIdField));
        var lookedUpBy = records.SelectMany(r => r.Lookups.Select(l => l.Target))
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

    /// <summary>The record a lookup points at, or null if it isn't in Salesforce.</summary>
    public CurrentRecord? Find(SObjectLookup lookup)
    {
        return Find(lookup.SObject, lookup.ExternalIdField, lookup.ExternalId);
    }
}