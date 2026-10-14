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

        if (composite.Count > SalesforceRestClient.MaxCompositeQueries)
            throw new InvalidOperationException(
                $"The strategy's queries need {composite.Count} SOQL queries; a Composite request allows {SalesforceRestClient.MaxCompositeQueries}. Use a smaller Sync:BatchSize.");
        return composite;
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

        return ownFields.Concat(lookedUpBy)
            .Append(query.WhereField)
            .Append("Id")
            .Select(f => target.Find(f)?.Name ?? f)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();
    }
}
