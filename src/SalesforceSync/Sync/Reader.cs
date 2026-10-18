using System.Text.Json;
using SalesforceSync.Infrastructure.Salesforce;
using static SalesforceSync.Sync.Normalizer;

namespace SalesforceSync.Sync;

/// <summary>
/// Step 3. Reads what Salesforce has now for the records of the batch: the queries (chunked to fit a URL), one call per
/// object, and the rows indexed for the diff.
/// </summary>
internal sealed class Reader(ISalesforceRestClient salesforce, SalesforceSchemaCache schemaCache)
{
    /// <summary>A query of the batch's read and the object its rows belong to.</summary>
    internal sealed record ReadQuery(string Soql, string SObject);

    /// <summary>
    /// The strategy's queries as SOQL, each selecting every field the diff compares, the Id (a delete needs it) and the
    /// external ids that show a parent exists, split into chunks of at most <paramref name="maxValuesPerQuery"/> values
    /// (the row type's) whose URL fits a call (<see cref="SalesforceRestClient.MaxUrlBytes"/>).
    /// </summary>
    /// <exception cref="InvalidOperationException">A query's URL is too long even with a single value.</exception>
    internal List<ReadQuery> BuildReadQueries(IReadOnlyList<NormalizedUnit> units, IReadOnlyList<SoqlQuery> queries,
        IReadOnlyDictionary<string, SObjectSchema> schemas, int maxValuesPerQuery)
    {
        var records = units.SelectMany(u => u.Records).ToList();
        var read = new List<ReadQuery>();

        foreach (var query in queries.Where(q => q.Values.Count > 0))
        {
            var target = schemas[query.SObject];

            // What the diff compares, the external ids its records and the foreign keys to it are matched by, the field the
            // query filters on, and the Id
            var fields = records.Where(r => r.Schema == target)
                .SelectMany(r => r.Fields.Keys.Select(f => f.Name).Append(r.Record.ExternalIdField))
                .Concat(records.SelectMany(r => r.Record.Parents)
                    .Where(p => target.Name.Equals(p.SObject, StringComparison.OrdinalIgnoreCase)).Select(p => p.ExternalIdField))
                .Append(query.WhereField)
                .Append("Id")
                .Select(f => target.Find(f)?.Name ?? f)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();

            read.AddRange(Chunked(target.Name, fields, query.WhereField, query.Values.Distinct(StringComparer.Ordinal), maxValuesPerQuery)
                .Select(soql => new ReadQuery(soql, target.Name)));
        }

        return read;
    }

    /// <summary>
    /// The values in order as queries of at most <paramref name="maxValues"/> values: a query takes the next value while
    /// its URL, as the client sends it, still fits. Percent-encoding works character by character, so a URL's length is the
    /// empty query's plus each value's encoded literal and separator: counted per value, not rebuilt.
    /// </summary>
    private IEnumerable<string> Chunked(string sobject, List<string> fields, string whereField, IEnumerable<string> values, int maxValues)
    {
        var empty = SalesforceRestClient.QueryUrl(salesforce.DataPath, SalesforceRestClient.Soql(sobject, fields, whereField, [])).Length;
        var separator = Uri.EscapeDataString(",").Length;
        var chunk = new List<string>();
        var length = empty;
        foreach (var value in values)
        {
            var literal = Uri.EscapeDataString(SalesforceRestClient.SoqlLiteral(value)).Length;
            if (chunk.Count > 0 && chunk.Count < maxValues && length + separator + literal <= SalesforceRestClient.MaxUrlBytes)
            {
                chunk.Add(value);
                length += separator + literal;
                continue;
            }

            if (chunk.Count > 0)
            {
                yield return SalesforceRestClient.Soql(sobject, fields, whereField, chunk);
            }

            chunk = [value];
            length = empty + literal;
            if (length > SalesforceRestClient.MaxUrlBytes)
            {
                throw new InvalidOperationException(
                    $"The read of {sobject} by {whereField} does not fit a URL ({SalesforceRestClient.MaxUrlBytes:N0} bytes) " +
                    $"even for one value ({value.Length:N0} characters) and its {fields.Count} fields");
            }
        }

        if (chunk.Count > 0)
        {
            yield return SalesforceRestClient.Soql(sobject, fields, whereField, chunk);
        }
    }

    internal async Task<CurrentRecords> ReadCurrentAsync(IReadOnlyList<ReadQuery> queries, IEnumerable<string> describedSObjects,
        CancellationToken ct)
    {
        var rows = new List<(string SObject, JsonElement Row)>();
        try
        {
            foreach (var query in queries)
            {
                rows.AddRange((await salesforce.SelectAsync(query.Soql, ct)).Select(row => (query.SObject, row)));
            }
        }
        catch (SalesforceResponseException ex) when (ex.ErrorCode == "INVALID_FIELD")
        {
            // The cached describe is out of date (a field was removed or renamed): describe again next time
            await InvalidateAsync(describedSObjects, ct);
            throw;
        }

        return new CurrentRecords(rows);
    }

    /// <summary>A record as Salesforce has it now: the fields the read selected, by API name (case-insensitive).</summary>
    internal sealed class CurrentRecord(Dictionary<string, JsonElement> fields)
    {
        /// <summary>The Id as Salesforce returned it; a delete addresses the record by it.</summary>
        public string Id => fields["Id"].GetString()!;

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

        /// <summary>The rows a read found, each with the object its query was for.</summary>
        public CurrentRecords(IEnumerable<(string SObject, JsonElement Row)> rows)
        {
            foreach (var (sobject, row) in rows)
            {
                var record = new CurrentRecord(row.EnumerateObject().Where(p => p.Name != "attributes")
                    .ToDictionary(p => p.Name, p => p.Value, StringComparer.OrdinalIgnoreCase));
                foreach (var (field, value) in record.Fields.Where(f => f.Value.ValueKind == JsonValueKind.String))
                {
                    _index.TryAdd((sobject.ToLowerInvariant(), field.ToLowerInvariant(), value.GetString()!), record);
                }
            }
        }

        public CurrentRecord? Find(string sobject, string field, string value)
        {
            return _index.GetValueOrDefault((sobject.ToLowerInvariant(), field.ToLowerInvariant(), value));
        }
    }

    private async Task InvalidateAsync(IEnumerable<string> sobjects, CancellationToken ct)
    {
        foreach (var sobject in sobjects)
        {
            await schemaCache.InvalidateAsync(sobject, ct);
        }
    }
}
