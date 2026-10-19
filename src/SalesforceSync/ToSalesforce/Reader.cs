using System.Text.Json;
using System.Text.RegularExpressions;
using SalesforceSync.Infrastructure.Salesforce;
using static SalesforceSync.ToSalesforce.Normalizer;

namespace SalesforceSync.ToSalesforce;

/// <summary>
/// Step 3. Reads what Salesforce has now for the batch's records: the queries (chunked to fit a URL), one call per
/// object, and the rows indexed for the diff.
/// </summary>
internal sealed partial class Reader(ISalesforceRestClient salesforce)
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
        int maxValuesPerQuery)
    {
        var records = units.SelectMany(u => u.Records).ToList();
        var read = new List<ReadQuery>();

        foreach (var query in queries.Where(q => q.Values.Count > 0))
        {
            // What the diff compares, the external ids its records and the foreign keys to it are matched by, the field the
            // query filters on, and the Id
            var fields = records.Where(r => r.Record.SObject.Equals(query.SObject, StringComparison.OrdinalIgnoreCase))
                .SelectMany(r => r.Fields.Keys.Append(r.Record.ExternalIdField))
                .Concat(records.SelectMany(r => r.Record.Parents)
                    .Where(p => query.SObject.Equals(p.SObject, StringComparison.OrdinalIgnoreCase)).Select(p => p.ExternalIdField))
                .Append(query.WhereField)
                .Append("Id")
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();

            read.AddRange(Chunked(query.SObject, fields, query.WhereField, query.Values.Distinct(StringComparer.Ordinal), maxValuesPerQuery)
                .Select(soql => new ReadQuery(soql, query.SObject)));
        }

        return read;
    }

    /// <summary>
    /// The values in order as queries of at most <paramref name="maxValues"/> values: a query takes the next value while
    /// its URL, as the client sends it, still fits. Percent-encoding works character by character, so a URL's length is
    /// the empty query's plus each value's encoded literal and separator: counted per value, not rebuilt.
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

    /// <summary>What Salesforce has now for the records the queries ask for.</summary>
    /// <exception cref="MappingErrorException">
    /// Salesforce has no such field on an object (INVALID_FIELD). The diff needs the fields the code sends, so a wrong
    /// mapping shows here first, before anything is written. Names the entities that have a record of that object.
    /// </exception>
    internal async Task<CurrentRecords> ReadCurrentAsync(IReadOnlyList<ReadQuery> queries, IReadOnlyList<NormalizedUnit> units,
        CancellationToken ct)
    {
        var rows = new List<(string SObject, JsonElement Row)>();
        foreach (var query in queries)
        {
            try
            {
                rows.AddRange((await salesforce.SelectAsync(query.Soql, ct)).Select(row => (query.SObject, row)));
            }
            catch (SalesforceResponseException ex) when (ex.ErrorCode == "INVALID_FIELD")
            {
                throw new MappingErrorException([.. UnitsWithField(units, query.SObject, ex.Message)
                    .Select(u => (u.Key, new SalesforceError(u.Records[0].ReferenceId, "INVALID_FIELD", ex.Message)))]);
            }
        }

        return new CurrentRecords(rows);
    }

    /// <summary>
    /// The units whose records of the object send the column Salesforce names in its "No such column 'X'" message; all
    /// of the object's when the message names none of them.
    /// </summary>
    private static List<NormalizedUnit> UnitsWithField(IReadOnlyList<NormalizedUnit> units, string sobject, string message)
    {
        var ofObject = units.Where(u => u.Records.Any(r => r.Record.SObject.Equals(sobject, StringComparison.OrdinalIgnoreCase))).ToList();
        var column = NoSuchColumn().Match(message) is { Success: true } m ? m.Groups["column"].Value : null;
        var named = ofObject.Where(u => u.Records.Any(r => r.Fields.ContainsKey(column ?? ""))).ToList();
        return named.Count > 0 ? named : ofObject;
    }

    [GeneratedRegex(@"No such column (?:'|\\u0027)(?<column>[^'\\]+)")]
    private static partial Regex NoSuchColumn();

    /// <summary>A record as Salesforce has it now: the fields the read selected, by API name (case-insensitive).</summary>
    internal sealed class CurrentRecord(Dictionary<string, JsonElement> fields)
    {
        /// <summary>The Id as Salesforce returned it. A delete addresses the record by it.</summary>
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
}
