using System.Text.RegularExpressions;

namespace SalesforceSync.FakeSalesforce;

/// <summary>
/// The queries the mock org understands: <c>SELECT a, b FROM Obj WHERE field IN ('x', 'y')</c> or
/// <c>... WHERE field != null</c> (<see cref="Values"/> null: any value), optionally followed by
/// <c>ORDER BY field [ASC|DESC]</c> and <c>LIMIT n</c>.
/// </summary>
internal sealed partial record FakeSoql(string[] Fields, string SObject, string WhereField, HashSet<string>? Values)
{
    /// <summary>The field rows are sorted by (as text), or null to keep the store's order.</summary>
    public string? OrderBy { get; init; }

    public bool Descending { get; init; }

    /// <summary>Most rows to return, or null for all.</summary>
    public int? Limit { get; init; }

    [GeneratedRegex(@"^\s*SELECT\s+(?<fields>.+?)\s+FROM\s+(?<obj>\w+)\s+WHERE\s+(?<field>\w+)\s+(?:IN\s*\((?<values>.*)\)|(?<notNull>!=\s*null))(?:\s+ORDER\s+BY\s+(?<order>\w+)(?:\s+(?<dir>ASC|DESC))?)?(?:\s+LIMIT\s+(?<limit>\d+))?\s*$",
        RegexOptions.IgnoreCase | RegexOptions.Singleline)]
    private static partial Regex Select();

    [GeneratedRegex(@"'((?:\\.|[^'\\])*)'")]
    private static partial Regex QuotedValue();

    /// <exception cref="FakeSalesforceException">Not such a query, or it names an object or field the schema doesn't have.</exception>
    public static FakeSoql Parse(string soql)
    {
        var m = Select().Match(soql);
        if (!m.Success)
        {
            throw new FakeSalesforceException(400, "MALFORMED_QUERY", $"Fake only supports SELECT ... WHERE x IN (...) or x != null, then ORDER BY and LIMIT: {soql}");
        }

        var fields = m.Groups["fields"].Value.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        var sobject = m.Groups["obj"].Value;
        var whereField = m.Groups["field"].Value;
        var isRecordType = sobject.Equals(FakeSchema.RecordTypeObject, StringComparison.OrdinalIgnoreCase);
        if (!isRecordType && !FakeSchema.Objects.ContainsKey(sobject))
        {
            throw new FakeSalesforceException(400, "INVALID_TYPE", $"sObject type '{sobject}' is not supported.");
        }

        var orderBy = m.Groups["order"].Success ? m.Groups["order"].Value : null;
        foreach (var f in fields.Append(whereField).Concat(orderBy is null ? [] : [orderBy]))
        {
            if (isRecordType ? !FakeSchema.RecordTypeFields.Contains(f, StringComparer.OrdinalIgnoreCase) : FakeSchema.Find(sobject, f) is null)
            {
                throw new FakeSalesforceException(400, "INVALID_FIELD", $"No such column '{f}' on entity '{sobject}'.");
            }
        }

        var values = m.Groups["notNull"].Success
            ? null
            : QuotedValue().Matches(m.Groups["values"].Value)
                .Select(v => v.Groups[1].Value.Replace("\\'", "'").Replace("\\\\", "\\"))
                .ToHashSet(StringComparer.Ordinal);
        return new FakeSoql(fields, sobject, whereField, values)
        {
            OrderBy = orderBy,
            Descending = m.Groups["dir"].Value.Equals("DESC", StringComparison.OrdinalIgnoreCase),
            Limit = m.Groups["limit"].Success ? int.Parse(m.Groups["limit"].Value) : null
        };
    }
}