using System.Text.RegularExpressions;

namespace SalesforceSync.FakeSalesforce;

/// <summary>The one kind of query the mock org understands: <c>SELECT a, b FROM Obj WHERE field IN ('x', 'y')</c>.</summary>
internal sealed partial record FakeSoql(string[] Fields, string SObject, string WhereField, HashSet<string> Values)
{
    [GeneratedRegex(@"^\s*SELECT\s+(?<fields>.+?)\s+FROM\s+(?<obj>\w+)\s+WHERE\s+(?<field>\w+)\s+IN\s*\((?<values>.*)\)\s*$",
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
            throw new FakeSalesforceException(400, "MALFORMED_QUERY", $"Fake only supports SELECT ... WHERE x IN (...): {soql}");
        }

        var fields = m.Groups["fields"].Value.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        var sobject = m.Groups["obj"].Value;
        var whereField = m.Groups["field"].Value;
        if (!FakeSchema.Objects.ContainsKey(sobject))
        {
            throw new FakeSalesforceException(400, "INVALID_TYPE", $"sObject type '{sobject}' is not supported.");
        }

        foreach (var f in fields.Append(whereField))
        {
            if (FakeSchema.Find(sobject, f) is null)
            {
                throw new FakeSalesforceException(400, "INVALID_FIELD", $"No such column '{f}' on entity '{sobject}'.");
            }
        }

        var values = QuotedValue().Matches(m.Groups["values"].Value)
            .Select(v => v.Groups[1].Value.Replace("\\'", "'").Replace("\\\\", "\\"))
            .ToHashSet(StringComparer.Ordinal);
        return new FakeSoql(fields, sobject, whereField, values);
    }
}