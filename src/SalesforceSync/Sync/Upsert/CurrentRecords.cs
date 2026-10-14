using System.Text.Json;
using SalesforceSync.Salesforce;

namespace SalesforceSync.Sync.Upsert;

/// <summary>A record as Salesforce has it now: the fields the read selected, by API name (case-insensitive).</summary>
internal sealed class CurrentRecord(Dictionary<string, JsonElement> fields)
{
    /// <summary>The Id as Salesforce returned it; an update addresses the record by it.</summary>
    public string Id => fields["Id"].GetString()!;

    /// <summary>The Id in its 18-character form, which is how lookup values are compared and sent.</summary>
    public string? Id18 => FieldValues.To18CharId(Id);

    public IEnumerable<KeyValuePair<string, JsonElement>> Fields => fields;

    public bool TryGetValue(string field, out JsonElement value) => fields.TryGetValue(field, out value);
}

/// <summary>Rows read from Salesforce, indexed by every text field that was selected (external ids, Id).</summary>
internal sealed class CurrentRecords
{
    public static readonly CurrentRecords None = new([]);

    private readonly Dictionary<(string SObject, string Field, string Value), CurrentRecord> _index = new();

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
                if (value.ValueKind == JsonValueKind.String)
                    _index.TryAdd((sobjectKey, field.ToLowerInvariant(), value.GetString()!), record);
        }
    }

    public CurrentRecord? Find(string sobject, string field, string value)
        => _index.GetValueOrDefault((sobject.ToLowerInvariant(), field.ToLowerInvariant(), value));

    /// <summary>The record as Salesforce has it, or null if it doesn't exist yet.</summary>
    public CurrentRecord? Find(SObjectRecord record) => Find(record.SObject, record.ExternalIdField, record.ExternalId);

    /// <summary>The record a lookup points at, or null if it isn't in Salesforce.</summary>
    public CurrentRecord? Find(SObjectLookup lookup) => Find(lookup.SObject, lookup.ExternalIdField, lookup.ExternalId);
}
