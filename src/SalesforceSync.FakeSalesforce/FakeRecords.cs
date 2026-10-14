using System.Text.Json.Nodes;

namespace SalesforceSync.FakeSalesforce;

/// <summary>
/// The mock org's data: records by object and Id, and per record the stored values of every committed write, oldest
/// first. Not thread-safe; <see cref="FakeSalesforceStore"/> holds its lock around every use.
/// </summary>
internal sealed class FakeRecords
{
    private readonly Dictionary<string, Dictionary<string, JsonObject>> _tables = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, List<JsonObject>> _history = new(StringComparer.Ordinal); // record Id -> writes
    private long _idCounter;

    public int Count => _tables.Values.Sum(t => t.Count);

    /// <summary>The object's records by Id.</summary>
    public Dictionary<string, JsonObject> Table(string sobject)
    {
        if (!_tables.TryGetValue(sobject, out var t))
        {
            _tables[sobject] = t = new Dictionary<string, JsonObject>(StringComparer.Ordinal);
        }

        return t;
    }

    public JsonObject? Find(string sobject, string field, string? value)
    {
        return Table(sobject).Values.FirstOrDefault(r => FakeJson.Str(r[field]) == value);
    }

    public List<JsonObject> HistoryOf(string id)
    {
        return _history[id];
    }

    /// <summary>A valid 18-character id: key prefix, "FAKE", a counter, and the suffix that encodes their case.</summary>
    public string NewId(string sobject)
    {
        return Id18($"{FakeSchema.KeyPrefixOf(sobject)}FAKE{++_idCounter:D8}");
    }

    /// <summary>The 18-character form of a 15-character id: the id and the suffix that encodes its case.</summary>
    public static string Id18(string id)
    {
        const string alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ012345";
        var suffix = new char[3];
        for (var chunk = 0; chunk < 3; chunk++)
        {
            var bits = 0;
            for (var i = 0; i < 5; i++)
            {
                if (char.IsAsciiLetterUpper(id[(chunk * 5) + i]))
                {
                    bits |= 1 << i;
                }
            }
            suffix[chunk] = alphabet[bits];
        }
        return id + new string(suffix);
    }

    public void Insert(string sobject, string id, JsonObject record, JsonObject storedValues)
    {
        Table(sobject)[id] = record;
        _history[id] = [storedValues];
    }

    public void Update(string sobject, string id, JsonObject storedValues)
    {
        var existing = Table(sobject)[id];
        foreach (var (k, v) in storedValues)
        {
            existing[k] = v?.DeepClone();
        }

        existing["LastModifiedDate"] = FakeSchema.FormatDateTime(DateTimeOffset.UtcNow);
        _history[id].Add(storedValues);
    }

    public void Clear()
    {
        _tables.Clear();
        _history.Clear();
        _idCounter = 0;
    }
}