using System.Text.Json.Nodes;

namespace SalesforceSync.FakeSalesforce;

/// <summary>
/// The mock org's data: records by object and Id, per record the stored values of every committed write, oldest
/// first, and the Ids of deleted records. Not thread-safe; <see cref="FakeSalesforceStore"/> holds its lock around
/// every use.
/// </summary>
internal sealed class FakeRecords
{
    private readonly Dictionary<string, Dictionary<string, JsonObject>> _tables = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, List<JsonObject>> _history = new(StringComparer.Ordinal); // record Id -> writes
    private readonly HashSet<string> _deleted = new(StringComparer.Ordinal); // the Recycle Bin, by record Id
    private long _idCounter;

    public int Count => _tables.Values.Sum(t => t.Count);

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

    /// <summary>
    /// The 18-character form of a well-formed Id, or null: 15 letters and digits, or 18 whose last 3 are the suffix of
    /// the first 15, compared case-insensitively (SOAP API Developer Guide, StatusCode MALFORMED_ID).
    /// </summary>
    public static string? WellFormedId(string id)
    {
        if (id.Length is not (15 or 18) || !id.All(char.IsAsciiLetterOrDigit))
        {
            return null;
        }

        var full = Id18(id[..15]);
        return id.Length == 15 || full[15..].Equals(id[15..], StringComparison.OrdinalIgnoreCase) ? full : null;
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

    /// <summary>The object and the record with this Id, or null if there is none (deleted ones included).</summary>
    public (string SObject, JsonObject Record)? FindById(string id)
    {
        foreach (var (sobject, table) in _tables)
        {
            if (table.TryGetValue(id, out var record))
            {
                return (sobject, record);
            }
        }

        return null;
    }

    /// <summary>Moves the record to the Recycle Bin: queries and upserts no longer see it, and its Id is known as deleted.</summary>
    public void Delete(string sobject, string id)
    {
        Table(sobject).Remove(id);
        _history.Remove(id);
        _deleted.Add(id);
    }

    public bool IsDeleted(string id)
    {
        return _deleted.Contains(id);
    }

    public void Clear()
    {
        _tables.Clear();
        _history.Clear();
        _deleted.Clear();
        _idCounter = 0;
    }

    /// <summary>A deep copy of the data, for a write that may have to roll back (allOrNone true): see <see cref="Restore"/>.</summary>
    public Saved Save()
    {
        return new Saved(
            _tables.ToDictionary(t => t.Key, t => t.Value.ToDictionary(r => r.Key, r => r.Value.DeepClone().AsObject())),
            _history.ToDictionary(h => h.Key, h => h.Value.Select(w => w.DeepClone().AsObject()).ToList()),
            [.. _deleted]);
    }

    /// <summary>
    /// Puts back the data as <see cref="Save"/> copied it: every write since is undone. Ids are not handed out again,
    /// as Salesforce never reuses one.
    /// </summary>
    public void Restore(Saved s)
    {
        _tables.Clear();
        _history.Clear();
        _deleted.Clear();
        foreach (var (sobject, table) in s.Tables)
        {
            _tables[sobject] = new Dictionary<string, JsonObject>(table, StringComparer.Ordinal);
        }

        foreach (var (id, writes) in s.History)
        {
            _history[id] = writes;
        }

        _deleted.UnionWith(s.Deleted);
    }

    /// <summary>The data at a <see cref="Save"/>.</summary>
    public sealed record Saved(Dictionary<string, Dictionary<string, JsonObject>> Tables, Dictionary<string, List<JsonObject>> History,
        List<string> Deleted);
}