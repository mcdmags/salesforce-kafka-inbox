using System.Collections.Concurrent;

namespace SalesforceSync.Salesforce;

/// <summary>A field value written to a record (JSON as sent).</summary>
public sealed record WriteEcho(string Object, string ExternalId, string Field, string Value);

/// <summary>
/// Detects "flapping" fields: the sync writes a value, Salesforce stores it differently, so the next identical event
/// sees a difference and writes it again, forever: an extra write per event and a field that never matches.
/// For example, with a before-save flow in the org that adds "https://" to Account.Website:
///   1. An event has Website = "example.com". The sync writes it; Salesforce stores "https://example.com".
///   2. The next event has "example.com" again. The read shows "https://example.com", so the sync writes it again.
///   3. Salesforce stores "https://example.com" again: same value sent, same different value stored, twice in a row.
/// Causes are org automation (a trigger, flow or workflow field update) that rewrites the value, or a storage rule
/// <see cref="FieldValues"/> doesn't model. Rules it does model can't flap: Amount = 12.345 on a 2-decimal field is
/// rounded and sent as 12.35, which then matches. A field counts as flapping when, after two consecutive identical
/// writes, Salesforce held the same different value both times. A person editing the record in between doesn't
/// trigger it. Per process and bounded; it only feeds a warning.
/// </summary>
public sealed class WriteEchoTracker
{
    private const int MaxEntries = 50_000;

    private sealed record Entry(string Sent, string? StoredAfterWrite);

    private readonly ConcurrentDictionary<(string, string, string), Entry> _entries = new();

    /// <summary>Called when <paramref name="echo"/>'s field differs from Salesforce's <paramref name="stored"/> value.</summary>
    public bool IsFlapping(WriteEcho echo, string stored)
    {
        var key = (echo.Object, echo.ExternalId, echo.Field);
        if (!_entries.TryGetValue(key, out var entry) || entry.Sent != echo.Value)
        {
            return false;
        }

        if (entry.StoredAfterWrite == stored)
        {
            return true;
        }

        _entries[key] = entry with { StoredAfterWrite = stored };
        return false;
    }

    /// <summary>Remembers the values of a successful write.</summary>
    public void Record(IEnumerable<WriteEcho> written)
    {
        if (_entries.Count > MaxEntries)
        {
            _entries.Clear();
        }

        foreach (var echo in written)
        {
            var key = (echo.Object, echo.ExternalId, echo.Field);
            _entries.AddOrUpdate(key, new Entry(echo.Value, null),
                (_, old) => old.Sent == echo.Value ? old : new Entry(echo.Value, null));
        }
    }
}