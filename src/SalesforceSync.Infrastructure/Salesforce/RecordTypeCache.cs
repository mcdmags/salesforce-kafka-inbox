using System.Collections.Concurrent;
using System.Text.Json;
using Microsoft.Extensions.Options;

namespace SalesforceSync.Infrastructure.Salesforce;

/// <summary>
/// The Ids of the active record types of each object, by developer name: Salesforce takes a record type as its Id, which
/// differs between orgs. Read with one SOQL query on RecordType per object, and kept in this process for
/// <see cref="SalesforceOptions.RecordTypeRefreshInterval"/>; concurrent callers share one in-flight read per object.
/// Nothing else of the org's metadata is read.
/// </summary>
public sealed class RecordTypeCache(TimeProvider clock, ISalesforceRestClient salesforce, IOptions<SalesforceOptions> options)
{
    /// <summary>The field that holds a record's record type.</summary>
    public const string RecordTypeIdField = "RecordTypeId";

    private sealed record Snapshot(IReadOnlyDictionary<string, string> Ids, DateTimeOffset ReadAt);

    private sealed class Entry
    {
        public readonly SemaphoreSlim Gate = new(1, 1);
        public volatile Snapshot? State;      // replaced as a whole, so lock-free readers never see a torn update
        public volatile bool Invalidated;
    }

    private readonly ConcurrentDictionary<string, Entry> _entries = new(StringComparer.OrdinalIgnoreCase);
    private readonly TimeSpan _refreshInterval = options.Value.RecordTypeRefreshInterval;

    /// <summary>The Id of the object's active record type with this developer name, or null if the org has none.</summary>
    public async Task<string?> IdOfAsync(string sobject, string developerName, CancellationToken ct)
    {
        var entry = _entries.GetOrAdd(sobject, _ => new Entry());
        var state = Fresh(entry);
        if (state is null)
        {
            await entry.Gate.WaitAsync(ct);
            try
            {
                state = Fresh(entry) ?? await ReadAsync(sobject, entry, ct);
            }
            finally
            {
                entry.Gate.Release();
            }
        }

        return state.Ids.GetValueOrDefault(developerName);
    }

    /// <summary>The object's record types are read again the next time they are needed (one was missing or refused).</summary>
    public void Invalidate(string sobject)
    {
        if (_entries.TryGetValue(sobject, out var entry))
        {
            entry.Invalidated = true;
        }
    }

    private Snapshot? Fresh(Entry entry)
    {
        return entry.State is { } state && !entry.Invalidated && clock.GetUtcNow() - state.ReadAt < _refreshInterval ? state : null;
    }

    private async Task<Snapshot> ReadAsync(string sobject, Entry entry, CancellationToken ct)
    {
        var rows = await salesforce.SelectAsync(
            SalesforceRestClient.Soql("RecordType", ["Id", "DeveloperName", "IsActive"], "SobjectType", [sobject]), ct);
        var ids = rows
            .Where(r => r.GetProperty("IsActive").ValueKind == JsonValueKind.True)
            .DistinctBy(r => r.GetProperty("DeveloperName").GetString(), StringComparer.OrdinalIgnoreCase) // unique per namespace only: the first is kept
            .ToDictionary(r => r.GetProperty("DeveloperName").GetString()!, r => r.GetProperty("Id").GetString()!, StringComparer.OrdinalIgnoreCase);
        var state = new Snapshot(ids, clock.GetUtcNow());
        entry.State = state;
        entry.Invalidated = false;
        return state;
    }
}
