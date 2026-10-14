using System.Collections.Concurrent;

namespace SalesforceSync.Salesforce;

/// <summary>
/// Process-wide cache of sObject Describe results. Metadata rarely changes, so each object is described once and then
/// revalidated every <see cref="SalesforceOptions.SchemaRefreshInterval"/> with If-Modified-Since (a 304 costs no
/// download). Concurrent callers share one in-flight fetch per object.
/// </summary>
public sealed class SalesforceSchemaCache(TimeProvider clock)
{
    private sealed record Snapshot(SObjectSchema Schema, DateTimeOffset FetchedAt, DateTimeOffset CheckedAt);

    private sealed class Entry
    {
        public readonly SemaphoreSlim Gate = new(1, 1);
        public volatile Snapshot? State;      // replaced as a whole, so lock-free readers never see a torn update
        public volatile bool Invalidated;
    }

    private readonly ConcurrentDictionary<string, Entry> _entries = new(StringComparer.OrdinalIgnoreCase);

    /// <param name="fetch">Calls Describe; the argument is the If-Modified-Since value (null = unconditional).</param>
    public async Task<SObjectSchema> GetAsync(string sobject, TimeSpan refreshInterval,
        Func<DateTimeOffset?, CancellationToken, Task<DescribeResult>> fetch, CancellationToken ct)
    {
        var entry = _entries.GetOrAdd(sobject, _ => new Entry());
        if (Fresh(entry, refreshInterval) is { } cached) return cached;

        await entry.Gate.WaitAsync(ct);
        try
        {
            if (Fresh(entry, refreshInterval) is { } refreshed) return refreshed; // another caller just refreshed it

            var state = entry.State;
            var invalidated = entry.Invalidated;
            var now = clock.GetUtcNow();
            var result = await fetch(state is not null && !invalidated ? state.FetchedAt : null, ct);

            var schema = result.Schema ?? state?.Schema
                         ?? throw new InvalidOperationException($"Describe of {sobject} returned no schema");
            entry.State = new Snapshot(schema, result.Schema is not null ? now : state!.FetchedAt, now);
            entry.Invalidated = false;
            return schema;
        }
        finally
        {
            entry.Gate.Release();
        }
    }

    /// <summary>Forces a full describe on next use (e.g. Salesforce rejected a field the cached schema has).</summary>
    public void Invalidate(string sobject)
    {
        if (_entries.TryGetValue(sobject, out var entry)) entry.Invalidated = true;
    }

    private SObjectSchema? Fresh(Entry entry, TimeSpan refreshInterval)
    {
        var state = entry.State;
        return state is not null && !entry.Invalidated && clock.GetUtcNow() - state.CheckedAt < refreshInterval
            ? state.Schema
            : null;
    }
}
