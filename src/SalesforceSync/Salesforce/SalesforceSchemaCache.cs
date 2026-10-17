using System.Collections.Concurrent;
using System.Text.Json;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Options;

namespace SalesforceSync.Salesforce;

/// <summary>
/// Cache of sObject Describe results (fields and record types). The sync needs Describe for change-only writes: values
/// are compared in the form Salesforce stores them (scale, digits, date/time precision, checkbox null as false, text
/// length, required), a record is checked before any call (unknown or read-only fields, lookups), the read selects
/// the fields that exist, and record types resolve to their Ids. Metadata rarely changes, so each object is described
/// once and then revalidated every <see cref="SalesforceOptions.SchemaRefreshInterval"/> with If-Modified-Since (a 304
/// costs no download, but is still an API call). Two layers:
/// <list type="number">
/// <item>In process: the snapshot each worker reads without I/O; concurrent callers share one in-flight fetch per
/// object.</item>
/// <item>Shared by all pods (<see cref="IDistributedCache"/>: Redis, or memory when none is configured): a pod whose
/// own snapshot is missing or stale takes another pod's if that one was checked within the refresh interval, so the
/// org is described about once per interval in total, and a new pod starts warm. An older entry still serves as the
/// basis of If-Modified-Since. The shared layer only saves calls: when it is unreachable or holds something
/// unreadable, the pod describes as if it were empty.</item>
/// </list>
/// </summary>
public sealed class SalesforceSchemaCache(
    TimeProvider clock,
    IDistributedCache shared,
    IOptions<SalesforceOptions> options,
    ILogger<SalesforceSchemaCache> log)
{
    /// <summary>A shared-cache call that takes longer is abandoned and the pod describes on its own.</summary>
    internal static readonly TimeSpan SharedCacheTimeout = TimeSpan.FromSeconds(2);

    /// <summary>
    /// How long a shared entry is kept. Much longer than the refresh interval: an entry too old to be trusted still
    /// gives If-Modified-Since its date, so a pod that starts after a quiet night gets a 304 rather than a download.
    /// </summary>
    internal static readonly TimeSpan SharedEntryLifetime = TimeSpan.FromDays(1);

    private static readonly TimeSpan WarningInterval = TimeSpan.FromMinutes(1);

    private sealed record Snapshot(SObjectSchema Schema, DateTimeOffset FetchedAt, DateTimeOffset CheckedAt);

    /// <summary>A snapshot as stored in the shared cache (JSON).</summary>
    private sealed record SharedSnapshot(string Name, List<SalesforceField> Fields, List<SalesforceRecordType> RecordTypes,
        DateTimeOffset FetchedAt, DateTimeOffset CheckedAt);

    private sealed class Entry
    {
        public readonly SemaphoreSlim Gate = new(1, 1);
        public volatile Snapshot? State;      // replaced as a whole, so lock-free readers never see a torn update
        public volatile bool Invalidated;
    }

    private readonly ConcurrentDictionary<string, Entry> _entries = new(StringComparer.OrdinalIgnoreCase);

    private readonly string _keyPrefix = KeyPrefix(options.Value.InstanceUrl);

    private long _lastWarningTicks;

    /// <param name="fetch">Calls Describe; the argument is the If-Modified-Since value (null = unconditional), null back on 304 Not Modified.</param>
    /// <param name="sobject">The object's API name, e.g. Account.</param>
    /// <param name="refreshInterval">How old a cached Describe may be before it is checked again.</param>
    /// <param name="ct">Cancels the wait and the call.</param>
    public async Task<SObjectSchema> GetAsync(string sobject, TimeSpan refreshInterval,
        Func<DateTimeOffset?, CancellationToken, Task<SObjectSchema?>> fetch, CancellationToken ct)
    {
        var entry = _entries.GetOrAdd(sobject, _ => new Entry());
        if (Fresh(entry.State, entry.Invalidated, refreshInterval) is { } cached)
        {
            return cached;
        }

        await entry.Gate.WaitAsync(ct);
        try
        {
            if (Fresh(entry.State, entry.Invalidated, refreshInterval) is { } refreshed)
            {
                return refreshed; // another caller just refreshed it
            }

            // An invalidated entry was found out of date: neither this pod's snapshot nor the shared one can be used
            var invalidated = entry.Invalidated;
            var sharedState = invalidated ? null : await ReadSharedAsync(sobject, ct);
            if (Fresh(sharedState, false, refreshInterval) is { } fromOtherPod)
            {
                entry.State = sharedState;
                return fromOtherPod;
            }

            var basis = invalidated ? null : Newest(entry.State, sharedState);
            var now = clock.GetUtcNow();
            var result = await fetch(basis?.FetchedAt, ct);

            var schema = result ?? basis?.Schema
                         ?? throw new InvalidOperationException($"Describe of {sobject} returned no schema");
            var state = new Snapshot(schema, result is not null ? now : basis!.FetchedAt, now);
            entry.State = state;
            entry.Invalidated = false;
            await WriteSharedAsync(sobject, state, ct);
            return schema;
        }
        finally
        {
            entry.Gate.Release();
        }
    }

    /// <summary>
    /// Forces a full describe on next use (e.g. Salesforce rejected a field the cached schema has), in every pod: the
    /// shared entry is removed too.
    /// </summary>
    public async Task InvalidateAsync(string sobject, CancellationToken ct)
    {
        if (_entries.TryGetValue(sobject, out var entry))
        {
            entry.Invalidated = true;
        }

        await SharedAsync(sobject, "remove", token => shared.RemoveAsync(Key(sobject), token), ct);
    }

    private SObjectSchema? Fresh(Snapshot? state, bool invalidated, TimeSpan refreshInterval)
    {
        return state is not null && !invalidated && clock.GetUtcNow() - state.CheckedAt < refreshInterval
            ? state.Schema
            : null;
    }

    private static Snapshot? Newest(Snapshot? a, Snapshot? b)
    {
        return a is null ? b : b is null || a.CheckedAt >= b.CheckedAt ? a : b;
    }

    /// <summary>
    /// <c>salesforce-sync:describe:{org host}:{API version}:</c>: different orgs or API versions never share an entry.
    /// </summary>
    private static string KeyPrefix(string instanceUrl)
    {
        var host = Uri.TryCreate(instanceUrl, UriKind.Absolute, out var url) ? url.Host : instanceUrl;
        return $"salesforce-sync:describe:{host.ToLowerInvariant()}:{SalesforceRestClient.ApiVersion}:";
    }

    /// <summary>The shared entry of an object, e.g. <c>salesforce-sync:describe:acme.my.salesforce.com:v62.0:account</c>.</summary>
    internal string Key(string sobject)
    {
        return _keyPrefix + sobject.ToLowerInvariant();
    }

    private async Task<Snapshot?> ReadSharedAsync(string sobject, CancellationToken ct)
    {
        var json = await SharedAsync(sobject, "read", token => shared.GetAsync(Key(sobject), token), ct);
        if (json is null)
        {
            return null;
        }

        try
        {
            var stored = JsonSerializer.Deserialize<SharedSnapshot>(json)
                         ?? throw new JsonException("null");
            return new Snapshot(new SObjectSchema(stored.Name, stored.Fields, stored.RecordTypes), stored.FetchedAt, stored.CheckedAt);
        }
        catch (Exception ex) when (ex is JsonException or NotSupportedException or ArgumentException)
        {
            // Written by another version, or damaged: describing replaces it
            Warn(ex, sobject, "read");
            return null;
        }
    }

    private Task WriteSharedAsync(string sobject, Snapshot state, CancellationToken ct)
    {
        var json = JsonSerializer.SerializeToUtf8Bytes(new SharedSnapshot(state.Schema.Name, [.. state.Schema.Fields.Values],
            [.. state.Schema.RecordTypes.Values], state.FetchedAt, state.CheckedAt));
        var lifetime = new DistributedCacheEntryOptions { AbsoluteExpirationRelativeToNow = SharedEntryLifetime };
        return SharedAsync(sobject, "write", token => shared.SetAsync(Key(sobject), json, lifetime, token), ct);
    }

    /// <summary>
    /// One shared-cache call, never longer than <see cref="SharedCacheTimeout"/> and never failing the describe: an
    /// error or a timeout is logged and treated as an empty cache. Only the caller's own cancellation is thrown.
    /// </summary>
    private async Task<T?> SharedAsync<T>(string sobject, string operation, Func<CancellationToken, Task<T>> call, CancellationToken ct)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(SharedCacheTimeout);
        try
        {
            // WaitAsync as well: a client may not honour the token while it is still connecting
            return await call(timeout.Token).WaitAsync(SharedCacheTimeout, ct);
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            Warn(ex, sobject, operation);
            return default;
        }
    }

    private Task SharedAsync(string sobject, string operation, Func<CancellationToken, Task> call, CancellationToken ct)
    {
        return SharedAsync(sobject, operation, async token =>
        {
            await call(token);
            return true;
        }, ct);
    }

    /// <summary>At most one warning a minute: an unreachable cache would otherwise log on every describe.</summary>
    private void Warn(Exception ex, string sobject, string operation)
    {
        var now = clock.GetUtcNow().UtcTicks;
        var last = Interlocked.Read(ref _lastWarningTicks);
        if (now - last >= WarningInterval.Ticks && Interlocked.CompareExchange(ref _lastWarningTicks, now, last) == last)
        {
            log.LogWarning(ex, "Shared Describe cache: {Operation} of {SObject} failed; describing without it", operation, sobject);
        }
    }
}