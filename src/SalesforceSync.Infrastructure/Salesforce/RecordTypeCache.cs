using System.Collections.Concurrent;
using System.Text.Json;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Options;

namespace SalesforceSync.Infrastructure.Salesforce;

/// <summary>
/// The Ids of the active record types of each object, by developer name: Salesforce takes a record type as its Id, which
/// differs between orgs. Read with one SOQL query on RecordType per object and kept for
/// <see cref="SalesforceOptions.RecordTypeRefreshInterval"/> in two layers:
/// <list type="number">
/// <item>In process: the snapshot each worker reads without I/O; concurrent callers share one in-flight read per
/// object.</item>
/// <item>Shared by all pods (<see cref="IDistributedCache"/>: Redis, or memory when none is configured): a pod whose own
/// snapshot is missing or stale takes another pod's if that one was read within the refresh interval, so the org is
/// asked about once per interval in total, and a new pod starts warm. The shared layer only saves calls: when it is
/// unreachable or holds something unreadable, the pod reads the org as if it were empty.</item>
/// </list>
/// Nothing else of the org's metadata is read.
/// </summary>
public sealed class RecordTypeCache(
    TimeProvider clock,
    ISalesforceRestClient salesforce,
    IDistributedCache shared,
    IOptions<SalesforceOptions> options,
    ILogger<RecordTypeCache> log)
{
    /// <summary>The field that holds a record's record type.</summary>
    public const string RecordTypeIdField = "RecordTypeId";

    /// <summary>A shared-cache call that takes longer is abandoned and the pod reads the org on its own.</summary>
    private static readonly TimeSpan SharedCacheTimeout = TimeSpan.FromSeconds(2);

    /// <summary>How long a shared entry is kept: a little longer than the refresh interval is enough, an old one is never trusted.</summary>
    private static readonly TimeSpan SharedEntryLifetime = TimeSpan.FromDays(1);

    private static readonly TimeSpan WarningInterval = TimeSpan.FromMinutes(1);

    /// <summary>The Ids by developer name, and when they were read; also the JSON form in the shared cache.</summary>
    private sealed record Snapshot(Dictionary<string, string> Ids, DateTimeOffset ReadAt);

    private sealed class Entry
    {
        public readonly SemaphoreSlim Gate = new(1, 1);
        public volatile Snapshot? State;      // replaced as a whole, so lock-free readers never see a torn update
        public volatile bool Invalidated;
    }

    private readonly ConcurrentDictionary<string, Entry> _entries = new(StringComparer.OrdinalIgnoreCase);
    private readonly TimeSpan _refreshInterval = options.Value.RecordTypeRefreshInterval;
    private readonly string _keyPrefix = KeyPrefix(options.Value.InstanceUrl, options.Value.ApiVersion);
    private long _lastWarningTicks;

    /// <summary>The Id of the object's active record type with this developer name, or null if the org has none.</summary>
    public async Task<string?> IdOfAsync(string sobject, string developerName, CancellationToken ct)
    {
        var entry = _entries.GetOrAdd(sobject, _ => new Entry());
        var state = Fresh(entry.State, entry.Invalidated);
        if (state is null)
        {
            await entry.Gate.WaitAsync(ct);
            try
            {
                state = Fresh(entry.State, entry.Invalidated) ?? await LoadAsync(sobject, entry, ct);
            }
            finally
            {
                entry.Gate.Release();
            }
        }

        return state.Ids.GetValueOrDefault(developerName);
    }

    /// <summary>
    /// The object's record types are read from the org again next time, in every pod (one was missing or refused): the
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

    private Snapshot? Fresh(Snapshot? state, bool invalidated)
    {
        return state is not null && !invalidated && clock.GetUtcNow() - state.ReadAt < _refreshInterval ? state : null;
    }

    private async Task<Snapshot> LoadAsync(string sobject, Entry entry, CancellationToken ct)
    {
        // An invalidated entry was found out of date: neither this pod's snapshot nor the shared one can be used
        if (!entry.Invalidated && Fresh(await ReadSharedAsync(sobject, ct), invalidated: false) is { } fromOtherPod)
        {
            entry.State = fromOtherPod;
            return fromOtherPod;
        }

        var rows = await salesforce.SelectAsync(
            SalesforceRestClient.Soql("RecordType", ["Id", "DeveloperName", "IsActive"], "SobjectType", [sobject]), ct);
        var ids = rows
            .Where(r => r.GetProperty("IsActive").ValueKind == JsonValueKind.True)
            .DistinctBy(r => r.GetProperty("DeveloperName").GetString(), StringComparer.OrdinalIgnoreCase) // unique per namespace only: the first is kept
            .ToDictionary(r => r.GetProperty("DeveloperName").GetString()!, r => r.GetProperty("Id").GetString()!, StringComparer.OrdinalIgnoreCase);
        var state = new Snapshot(ids, clock.GetUtcNow());
        entry.State = state;
        entry.Invalidated = false;
        await WriteSharedAsync(sobject, state, ct);
        return state;
    }

    /// <summary><c>salesforce-sync:recordtypes:{org host}:{API version}:</c>: different orgs or API versions never share an entry.</summary>
    private static string KeyPrefix(string instanceUrl, string apiVersion)
    {
        var host = Uri.TryCreate(instanceUrl, UriKind.Absolute, out var url) ? url.Host : instanceUrl;
        return $"salesforce-sync:recordtypes:{host.ToLowerInvariant()}:{apiVersion}:";
    }

    /// <summary>The shared entry of an object, e.g. <c>salesforce-sync:recordtypes:acme.my.salesforce.com:v62.0:account</c>.</summary>
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
            var stored = JsonSerializer.Deserialize<Snapshot>(json) ?? throw new JsonException("null");
            return stored with { Ids = new Dictionary<string, string>(stored.Ids, StringComparer.OrdinalIgnoreCase) };
        }
        catch (Exception ex) when (ex is JsonException or NotSupportedException or ArgumentException)
        {
            // Written by another version, or damaged: reading the org replaces it
            Warn(ex, sobject, "read");
            return null;
        }
    }

    private Task WriteSharedAsync(string sobject, Snapshot state, CancellationToken ct)
    {
        var json = JsonSerializer.SerializeToUtf8Bytes(state);
        var lifetime = new DistributedCacheEntryOptions { AbsoluteExpirationRelativeToNow = SharedEntryLifetime };
        return SharedAsync(sobject, "write", token => shared.SetAsync(Key(sobject), json, lifetime, token), ct);
    }

    /// <summary>
    /// One shared-cache call, never longer than <see cref="SharedCacheTimeout"/> and never failing the read: an error or a
    /// timeout is logged and treated as an empty cache. Only the caller's own cancellation is thrown.
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

    /// <summary>At most one warning a minute: an unreachable cache would otherwise log on every read.</summary>
    private void Warn(Exception ex, string sobject, string operation)
    {
        var now = clock.GetUtcNow().UtcTicks;
        var last = Interlocked.Read(ref _lastWarningTicks);
        if (now - last >= WarningInterval.Ticks && Interlocked.CompareExchange(ref _lastWarningTicks, now, last) == last)
        {
            log.LogWarning(ex, "Shared record type cache: {Operation} of {SObject} failed; reading the org without it", operation, sobject);
        }
    }
}
