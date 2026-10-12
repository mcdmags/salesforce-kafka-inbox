using System.Collections.Concurrent;

namespace SalesforceSync.FakeSalesforce.Server;

/// <summary>Composite graph requests per client (User-Agent), to show which worker pods did the writing.</summary>
public sealed class ClientLog
{
    private readonly ConcurrentDictionary<string, long> _graphRequests = new(StringComparer.Ordinal);

    public void RecordGraphRequest(string? userAgent) =>
        _graphRequests.AddOrUpdate(string.IsNullOrEmpty(userAgent) ? "(none)" : userAgent, 1, (_, n) => n + 1);

    public IReadOnlyDictionary<string, long> GraphRequests => _graphRequests;

    public void Reset() => _graphRequests.Clear();
}
