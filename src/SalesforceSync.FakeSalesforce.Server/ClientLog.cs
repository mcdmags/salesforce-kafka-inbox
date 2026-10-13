using System.Collections.Concurrent;

namespace SalesforceSync.FakeSalesforce.Server;

/// <summary>
/// What clients sent: composite graph requests per client (User-Agent), to show which worker pods did the writing,
/// and how many Salesforce API requests arrived gzip-compressed / accepted a compressed response.
/// </summary>
public sealed class ClientLog
{
    private readonly ConcurrentDictionary<string, long> _graphRequests = new(StringComparer.Ordinal);
    private long _requests, _gzipRequests, _acceptGzip;

    public void RecordGraphRequest(string? userAgent) =>
        _graphRequests.AddOrUpdate(string.IsNullOrEmpty(userAgent) ? "(none)" : userAgent, 1, (_, n) => n + 1);

    public void RecordRequest(bool gzipBody, bool acceptsGzip)
    {
        Interlocked.Increment(ref _requests);
        if (gzipBody) Interlocked.Increment(ref _gzipRequests);
        if (acceptsGzip) Interlocked.Increment(ref _acceptGzip);
    }

    public IReadOnlyDictionary<string, long> GraphRequests => _graphRequests;

    public object Transport => new
    {
        requests = Interlocked.Read(ref _requests),
        gzipRequests = Interlocked.Read(ref _gzipRequests),
        acceptGzip = Interlocked.Read(ref _acceptGzip)
    };

    public void Reset()
    {
        _graphRequests.Clear();
        Interlocked.Exchange(ref _requests, 0);
        Interlocked.Exchange(ref _gzipRequests, 0);
        Interlocked.Exchange(ref _acceptGzip, 0);
    }
}
