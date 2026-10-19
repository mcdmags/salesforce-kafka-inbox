using System.Collections.Concurrent;

namespace SalesforceSync.FakeSalesforce.Server;

/// <summary>
/// What clients sent: requests that wrote records (sObject Collections upserts and deletes) per client (User-Agent),
/// to show which worker pods did the work, and how many Salesforce API requests arrived gzip-compressed or accepted
/// a compressed response.
/// </summary>
public sealed class ClientLog
{
    private readonly ConcurrentDictionary<string, long> _dataRequests = new(StringComparer.Ordinal);
    private long _requests, _gzipRequests, _acceptGzip;

    public void RecordDataRequest(string? userAgent)
    {
        _dataRequests.AddOrUpdate(string.IsNullOrEmpty(userAgent) ? "(none)" : userAgent, 1, (_, n) => n + 1);
    }

    public void RecordRequest(bool gzipBody, bool acceptsGzip)
    {
        Interlocked.Increment(ref _requests);
        if (gzipBody)
        {
            Interlocked.Increment(ref _gzipRequests);
        }

        if (acceptsGzip)
        {
            Interlocked.Increment(ref _acceptGzip);
        }
    }

    public IReadOnlyDictionary<string, long> DataRequests => _dataRequests;

    public object Transport => new
    {
        requests = Interlocked.Read(ref _requests),
        gzipRequests = Interlocked.Read(ref _gzipRequests),
        acceptGzip = Interlocked.Read(ref _acceptGzip)
    };

    public void Reset()
    {
        _dataRequests.Clear();
        Interlocked.Exchange(ref _requests, 0);
        Interlocked.Exchange(ref _gzipRequests, 0);
        Interlocked.Exchange(ref _acceptGzip, 0);
    }
}