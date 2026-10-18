namespace SalesforceSync.FakeSalesforce;

/// <summary>
/// What the mock org counts (see <see cref="FakeSalesforceStats"/>). API calls and concurrency rejections are counted
/// outside the store's lock, so they are atomic; the others change only under it.
/// </summary>
internal sealed class FakeCounters
{
    private long _apiCalls, _concurrencyRejections;

    public long Queries, WriteRequests, RecordsSucceeded, RecordsFailed, Creates, Updates, Describes, DescribesNotModified,
        UpsertRequests, Deletes, DeleteRequests;

    public long CountApiCall()
    {
        return Interlocked.Increment(ref _apiCalls);
    }

    public void CountConcurrencyRejection()
    {
        Interlocked.Increment(ref _concurrencyRejections);
    }

    public FakeSalesforceStats Snapshot(int records)
    {
        return new(
            Interlocked.Read(ref _apiCalls), Queries, WriteRequests, RecordsSucceeded, RecordsFailed,
            Creates, Updates, Interlocked.Read(ref _concurrencyRejections), Describes, DescribesNotModified,
            UpsertRequests, Deletes, DeleteRequests, records);
    }

    public void Reset()
    {
        _apiCalls = Queries = WriteRequests = RecordsSucceeded = RecordsFailed = Creates = Updates = 0;
        Describes = DescribesNotModified = UpsertRequests = Deletes = DeleteRequests = 0;
        Interlocked.Exchange(ref _concurrencyRejections, 0);
    }
}