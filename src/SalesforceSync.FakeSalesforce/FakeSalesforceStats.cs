namespace SalesforceSync.FakeSalesforce;

/// <param name="WriteRequests">Write calls: Composite requests that contained sObject Collections subrequests, and
/// sObject Collections upserts.</param>
/// <param name="RecordsSucceeded">Records created or updated.</param>
/// <param name="RecordsFailed">Records rejected with an error of their own.</param>
/// <param name="UpsertRequests">sObject Collections upserts (also counted in <paramref name="WriteRequests"/>).</param>
public sealed record FakeSalesforceStats(
    long ApiCalls,
    long Queries,
    long WriteRequests,
    long RecordsSucceeded,
    long RecordsFailed,
    long Creates,
    long Updates,
    long ConcurrencyRejections,
    long Describes,
    long DescribesNotModified,
    long CompositeRequests,
    long UpsertRequests,
    int Records)
{
    public override string ToString()
    {
        return $"api={ApiCalls} composite={CompositeRequests} upserts={UpsertRequests} queries={Queries} writeRequests={WriteRequests} " +
        $"recordsOk={RecordsSucceeded} recordsFailed={RecordsFailed} creates={Creates} " +
        $"updates={Updates} describes={Describes} (notModified={DescribesNotModified}) " +
        $"concurrencyRejected={ConcurrencyRejections} records={Records}";
    }
}