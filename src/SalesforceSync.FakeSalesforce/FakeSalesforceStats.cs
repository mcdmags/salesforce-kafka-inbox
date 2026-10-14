namespace SalesforceSync.FakeSalesforce;

/// <param name="WriteRequests">Composite requests that contained sObject Collections subrequests.</param>
/// <param name="RecordsSucceeded">Records created or updated.</param>
/// <param name="RecordsFailed">Records rejected with an error of their own.</param>
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
    int Records)
{
    public override string ToString()
    {
        return $"api={ApiCalls} composite={CompositeRequests} queries={Queries} writeRequests={WriteRequests} " +
        $"recordsOk={RecordsSucceeded} recordsFailed={RecordsFailed} creates={Creates} " +
        $"updates={Updates} describes={Describes} (notModified={DescribesNotModified}) " +
        $"concurrencyRejected={ConcurrencyRejections} records={Records}";
    }
}