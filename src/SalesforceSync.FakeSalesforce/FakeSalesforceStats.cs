namespace SalesforceSync.FakeSalesforce;

/// <param name="WriteRequests">Composite requests that contained sObject Collections subrequests.</param>
/// <param name="RecordsSucceeded">Records created or updated (and not rolled back).</param>
/// <param name="RecordsFailed">Records rejected with an error of their own.</param>
/// <param name="RecordsRolledBack">Records undone because of another record's failure (allOrNone).</param>
public sealed record FakeSalesforceStats(
    long ApiCalls,
    long Queries,
    long WriteRequests,
    long RecordsSucceeded,
    long RecordsFailed,
    long RecordsRolledBack,
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
        $"recordsOk={RecordsSucceeded} recordsFailed={RecordsFailed} recordsRolledBack={RecordsRolledBack} creates={Creates} " +
        $"updates={Updates} describes={Describes} (notModified={DescribesNotModified}) " +
        $"concurrencyRejected={ConcurrencyRejections} records={Records}";
    }
}