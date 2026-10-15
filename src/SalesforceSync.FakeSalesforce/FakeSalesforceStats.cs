namespace SalesforceSync.FakeSalesforce;

/// <param name="WriteRequests">Write calls: Composite requests that contained sObject Collections subrequests, and
/// sObject Collections upserts.</param>
/// <param name="RecordsSucceeded">Records created or updated.</param>
/// <param name="RecordsFailed">Records rejected with an error of their own.</param>
/// <param name="UpsertRequests">sObject Collections upserts (also counted in <paramref name="WriteRequests"/>).</param>
/// <param name="ApiCalls">Calls counted against the daily API allocation.</param>
/// <param name="Queries">SOQL queries run, as Composite subrequests or on their own.</param>
/// <param name="Creates">Records created.</param>
/// <param name="Updates">Records updated.</param>
/// <param name="ConcurrencyRejections">Calls refused for exceeding the concurrent long-running request limit.</param>
/// <param name="Describes">sObject Describe calls.</param>
/// <param name="DescribesNotModified">Describe calls answered 304 Not Modified.</param>
/// <param name="CompositeRequests">Composite requests, reads and writes.</param>
/// <param name="Records">Records the org holds now.</param>
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