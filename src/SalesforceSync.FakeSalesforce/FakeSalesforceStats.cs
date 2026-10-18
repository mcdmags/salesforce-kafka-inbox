namespace SalesforceSync.FakeSalesforce;

/// <param name="WriteRequests">Write calls: sObject Collections upserts and deletes.</param>
/// <param name="RecordsSucceeded">Records created, updated or deleted.</param>
/// <param name="RecordsFailed">Records rejected with an error of their own.</param>
/// <param name="UpsertRequests">sObject Collections upserts (also counted in <paramref name="WriteRequests"/>).</param>
/// <param name="Deletes">Records deleted.</param>
/// <param name="DeleteRequests">sObject Collections deletes.</param>
/// <param name="ApiCalls">Calls counted against the daily API allocation (reported in Sforce-Limit-Info).</param>
/// <param name="Queries">SOQL queries run.</param>
/// <param name="Creates">Records created.</param>
/// <param name="Updates">Records updated.</param>
/// <param name="ConcurrencyRejections">Calls refused for exceeding the concurrent long-running request limit.</param>
/// <param name="RecordTypeQueries">SOQL queries on RecordType (also counted in <paramref name="Queries"/>).</param>
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
    long RecordTypeQueries,
    long UpsertRequests,
    long Deletes,
    long DeleteRequests,
    int Records)
{
    public override string ToString()
    {
        return $"api={ApiCalls} upserts={UpsertRequests} queries={Queries} " +
               $"writeRequests={WriteRequests} recordsOk={RecordsSucceeded} recordsFailed={RecordsFailed} " +
               $"creates={Creates} updates={Updates} deletes={Deletes} (requests={DeleteRequests}) " +
               $"recordTypeQueries={RecordTypeQueries} " +
               $"concurrencyRejected={ConcurrencyRejections} records={Records}";
    }
}