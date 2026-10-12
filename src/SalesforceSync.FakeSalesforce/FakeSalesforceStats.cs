namespace SalesforceSync.FakeSalesforce;

public sealed record FakeSalesforceStats(
    long ApiCalls,
    long Queries,
    long GraphRequests,
    long GraphsSucceeded,
    long GraphsFailed,
    long StaleRejections,
    long VersionRegressions,
    int Records)
{
    public override string ToString() =>
        $"api={ApiCalls} queries={Queries} graphRequests={GraphRequests} graphsOk={GraphsSucceeded} " +
        $"graphsFailed={GraphsFailed} staleRejected={StaleRejections} regressions={VersionRegressions} records={Records}";
}
