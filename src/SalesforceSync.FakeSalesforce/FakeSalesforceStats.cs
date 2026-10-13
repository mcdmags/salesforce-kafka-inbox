namespace SalesforceSync.FakeSalesforce;

public sealed record FakeSalesforceStats(
    long ApiCalls,
    long Queries,
    long GraphRequests,
    long GraphsSucceeded,
    long GraphsFailed,
    long Creates,
    long Updates,
    long ConcurrencyRejections,
    int Records)
{
    public override string ToString() =>
        $"api={ApiCalls} queries={Queries} graphRequests={GraphRequests} graphsOk={GraphsSucceeded} " +
        $"graphsFailed={GraphsFailed} creates={Creates} updates={Updates} concurrencyRejected={ConcurrencyRejections} records={Records}";
}
