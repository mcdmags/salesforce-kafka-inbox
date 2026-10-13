namespace SalesforceSync.FakeSalesforce;

public sealed record FakeSalesforceStats(
    long ApiCalls,
    long Queries,
    long GraphRequests,
    long GraphsSucceeded,
    long GraphsFailed,
    long GraphsHalted,
    long Creates,
    long Updates,
    long ConcurrencyRejections,
    long Describes,
    long DescribesNotModified,
    long CompositeRequests,
    int Records)
{
    public override string ToString() =>
        $"api={ApiCalls} composite={CompositeRequests} queries={Queries} graphRequests={GraphRequests} " +
        $"graphsOk={GraphsSucceeded} graphsFailed={GraphsFailed} graphsHalted={GraphsHalted} creates={Creates} " +
        $"updates={Updates} describes={Describes} (notModified={DescribesNotModified}) " +
        $"concurrencyRejected={ConcurrencyRejections} records={Records}";
}
