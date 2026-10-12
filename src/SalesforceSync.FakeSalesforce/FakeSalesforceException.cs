namespace SalesforceSync.FakeSalesforce;

public sealed class FakeSalesforceException(int status, string errorCode, string message) : Exception(message)
{
    public int Status { get; } = status;
    public string ErrorCode { get; } = errorCode;
}
